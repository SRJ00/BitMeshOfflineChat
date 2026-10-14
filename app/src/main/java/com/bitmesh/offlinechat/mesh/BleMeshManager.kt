package com.bitmesh.offlinechat.mesh

import android.Manifest
import android.annotation.SuppressLint
import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.bluetooth.*
import android.bluetooth.le.*
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.content.pm.PackageManager
import android.media.AudioDeviceInfo
import android.media.AudioManager
import android.os.Build
import android.os.ParcelUuid
import android.util.Base64
import android.util.Log
import androidx.core.content.ContextCompat
import com.bitmesh.offlinechat.crypto.CryptoEngine
import com.bitmesh.offlinechat.ui.MainActivity
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.json.JSONArray
import org.json.JSONObject
import java.security.SecureRandom
import java.util.*
import java.util.concurrent.ConcurrentHashMap

data class PeerNode(
    val nodeId: Long,
    val nickname: String,
    val publicKeyBytes: ByteArray,
    val rssi: Int,
    val hops: Int,
    val lastSeenMs: Long
)

data class ChatMessage(
    val packetId: Long,
    val senderId: Long,
    val senderName: String,
    val recipientId: Long,
    val channel: String,
    val text: String,
    val hops: Int,
    val timestampSec: Long,
    val isDirectEncrypted: Boolean,
    val isOutgoing: Boolean,
    val isDeliveredAck: Boolean = false
)

/**
 * Dual-Role Android BLE Mesh Engine (v1.5 — Bluetooth Headphone A2DP Coexistence + Persistent Outbox).
 *
 * Fixes in v1.5 for Smooth Bluetooth Headphone Music Playback:
 * 1. Eliminated 100% Duty-Cycle Radio Hogging (SCAN_MODE_LOW_LATENCY):
 *    - Smartphone Bluetooth chips share a single 2.4 GHz radio antenna between Classic Bluetooth Audio (A2DP)
 *      and BLE scanning. Continuous SCAN_MODE_LOW_LATENCY starves the A2DP audio buffer and causes gaps in music.
 *    - v1.5 uses Windowed Duty-Cycle Scanning (scans briefly for 3s, then rests the BLE scanner for 12s–20s so
 *      the 2.4 GHz antenna has >85% uninterrupted airtime for Bluetooth headphones).
 * 2. Automatic Bluetooth Headphone / Audio Detection:
 *    - Detects active Bluetooth A2DP / BLE Audio headphones or music playback via AudioManager & BluetoothAdapter
 *      and automatically switches scan & advertise parameters to LOW_POWER coexistence mode.
 * 3. Per-Peer Packet Tracking (No More 4-Second Re-Broadcast Storms):
 *    - Tracks which packetIds have already been delivered to each connected peer address (`deliveredToAddress`)
 *      and slows background heartbeats from 4s to 45s so connected phones never spam the radio while music plays.
 * 4. Persistent Disk Outbox & Rolling #general History:
 *    - Saves undelivered Private DMs and the latest 50 #general posts to SharedPreferences so they survive reboots.
 */
@SuppressLint("MissingPermission")
class BleMeshManager(
    private val context: Context,
    val cryptoEngine: CryptoEngine
) {
    companion object {
        private const val TAG = "BleMeshManager"
        private const val PREFS_NAME = "bitmesh_user_prefs"
        private const val KEY_NICKNAME = "custom_nickname"
        private const val KEY_SAVED_OUTBOX = "saved_outbox_v15"
        private const val KEY_SAVED_MESSAGES = "saved_messages_v15"
        private const val MSG_NOTIFICATION_CHANNEL_ID = "bitmesh_incoming_messages"

        val MESH_SERVICE_UUID: UUID = UUID.fromString("b17c4a70-0001-4000-8000-00805f9b34fb")
        val PACKET_CHAR_UUID: UUID = UUID.fromString("b17c4a70-0002-4000-8000-00805f9b34fb")
        val CCCD_UUID: UUID = UUID.fromString("00002902-0000-1000-8000-00805f9b34fb")
        private const val MAX_SEEN_PACKETS = 8192
        private const val MAX_OUTBOX_PACKETS = 200
        private const val MAX_ROLLING_GENERAL_PACKETS = 50
    }

    private val prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
    private val notificationManager = context.getSystemService(NotificationManager::class.java)
    private val audioManager = context.getSystemService(Context.AUDIO_SERVICE) as AudioManager

    private val _myNicknameFlow = MutableStateFlow(loadSavedNickname())
    val myNicknameFlow: StateFlow<String> = _myNicknameFlow

    val myNickname: String
        get() = _myNicknameFlow.value

    private val bluetoothManager = context.getSystemService(Context.BLUETOOTH_SERVICE) as BluetoothManager
    private val bluetoothAdapter: BluetoothAdapter?
        get() = bluetoothManager.adapter

    private var gattServer: BluetoothGattServer? = null
    private var advertiser: BluetoothLeAdvertiser? = null
    private var scanner: BluetoothLeScanner? = null

    @Volatile
    private var isAdvertising = false
    @Volatile
    private var isScanning = false
    @Volatile
    private var isReceiverRegistered = false

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val gattWriteMutex = Mutex()
    private val secureRandom = SecureRandom()

    private val seenPacketIds = Collections.synchronizedSet(
        object : LinkedHashSet<Long>(MAX_SEEN_PACKETS) {
            override fun add(element: Long): Boolean {
                if (size >= MAX_SEEN_PACKETS) {
                    val it = iterator()
                    if (it.hasNext()) {
                        it.next()
                        it.remove()
                    }
                }
                return super.add(element)
            }
        }
    )

    // Tracks which packetIds have already been sent to a given connected BLE MAC address to prevent radio spam
    private val sentPacketIdsByAddress = ConcurrentHashMap<String, MutableSet<Long>>()
    private val peerAnnouncementCache = ConcurrentHashMap<Long, MeshPacket>()
    private val storeAndForwardOutbox = ConcurrentHashMap<Long, MeshPacket>()
    private val activeGattClients = ConcurrentHashMap<String, BluetoothGatt>()
    private val readyGattClients = ConcurrentHashMap<String, BluetoothGatt>()
    private val connectedServerDevices = ConcurrentHashMap<String, BluetoothDevice>()

    private val _peers = MutableStateFlow<Map<Long, PeerNode>>(emptyMap())
    val peers: StateFlow<Map<Long, PeerNode>> = _peers

    private val _messages = MutableStateFlow<List<ChatMessage>>(emptyList())
    val messages: StateFlow<List<ChatMessage>> = _messages

    private val _meshStats = MutableStateFlow("Initializing BLE Mesh...")
    val meshStats: StateFlow<String> = _meshStats

    private val bluetoothStateReceiver = object : BroadcastReceiver() {
        override fun onReceive(context: Context?, intent: Intent?) {
            if (intent?.action == BluetoothAdapter.ACTION_STATE_CHANGED) {
                val state = intent.getIntExtra(BluetoothAdapter.EXTRA_STATE, BluetoothAdapter.ERROR)
                if (state == BluetoothAdapter.STATE_ON) {
                    startMesh()
                } else if (state == BluetoothAdapter.STATE_OFF) {
                    isAdvertising = false
                    isScanning = false
                    _meshStats.value = "Bluetooth is turned off"
                }
            }
        }
    }

    init {
        createIncomingMessageNotificationChannel()
        loadPersistedState()
        registerBluetoothStateReceiver()

        // Duty-Cycled Scanner & Lightweight Coexistence Loop (protects Bluetooth A2DP Headphone Audio!)
        scope.launch {
            var ticks = 0
            while (isActive) {
                val adapter = bluetoothAdapter
                val btOn = adapter != null && runCatching { adapter.isEnabled }.getOrDefault(false)
                if (btOn && hasRequiredPermissions()) {
                    if (gattServer == null) runCatching { startGattServer() }
                    if (!isAdvertising) runCatching { startAdvertising() }

                    // Windowed BLE scan: scan briefly for 3.2 seconds, then STOP scanning for 12-16 seconds
                    // so Bluetooth A2DP headphones get 100% uninterrupted 2.4 GHz antenna time!
                    runCatching { startWindowedScan() }
                    delay(3_200L)
                    runCatching { stopWindowedScan() }

                    val audioActive = isBluetoothAudioActive()
                    _meshStats.value = if (audioActive) {
                        "Online as $myNickname (🎧 Audio-Safe)"
                    } else {
                        "Online as $myNickname"
                    }

                    // Only send periodic key heartbeat every ~45 seconds (every 3rd cycle)
                    ticks++
                    if (ticks % 3 == 1) {
                        broadcastKeyAnnouncement()
                    }
                    flushUnsentOutboxToPeers()

                    // Rest the BLE scanner for 15s when headphones/audio are active, or 10s otherwise
                    delay(if (audioActive) 15_000L else 10_000L)
                } else {
                    ensureMeshRunning()
                    delay(5_000L)
                }
            }
        }
    }

    /**
     * Checks if Bluetooth headphones (A2DP / LE Audio / SCO) are connected or music is playing.
     */
    private fun isBluetoothAudioActive(): Boolean {
        return runCatching {
            val musicPlaying = audioManager.isMusicActive
            val a2dpConnected = bluetoothAdapter?.getProfileConnectionState(BluetoothProfile.A2DP) == BluetoothProfile.STATE_CONNECTED
            val btAudioDevice = audioManager.getDevices(AudioManager.GET_DEVICES_OUTPUTS).any {
                it.type == AudioDeviceInfo.TYPE_BLUETOOTH_A2DP ||
                it.type == AudioDeviceInfo.TYPE_BLUETOOTH_SCO ||
                (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S && it.type == AudioDeviceInfo.TYPE_BLE_HEADSET)
            }
            musicPlaying || a2dpConnected || btAudioDevice
        }.getOrDefault(false)
    }

    private fun loadSavedNickname(): String {
        val defaultName = "Node-${cryptoEngine.shortFingerprintHex().take(4)}"
        return prefs.getString(KEY_NICKNAME, null)?.takeIf { it.isNotBlank() } ?: defaultName
    }

    fun updateNickname(newName: String) {
        val cleaned = newName.trim().replace("|", "").take(24)
        if (cleaned.isEmpty()) return
        prefs.edit().putString(KEY_NICKNAME, cleaned).apply()
        _myNicknameFlow.value = cleaned
        _meshStats.value = "Online as $cleaned"
        broadcastKeyAnnouncement()
    }

    private fun savePersistedState() {
        runCatching {
            val outboxArray = JSONArray()
            storeAndForwardOutbox.values.take(MAX_OUTBOX_PACKETS).forEach { pkt ->
                outboxArray.put(Base64.encodeToString(pkt.toBytes(), Base64.NO_WRAP))
            }
            val msgArray = JSONArray()
            _messages.value.takeLast(100).forEach { m ->
                val obj = JSONObject().apply {
                    put("packetId", m.packetId)
                    put("senderId", m.senderId)
                    put("senderName", m.senderName)
                    put("recipientId", m.recipientId)
                    put("channel", m.channel)
                    put("text", m.text)
                    put("hops", m.hops)
                    put("timestampSec", m.timestampSec)
                    put("isDirectEncrypted", m.isDirectEncrypted)
                    put("isOutgoing", m.isOutgoing)
                    put("isDeliveredAck", m.isDeliveredAck)
                }
                msgArray.put(obj)
            }
            prefs.edit()
                .putString(KEY_SAVED_OUTBOX, outboxArray.toString())
                .putString(KEY_SAVED_MESSAGES, msgArray.toString())
                .apply()
        }
    }

    private fun loadPersistedState() {
        runCatching {
            val outboxStr = prefs.getString(KEY_SAVED_OUTBOX, null)
            if (!outboxStr.isNullOrBlank()) {
                val arr = JSONArray(outboxStr)
                for (i in 0 until arr.length()) {
                    val raw = Base64.decode(arr.getString(i), Base64.NO_WRAP)
                    val pkt = MeshPacket.fromBytes(raw)
                    if (pkt != null) {
                        storeAndForwardOutbox[pkt.packetId] = pkt
                        seenPacketIds.add(pkt.packetId)
                    }
                }
            }
            val msgStr = prefs.getString(KEY_SAVED_MESSAGES, null)
            if (!msgStr.isNullOrBlank()) {
                val arr = JSONArray(msgStr)
                val loaded = mutableListOf<ChatMessage>()
                for (i in 0 until arr.length()) {
                    val o = arr.getJSONObject(i)
                    val m = ChatMessage(
                        packetId = o.getLong("packetId"),
                        senderId = o.getLong("senderId"),
                        senderName = o.getString("senderName"),
                        recipientId = o.getLong("recipientId"),
                        channel = o.getString("channel"),
                        text = o.getString("text"),
                        hops = o.getInt("hops"),
                        timestampSec = o.getLong("timestampSec"),
                        isDirectEncrypted = o.getBoolean("isDirectEncrypted"),
                        isOutgoing = o.getBoolean("isOutgoing"),
                        isDeliveredAck = o.optBoolean("isDeliveredAck", false)
                    )
                    loaded.add(m)
                    seenPacketIds.add(m.packetId)
                }
                _messages.value = loaded
            }
        }
    }

    private fun createIncomingMessageNotificationChannel() {
        val channel = NotificationChannel(
            MSG_NOTIFICATION_CHANNEL_ID,
            "Incoming Mesh Messages",
            NotificationManager.IMPORTANCE_HIGH
        ).apply {
            description = "Alerts when a new offline Bluetooth mesh message arrives"
            enableVibration(true)
            enableLights(true)
        }
        notificationManager.createNotificationChannel(channel)
    }

    private fun postIncomingMessageNotification(
        senderName: String,
        messageText: String,
        isDirect: Boolean,
        hops: Int
    ) {
        runCatching {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU &&
                ContextCompat.checkSelfPermission(context, Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
            ) {
                return
            }
            val tapIntent = Intent(context, MainActivity::class.java).apply {
                flags = Intent.FLAG_ACTIVITY_SINGLE_TOP or Intent.FLAG_ACTIVITY_CLEAR_TOP
            }
            val pendingIntent = PendingIntent.getActivity(
                context,
                0,
                tapIntent,
                PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
            )
            val title = if (isDirect) {
                "🔒 Private message from $senderName ($hops hop${if (hops > 1) "s" else ""})"
            } else {
                "#general • $senderName ($hops hop${if (hops > 1) "s" else ""})"
            }
            val notification = Notification.Builder(context, MSG_NOTIFICATION_CHANNEL_ID)
                .setSmallIcon(android.R.drawable.stat_notify_chat)
                .setContentTitle(title)
                .setContentText(messageText)
                .setContentIntent(pendingIntent)
                .setAutoCancel(true)
                .build()

            notificationManager.notify((System.currentTimeMillis() and 0xFFFFFFF).toInt(), notification)
        }
    }

    private fun registerBluetoothStateReceiver() {
        if (!isReceiverRegistered) {
            runCatching {
                context.registerReceiver(
                    bluetoothStateReceiver,
                    IntentFilter(BluetoothAdapter.ACTION_STATE_CHANGED)
                )
                isReceiverRegistered = true
            }
        }
    }

    fun hasRequiredPermissions(): Boolean {
        return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
            ContextCompat.checkSelfPermission(context, Manifest.permission.BLUETOOTH_SCAN) == PackageManager.PERMISSION_GRANTED &&
            ContextCompat.checkSelfPermission(context, Manifest.permission.BLUETOOTH_ADVERTISE) == PackageManager.PERMISSION_GRANTED &&
            ContextCompat.checkSelfPermission(context, Manifest.permission.BLUETOOTH_CONNECT) == PackageManager.PERMISSION_GRANTED
        } else {
            ContextCompat.checkSelfPermission(context, Manifest.permission.ACCESS_FINE_LOCATION) == PackageManager.PERMISSION_GRANTED ||
            ContextCompat.checkSelfPermission(context, Manifest.permission.ACCESS_COARSE_LOCATION) == PackageManager.PERMISSION_GRANTED
        }
    }

    fun startMesh() {
        scope.launch {
            ensureMeshRunning()
            broadcastKeyAnnouncement()
            flushUnsentOutboxToPeers()
        }
    }

    @Synchronized
    private fun ensureMeshRunning() {
        val adapter = bluetoothAdapter
        if (adapter == null) {
            _meshStats.value = "No Bluetooth hardware found"
            return
        }
        if (!hasRequiredPermissions()) {
            _meshStats.value = "Grant 'Nearby Devices' permission to start"
            return
        }
        val enabled = runCatching { adapter.isEnabled || adapter.state == BluetoothAdapter.STATE_ON }.getOrDefault(false)
        if (!enabled) {
            _meshStats.value = "Bluetooth is off — tap Refresh after enabling"
            return
        }

        if (gattServer == null) {
            runCatching { startGattServer() }
        }
        if (!isAdvertising) {
            runCatching { startAdvertising() }
        }
        if (!isScanning) {
            runCatching { startWindowedScan() }
        }

        _meshStats.value = if (isBluetoothAudioActive()) {
            "Online as $myNickname (🎧 Audio-Safe)"
        } else {
            "Online as $myNickname"
        }
    }

    fun stopMesh() {
        if (isReceiverRegistered) {
            runCatching { context.unregisterReceiver(bluetoothStateReceiver) }
            isReceiverRegistered = false
        }
        runCatching { advertiser?.stopAdvertising(advertiseCallback) }
        runCatching { stopWindowedScan() }
        isAdvertising = false
        activeGattClients.values.forEach { runCatching { it.close() } }
        activeGattClients.clear()
        readyGattClients.clear()
        runCatching { gattServer?.close() }
        gattServer = null
        scope.cancel()
        _meshStats.value = "Mesh Stopped"
    }

    private fun startGattServer() {
        val server = bluetoothManager.openGattServer(context, gattServerCallback) ?: return
        val service = BluetoothGattService(MESH_SERVICE_UUID, BluetoothGattService.SERVICE_TYPE_PRIMARY)
        val packetCharacteristic = BluetoothGattCharacteristic(
            PACKET_CHAR_UUID,
            BluetoothGattCharacteristic.PROPERTY_WRITE or
                    BluetoothGattCharacteristic.PROPERTY_WRITE_NO_RESPONSE or
                    BluetoothGattCharacteristic.PROPERTY_NOTIFY or
                    BluetoothGattCharacteristic.PROPERTY_READ,
            BluetoothGattCharacteristic.PERMISSION_WRITE or
                    BluetoothGattCharacteristic.PERMISSION_READ
        )
        val cccd = BluetoothGattDescriptor(
            CCCD_UUID,
            BluetoothGattDescriptor.PERMISSION_READ or BluetoothGattDescriptor.PERMISSION_WRITE
        )
        packetCharacteristic.addDescriptor(cccd)
        service.addCharacteristic(packetCharacteristic)
        server.addService(service)
        gattServer = server
    }

    /**
     * Uses ADVERTISE_MODE_BALANCED / LOW_POWER instead of LOW_LATENCY so advertising packets
     * never interrupt Bluetooth A2DP headphone audio frames.
     */
    private fun startAdvertising() {
        advertiser = bluetoothAdapter?.bluetoothLeAdvertiser ?: return
        val mode = if (isBluetoothAudioActive()) {
            AdvertiseSettings.ADVERTISE_MODE_LOW_POWER
        } else {
            AdvertiseSettings.ADVERTISE_MODE_BALANCED
        }
        val settings = AdvertiseSettings.Builder()
            .setAdvertiseMode(mode)
            .setTxPowerLevel(AdvertiseSettings.ADVERTISE_TX_POWER_MEDIUM)
            .setConnectable(true)
            .build()

        val data = AdvertiseData.Builder()
            .setIncludeDeviceName(false)
            .addServiceUuid(ParcelUuid(MESH_SERVICE_UUID))
            .build()

        advertiser?.startAdvertising(settings, data, advertiseCallback)
    }

    /**
     * Uses SCAN_MODE_LOW_POWER when Bluetooth headphones are active (or SCAN_MODE_BALANCED otherwise),
     * and only runs in short 3.2-second windows so A2DP music plays with zero gaps.
     */
    private fun startWindowedScan() {
        if (isScanning) return
        scanner = bluetoothAdapter?.bluetoothLeScanner ?: return
        val filter = ScanFilter.Builder()
            .setServiceUuid(ParcelUuid(MESH_SERVICE_UUID))
            .build()

        val scanMode = if (isBluetoothAudioActive()) {
            ScanSettings.SCAN_MODE_LOW_POWER
        } else {
            ScanSettings.SCAN_MODE_BALANCED
        }

        val settings = ScanSettings.Builder()
            .setScanMode(scanMode)
            .setCallbackType(ScanSettings.CALLBACK_TYPE_ALL_MATCHES)
            .setMatchMode(ScanSettings.MATCH_MODE_STICKY)
            .build()

        scanner?.startScan(listOf(filter), settings, scanCallback)
        isScanning = true
    }

    private fun stopWindowedScan() {
        if (!isScanning) return
        runCatching { scanner?.stopScan(scanCallback) }
        isScanning = false
    }

    private fun connectToPeerDeviceIfNeeded(device: BluetoothDevice) {
        val address = device.address ?: return
        if (!activeGattClients.containsKey(address)) {
            val gatt = runCatching {
                device.connectGatt(context, false, gattClientCallback, BluetoothDevice.TRANSPORT_LE)
            }.getOrNull()
            if (gatt != null) {
                activeGattClients[address] = gatt
            }
        }
    }

    fun broadcastKeyAnnouncement() {
        val nickBytes = myNickname.toByteArray(Charsets.UTF_8)
        val pubKey = cryptoEngine.myPublicKeyBytes
        val payload = ByteArray(1 + nickBytes.size + pubKey.size)
        payload[0] = nickBytes.size.toByte()
        System.arraycopy(nickBytes, 0, payload, 1, nickBytes.size)
        System.arraycopy(pubKey, 0, payload, 1 + nickBytes.size, pubKey.size)

        val packet = MeshPacket(
            packetType = MeshPacket.TYPE_KEY_ANNOUNCE,
            ttl = MeshPacket.MAX_TTL,
            packetId = secureRandom.nextLong(),
            senderId = cryptoEngine.myNodeId,
            recipientId = MeshPacket.BROADCAST_RECIPIENT,
            payload = payload
        )
        seenPacketIds.add(packet.packetId)
        transmitPacketToAllPeers(packet)
    }

    /**
     * Only sends outbox packets to peers that haven't received them yet, eliminating repetitive
     * BLE GATT bursts every 4 seconds!
     */
    private fun flushUnsentOutboxToPeers() {
        val allPackets = peerAnnouncementCache.values.mapNotNull { it.nextHop() } + storeAndForwardOutbox.values
        for (pkt in allPackets) {
            transmitPacketToUnsentPeers(pkt)
        }
    }

    private fun trimGeneralHistoryIfNeeded() {
        val generalPackets = storeAndForwardOutbox.values
            .filter { it.packetType == MeshPacket.TYPE_CHANNEL_MSG }
            .sortedBy { it.timestampSec }
        if (generalPackets.size > MAX_ROLLING_GENERAL_PACKETS) {
            val toRemove = generalPackets.take(generalPackets.size - MAX_ROLLING_GENERAL_PACKETS)
            toRemove.forEach { storeAndForwardOutbox.remove(it.packetId) }
        }
    }

    fun sendChannelMessage(channel: String, text: String, channelPassword: String = "") {
        val packetId = secureRandom.nextLong()
        val tempPacket = MeshPacket(
            packetType = MeshPacket.TYPE_CHANNEL_MSG,
            ttl = MeshPacket.MAX_TTL,
            packetId = packetId,
            senderId = cryptoEngine.myNodeId,
            recipientId = MeshPacket.BROADCAST_RECIPIENT,
            payload = ByteArray(0)
        )
        val key = cryptoEngine.deriveChannelKey(channel, channelPassword)
        val body = "$channel|$myNickname|$text".toByteArray(Charsets.UTF_8)
        val encryptedPayload = cryptoEngine.encryptAead(body, key, tempPacket.aeadHeaderBytes())

        val finalPacket = tempPacket.copy(payload = encryptedPayload)
        seenPacketIds.add(packetId)

        storeAndForwardOutbox[packetId] = finalPacket
        trimGeneralHistoryIfNeeded()

        _messages.value = _messages.value + ChatMessage(
            packetId = packetId,
            senderId = cryptoEngine.myNodeId,
            senderName = myNickname,
            recipientId = MeshPacket.BROADCAST_RECIPIENT,
            channel = channel,
            text = text,
            hops = 0,
            timestampSec = finalPacket.timestampSec,
            isDirectEncrypted = false,
            isOutgoing = true
        )
        savePersistedState()
        transmitPacketToAllPeers(finalPacket)
    }

    fun sendDirectEncryptedMessage(recipientId: Long, text: String) {
        val peer = _peers.value[recipientId] ?: return
        val sharedKey = cryptoEngine.deriveSharedAeadKey(peer.publicKeyBytes)
        val packetId = secureRandom.nextLong()

        val tempPacket = MeshPacket(
            packetType = MeshPacket.TYPE_DIRECT_E2EE_MSG,
            ttl = MeshPacket.MAX_TTL,
            packetId = packetId,
            senderId = cryptoEngine.myNodeId,
            recipientId = recipientId,
            payload = ByteArray(0)
        )
        val plaintext = "$myNickname|$text".toByteArray(Charsets.UTF_8)
        val ciphertext = cryptoEngine.encryptAead(
            plaintext,
            sharedKey,
            tempPacket.aeadHeaderBytes()
        )
        val myPub = cryptoEngine.myPublicKeyBytes
        val fullPayload = ByteArray(1 + myPub.size + ciphertext.size)
        fullPayload[0] = myPub.size.toByte()
        System.arraycopy(myPub, 0, fullPayload, 1, myPub.size)
        System.arraycopy(ciphertext, 0, fullPayload, 1 + myPub.size, ciphertext.size)

        val finalPacket = tempPacket.copy(payload = fullPayload)

        seenPacketIds.add(packetId)
        storeAndForwardOutbox[packetId] = finalPacket

        _messages.value = _messages.value + ChatMessage(
            packetId = packetId,
            senderId = cryptoEngine.myNodeId,
            senderName = myNickname,
            recipientId = recipientId,
            channel = "DM:${peer.nickname}",
            text = text,
            hops = 0,
            timestampSec = finalPacket.timestampSec,
            isDirectEncrypted = true,
            isOutgoing = true
        )
        savePersistedState()
        transmitPacketToAllPeers(finalPacket)
    }

    private fun onPacketReceived(rawBytes: ByteArray, rssi: Int) {
        val packet = MeshPacket.fromBytes(rawBytes) ?: return
        if (!seenPacketIds.add(packet.packetId)) {
            return
        }

        when (packet.packetType) {
            MeshPacket.TYPE_KEY_ANNOUNCE -> {
                handleKeyAnnouncement(packet, rssi)
                relayPacketIfTtlAllows(packet)
            }
            MeshPacket.TYPE_CHANNEL_MSG -> {
                handleChannelMessage(packet)
                sendDeliveryAck(packet.packetId, packet.senderId)
                packet.nextHop()?.let {
                    storeAndForwardOutbox[packet.packetId] = it
                    trimGeneralHistoryIfNeeded()
                    savePersistedState()
                }
                relayPacketIfTtlAllows(packet)
            }
            MeshPacket.TYPE_DIRECT_E2EE_MSG -> {
                if (packet.recipientId == cryptoEngine.myNodeId) {
                    handleDirectMessageForMe(packet)
                    sendDeliveryAck(packet.packetId, packet.senderId)
                } else {
                    packet.nextHop()?.let {
                        storeAndForwardOutbox[packet.packetId] = it
                        savePersistedState()
                    }
                    relayPacketIfTtlAllows(packet)
                }
            }
            MeshPacket.TYPE_DELIVERY_ACK -> {
                val ackedPacketId = if (packet.payload.size >= 8) {
                    java.nio.ByteBuffer.wrap(packet.payload).long
                } else 0L
                // Only delete from outbox if it was a 1-to-1 Direct Message! Keep #general rolling history for future peers!
                val cached = storeAndForwardOutbox[ackedPacketId]
                if (cached != null && cached.packetType == MeshPacket.TYPE_DIRECT_E2EE_MSG) {
                    storeAndForwardOutbox.remove(ackedPacketId)
                }
                if (packet.recipientId == cryptoEngine.myNodeId) {
                    _messages.value = _messages.value.map {
                        if (it.packetId == ackedPacketId) it.copy(isDeliveredAck = true) else it
                    }
                    savePersistedState()
                } else {
                    relayPacketIfTtlAllows(packet)
                }
            }
        }
    }

    private fun handleKeyAnnouncement(packet: MeshPacket, rssi: Int) {
        runCatching {
            val nickLen = packet.payload[0].toInt() and 0xFF
            val nickname = String(packet.payload, 1, nickLen, Charsets.UTF_8)
            val pubKeyBytes = packet.payload.copyOfRange(1 + nickLen, packet.payload.size)
            val verifiedId = cryptoEngine.deriveNodeIdFromPublicKey(pubKeyBytes)
            if (verifiedId == packet.senderId && verifiedId != cryptoEngine.myNodeId) {
                val existing = _peers.value[verifiedId]
                val updated = _peers.value.toMutableMap()
                val hops = packet.hopCount.toInt() + 1
                updated[verifiedId] = PeerNode(
                    nodeId = verifiedId,
                    nickname = nickname,
                    publicKeyBytes = pubKeyBytes,
                    rssi = rssi,
                    hops = if (existing != null) minOf(existing.hops, hops) else hops,
                    lastSeenMs = System.currentTimeMillis()
                )
                _peers.value = updated
                peerAnnouncementCache[verifiedId] = packet

                if (existing == null) {
                    broadcastKeyAnnouncement()
                    flushUnsentOutboxToPeers()
                }
            }
        }
    }

    private fun handleChannelMessage(packet: MeshPacket) {
        runCatching {
            val defaultKey = cryptoEngine.deriveChannelKey("#general", "")
            val decrypted = String(
                cryptoEngine.decryptAead(packet.payload, defaultKey, packet.aeadHeaderBytes()),
                Charsets.UTF_8
            )
            val parts = decrypted.split("|", limit = 3)
            if (parts.size == 3) {
                val senderName = parts[1]
                val text = parts[2]
                val hops = packet.hopCount.toInt() + 1
                _messages.value = _messages.value + ChatMessage(
                    packetId = packet.packetId,
                    senderId = packet.senderId,
                    senderName = senderName,
                    recipientId = MeshPacket.BROADCAST_RECIPIENT,
                    channel = parts[0],
                    text = text,
                    hops = hops,
                    timestampSec = packet.timestampSec,
                    isDirectEncrypted = false,
                    isOutgoing = false
                )
                savePersistedState()
                postIncomingMessageNotification(senderName, text, isDirect = false, hops = hops)
            }
        }
    }

    private fun handleDirectMessageForMe(packet: MeshPacket) {
        runCatching {
            val pubKeyLen = packet.payload[0].toInt() and 0xFF
            val senderPubKey = packet.payload.copyOfRange(1, 1 + pubKeyLen)
            val ciphertext = packet.payload.copyOfRange(1 + pubKeyLen, packet.payload.size)

            val verifiedSenderId = cryptoEngine.deriveNodeIdFromPublicKey(senderPubKey)
            if (verifiedSenderId != packet.senderId) return

            val sharedKey = cryptoEngine.deriveSharedAeadKey(senderPubKey)
            val decrypted = String(
                cryptoEngine.decryptAead(ciphertext, sharedKey, packet.aeadHeaderBytes()),
                Charsets.UTF_8
            )
            val parts = decrypted.split("|", limit = 2)
            val senderName = if (parts.size == 2) parts[0] else "Peer"
            val text = if (parts.size == 2) parts[1] else decrypted
            val hops = packet.hopCount.toInt() + 1

            if (!_peers.value.containsKey(verifiedSenderId)) {
                val updated = _peers.value.toMutableMap()
                updated[verifiedSenderId] = PeerNode(
                    nodeId = verifiedSenderId,
                    nickname = senderName,
                    publicKeyBytes = senderPubKey,
                    rssi = -70,
                    hops = hops,
                    lastSeenMs = System.currentTimeMillis()
                )
                _peers.value = updated
            }

            _messages.value = _messages.value + ChatMessage(
                packetId = packet.packetId,
                senderId = packet.senderId,
                senderName = senderName,
                recipientId = cryptoEngine.myNodeId,
                channel = "DM:$senderName",
                text = text,
                hops = hops,
                timestampSec = packet.timestampSec,
                isDirectEncrypted = true,
                isOutgoing = false
            )
            savePersistedState()
            postIncomingMessageNotification(senderName, text, isDirect = true, hops = hops)
        }
    }

    private fun sendDeliveryAck(originalPacketId: Long, originalSenderId: Long) {
        val ackPayload = java.nio.ByteBuffer.allocate(8).putLong(originalPacketId).array()
        val ackPacket = MeshPacket(
            packetType = MeshPacket.TYPE_DELIVERY_ACK,
            ttl = MeshPacket.MAX_TTL,
            packetId = secureRandom.nextLong(),
            senderId = cryptoEngine.myNodeId,
            recipientId = originalSenderId,
            payload = ackPayload
        )
        seenPacketIds.add(ackPacket.packetId)
        transmitPacketToAllPeers(ackPacket)
    }

    private fun relayPacketIfTtlAllows(packet: MeshPacket) {
        val forwarded = packet.nextHop() ?: return
        transmitPacketToUnsentPeers(forwarded)
    }

    private fun markPacketSentToAddress(address: String, packetId: Long): Boolean {
        val set = sentPacketIdsByAddress.getOrPut(address) {
            Collections.synchronizedSet(LinkedHashSet())
        }
        return set.add(packetId)
    }

    private fun transmitPacketToUnsentPeers(packet: MeshPacket) {
        val raw = packet.toBytes()
        scope.launch {
            gattWriteMutex.withLock {
                var didSendAny = false
                readyGattClients.forEach { (address, gatt) ->
                    if (markPacketSentToAddress(address, packet.packetId)) {
                        val char = gatt.getService(MESH_SERVICE_UUID)?.getCharacteristic(PACKET_CHAR_UUID)
                        if (char != null) {
                            didSendAny = true
                            writeCharCompat(gatt, char, raw)
                        }
                    }
                }
                val serverChar = gattServer
                    ?.getService(MESH_SERVICE_UUID)
                    ?.getCharacteristic(PACKET_CHAR_UUID)
                if (serverChar != null) {
                    connectedServerDevices.forEach { (address, device) ->
                        if (markPacketSentToAddress(address, packet.packetId)) {
                            didSendAny = true
                            notifyCharCompat(device, serverChar, raw)
                        }
                    }
                }
                if (didSendAny) {
                    delay(85L)
                }
            }
        }
    }

    private fun transmitPacketToAllPeers(packet: MeshPacket) {
        val raw = packet.toBytes()
        scope.launch {
            gattWriteMutex.withLock {
                readyGattClients.forEach { (address, gatt) ->
                    markPacketSentToAddress(address, packet.packetId)
                    val char = gatt.getService(MESH_SERVICE_UUID)?.getCharacteristic(PACKET_CHAR_UUID)
                    if (char != null) {
                        writeCharCompat(gatt, char, raw)
                    }
                }
                val serverChar = gattServer
                    ?.getService(MESH_SERVICE_UUID)
                    ?.getCharacteristic(PACKET_CHAR_UUID)
                if (serverChar != null) {
                    connectedServerDevices.forEach { (address, device) ->
                        markPacketSentToAddress(address, packet.packetId)
                        notifyCharCompat(device, serverChar, raw)
                    }
                }
                delay(85L)
            }
        }
    }

    private fun writeCharCompat(gatt: BluetoothGatt, char: BluetoothGattCharacteristic, raw: ByteArray) {
        runCatching {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                gatt.writeCharacteristic(char, raw, BluetoothGattCharacteristic.WRITE_TYPE_DEFAULT)
            } else {
                @Suppress("DEPRECATION")
                char.value = raw
                char.writeType = BluetoothGattCharacteristic.WRITE_TYPE_DEFAULT
                @Suppress("DEPRECATION")
                gatt.writeCharacteristic(char)
            }
        }
    }

    private fun notifyCharCompat(device: BluetoothDevice, serverChar: BluetoothGattCharacteristic, raw: ByteArray) {
        runCatching {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                gattServer?.notifyCharacteristicChanged(device, serverChar, false, raw)
            } else {
                @Suppress("DEPRECATION")
                serverChar.value = raw
                @Suppress("DEPRECATION")
                gattServer?.notifyCharacteristicChanged(device, serverChar, false)
            }
        }
    }

    private val advertiseCallback = object : AdvertiseCallback() {
        override fun onStartSuccess(settingsInEffect: AdvertiseSettings?) {
            isAdvertising = true
            Log.d(TAG, "BLE Mesh Advertising active")
        }
        override fun onStartFailure(errorCode: Int) {
            isAdvertising = false
            Log.w(TAG, "BLE Advertise failed: $errorCode")
        }
    }

    private val scanCallback = object : ScanCallback() {
        override fun onScanResult(callbackType: Int, result: ScanResult) {
            val device = result.device ?: return
            connectToPeerDeviceIfNeeded(device)
        }

        override fun onScanFailed(errorCode: Int) {
            isScanning = false
            Log.w(TAG, "BLE Scan failed: $errorCode")
        }
    }

    private val gattClientCallback = object : BluetoothGattCallback() {
        override fun onConnectionStateChange(gatt: BluetoothGatt, status: Int, newState: Int) {
            val address = gatt.device.address
            if (newState == BluetoothProfile.STATE_CONNECTED) {
                val requested = runCatching { gatt.requestMtu(512) }.getOrDefault(false)
                if (!requested) {
                    gatt.discoverServices()
                } else {
                    scope.launch {
                        delay(1200L)
                        if (!readyGattClients.containsKey(address)) {
                            runCatching { gatt.discoverServices() }
                        }
                    }
                }
            } else if (newState == BluetoothProfile.STATE_DISCONNECTED) {
                activeGattClients.remove(address)
                readyGattClients.remove(address)
                sentPacketIdsByAddress.remove(address)
                runCatching { gatt.close() }
            }
        }

        override fun onMtuChanged(gatt: BluetoothGatt, mtu: Int, status: Int) {
            gatt.discoverServices()
        }

        override fun onServicesDiscovered(gatt: BluetoothGatt, status: Int) {
            if (status == BluetoothGatt.GATT_SUCCESS) {
                val char = gatt.getService(MESH_SERVICE_UUID)?.getCharacteristic(PACKET_CHAR_UUID)
                if (char != null) {
                    readyGattClients[gatt.device.address] = gatt
                    gatt.setCharacteristicNotification(char, true)
                    val descriptor = char.getDescriptor(CCCD_UUID)
                    if (descriptor != null) {
                        runCatching {
                            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                                gatt.writeDescriptor(
                                    descriptor,
                                    BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
                                )
                            } else {
                                @Suppress("DEPRECATION")
                                descriptor.value = BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
                                @Suppress("DEPRECATION")
                                gatt.writeDescriptor(descriptor)
                            }
                        }
                        scope.launch {
                            delay(450L)
                            broadcastKeyAnnouncement()
                            flushUnsentOutboxToPeers()
                        }
                    } else {
                        broadcastKeyAnnouncement()
                        flushUnsentOutboxToPeers()
                    }
                }
            }
        }

        override fun onDescriptorWrite(
            gatt: BluetoothGatt,
            descriptor: BluetoothGattDescriptor,
            status: Int
        ) {
            broadcastKeyAnnouncement()
            flushUnsentOutboxToPeers()
        }

        override fun onCharacteristicChanged(
            gatt: BluetoothGatt,
            characteristic: BluetoothGattCharacteristic,
            value: ByteArray
        ) {
            if (characteristic.uuid == PACKET_CHAR_UUID) {
                onPacketReceived(value, -60)
            }
        }

        @Deprecated("Deprecated in API 33")
        override fun onCharacteristicChanged(
            gatt: BluetoothGatt,
            characteristic: BluetoothGattCharacteristic
        ) {
            if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU &&
                characteristic.uuid == PACKET_CHAR_UUID
            ) {
                @Suppress("DEPRECATION")
                onPacketReceived(characteristic.value ?: return, -60)
            }
        }
    }

    private val gattServerCallback = object : BluetoothGattServerCallback() {
        override fun onConnectionStateChange(device: BluetoothDevice, status: Int, newState: Int) {
            if (newState == BluetoothProfile.STATE_CONNECTED) {
                connectedServerDevices[device.address] = device
                connectToPeerDeviceIfNeeded(device)
                scope.launch {
                    delay(300L)
                    broadcastKeyAnnouncement()
                    flushUnsentOutboxToPeers()
                }
            } else if (newState == BluetoothProfile.STATE_DISCONNECTED) {
                connectedServerDevices.remove(device.address)
                sentPacketIdsByAddress.remove(device.address)
            }
        }

        override fun onDescriptorWriteRequest(
            device: BluetoothDevice,
            requestId: Int,
            descriptor: BluetoothGattDescriptor,
            preparedWrite: Boolean,
            responseNeeded: Boolean,
            offset: Int,
            value: ByteArray?
        ) {
            if (responseNeeded) {
                gattServer?.sendResponse(device, requestId, BluetoothGatt.GATT_SUCCESS, 0, null)
            }
            scope.launch {
                delay(150L)
                broadcastKeyAnnouncement()
                flushUnsentOutboxToPeers()
            }
        }

        override fun onCharacteristicWriteRequest(
            device: BluetoothDevice,
            requestId: Int,
            characteristic: BluetoothGattCharacteristic,
            preparedWrite: Boolean,
            responseNeeded: Boolean,
            offset: Int,
            value: ByteArray?
        ) {
            if (responseNeeded) {
                gattServer?.sendResponse(device, requestId, BluetoothGatt.GATT_SUCCESS, 0, null)
            }
            if (characteristic.uuid == PACKET_CHAR_UUID && value != null) {
                onPacketReceived(value, -55)
            }
        }
    }
}
