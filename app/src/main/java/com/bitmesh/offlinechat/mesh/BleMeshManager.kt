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
import android.os.Build
import android.os.ParcelUuid
import android.util.Log
import androidx.core.content.ContextCompat
import com.bitmesh.offlinechat.crypto.CryptoEngine
import com.bitmesh.offlinechat.ui.MainActivity
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
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
 * Dual-Role Android BLE Mesh Engine (v1.4).
 *
 * New in v1.4:
 * 1. Generalized 64-Hop Long-Distance Relaying (1 km to 5+ km across chains of nodes):
 *    - MAX_TTL = 64 on all packets (KEY_ANNOUNCE, CHANNEL_MSG, DIRECT_E2EE_MSG, DELIVERY_ACK).
 *    - Intermediate nodes cache & forward peer KEY_ANNOUNCE packets (peerAnnouncementCache) AND
 *      unacknowledged messages (storeAndForwardOutbox) so distant nodes 1 km+ apart discover each
 *      other's public keys, custom names, and messages across any number of intermediate hops up to 64.
 *    - Self-contained E2EE Direct Message envelope embeds the sender's public key so even if a DM arrives
 *      before KEY_ANNOUNCE over a multi-hop path, the recipient can always verify & decrypt it.
 * 2. Custom Personal Name Configuration:
 *    - Persists the user's custom name in SharedPreferences and immediately broadcasts it across the mesh.
 * 3. High-Priority Heads-Up Push Notifications:
 *    - Posts a banner notification with sound & vibration on the IMPORTANCE_HIGH channel whenever a
 *      new #general or Direct Message arrives.
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
        private const val MSG_NOTIFICATION_CHANNEL_ID = "bitmesh_incoming_messages"

        val MESH_SERVICE_UUID: UUID = UUID.fromString("b17c4a70-0001-4000-8000-00805f9b34fb")
        val PACKET_CHAR_UUID: UUID = UUID.fromString("b17c4a70-0002-4000-8000-00805f9b34fb")
        val CCCD_UUID: UUID = UUID.fromString("00002902-0000-1000-8000-00805f9b34fb")
        private const val MAX_SEEN_PACKETS = 8192
        private const val MAX_OUTBOX_PACKETS = 512
    }

    private val prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
    private val notificationManager = context.getSystemService(NotificationManager::class.java)

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

    // Caches latest KEY_ANNOUNCE packet per distant node so intermediate nodes propagate the mesh directory across 1km+
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
        registerBluetoothStateReceiver()
        scope.launch {
            while (isActive) {
                if (!isAdvertising || !isScanning || gattServer == null) {
                    ensureMeshRunning()
                } else {
                    broadcastKeyAnnouncement()
                    flushOutboxToPeers()
                }
                delay(4_000L)
            }
        }
    }

    private fun loadSavedNickname(): String {
        val defaultName = "Node-${cryptoEngine.shortFingerprintHex().take(4)}"
        return prefs.getString(KEY_NICKNAME, null)?.takeIf { it.isNotBlank() } ?: defaultName
    }

    /**
     * Updates the user's personal display name, saves it persistently, and broadcasts it across all mesh hops.
     */
    fun updateNickname(newName: String) {
        val cleaned = newName.trim().replace("|", "").take(24)
        if (cleaned.isEmpty()) return
        prefs.edit().putString(KEY_NICKNAME, cleaned).apply()
        _myNicknameFlow.value = cleaned
        _meshStats.value = "Online as $cleaned"
        broadcastKeyAnnouncement()
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
            flushOutboxToPeers()
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
            runCatching { startScanning() }
        }

        _meshStats.value = "Online as $myNickname"
    }

    fun stopMesh() {
        if (isReceiverRegistered) {
            runCatching { context.unregisterReceiver(bluetoothStateReceiver) }
            isReceiverRegistered = false
        }
        runCatching { advertiser?.stopAdvertising(advertiseCallback) }
        runCatching { scanner?.stopScan(scanCallback) }
        isAdvertising = false
        isScanning = false
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

    private fun startAdvertising() {
        advertiser = bluetoothAdapter?.bluetoothLeAdvertiser ?: return
        val settings = AdvertiseSettings.Builder()
            .setAdvertiseMode(AdvertiseSettings.ADVERTISE_MODE_LOW_LATENCY)
            .setTxPowerLevel(AdvertiseSettings.ADVERTISE_TX_POWER_HIGH)
            .setConnectable(true)
            .build()

        val data = AdvertiseData.Builder()
            .setIncludeDeviceName(false)
            .addServiceUuid(ParcelUuid(MESH_SERVICE_UUID))
            .build()

        advertiser?.startAdvertising(settings, data, advertiseCallback)
    }

    private fun startScanning() {
        scanner = bluetoothAdapter?.bluetoothLeScanner ?: return
        val filter = ScanFilter.Builder()
            .setServiceUuid(ParcelUuid(MESH_SERVICE_UUID))
            .build()

        val settings = ScanSettings.Builder()
            .setScanMode(ScanSettings.SCAN_MODE_LOW_LATENCY)
            .setCallbackType(ScanSettings.CALLBACK_TYPE_ALL_MATCHES)
            .setMatchMode(ScanSettings.MATCH_MODE_AGGRESSIVE)
            .build()

        scanner?.startScan(listOf(filter), settings, scanCallback)
        isScanning = true
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

    /**
     * Broadcasts our public key + custom nickname with MAX_TTL = 64 hops so nodes 1km+ away
     * across a multi-hop chain discover our name and public key.
     */
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

    private fun flushOutboxToPeers() {
        // Propagate known multi-hop peer announcements first so distant nodes learn public keys
        peerAnnouncementCache.values.forEach { announcePacket ->
            announcePacket.nextHop()?.let { transmitPacketToAllPeers(it) }
        }
        // Then replay any cached messages
        storeAndForwardOutbox.values.forEach { packet ->
            transmitPacketToAllPeers(packet)
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

        if (storeAndForwardOutbox.size < MAX_OUTBOX_PACKETS) {
            storeAndForwardOutbox[packetId] = finalPacket
        }

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
        transmitPacketToAllPeers(finalPacket)
    }

    /**
     * Sends an End-to-End Encrypted Direct Message with MAX_TTL = 64 hops.
     * Embeds the sender's public key in the outer envelope ([1-byte pubKeyLen][senderPubKey][AES-GCM])
     * so even a recipient 1km+ away who receives the DM before KEY_ANNOUNCE can immediately decrypt it!
     */
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
        if (storeAndForwardOutbox.size < MAX_OUTBOX_PACKETS) {
            storeAndForwardOutbox[packetId] = finalPacket
        }

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
        transmitPacketToAllPeers(finalPacket)
    }

    private fun onPacketReceived(rawBytes: ByteArray, rssi: Int) {
        val packet = MeshPacket.fromBytes(rawBytes) ?: return
        if (!seenPacketIds.add(packet.packetId)) {
            return // Already seen/relayed this packetId — prevents broadcast loops
        }

        when (packet.packetType) {
            MeshPacket.TYPE_KEY_ANNOUNCE -> {
                handleKeyAnnouncement(packet, rssi)
                // Relay announcement across up to 64 hops so nodes 1km+ away discover this peer
                relayPacketIfTtlAllows(packet)
            }
            MeshPacket.TYPE_CHANNEL_MSG -> {
                handleChannelMessage(packet)
                sendDeliveryAck(packet.packetId, packet.senderId)
                // Cache & relay across up to 64 hops
                if (storeAndForwardOutbox.size < MAX_OUTBOX_PACKETS) {
                    packet.nextHop()?.let { storeAndForwardOutbox[packet.packetId] = it }
                }
                relayPacketIfTtlAllows(packet)
            }
            MeshPacket.TYPE_DIRECT_E2EE_MSG -> {
                if (packet.recipientId == cryptoEngine.myNodeId) {
                    // Addressed to us! Decrypt and send multi-hop ACK back to sender
                    handleDirectMessageForMe(packet)
                    sendDeliveryAck(packet.packetId, packet.senderId)
                } else {
                    // Addressed to someone further away (e.g. 1km away): cache & jump to next hop!
                    if (storeAndForwardOutbox.size < MAX_OUTBOX_PACKETS) {
                        packet.nextHop()?.let { storeAndForwardOutbox[packet.packetId] = it }
                    }
                    relayPacketIfTtlAllows(packet)
                }
            }
            MeshPacket.TYPE_DELIVERY_ACK -> {
                val ackedPacketId = if (packet.payload.size >= 8) {
                    java.nio.ByteBuffer.wrap(packet.payload).long
                } else 0L
                storeAndForwardOutbox.remove(ackedPacketId)
                if (packet.recipientId == cryptoEngine.myNodeId) {
                    _messages.value = _messages.value.map {
                        if (it.packetId == ackedPacketId) it.copy(isDeliveredAck = true) else it
                    }
                } else {
                    // Relay the ACK back across the multi-hop chain to the original sender!
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
                    flushOutboxToPeers()
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
                postIncomingMessageNotification(senderName, text, isDirect = false, hops = hops)
            }
        }
    }

    private fun handleDirectMessageForMe(packet: MeshPacket) {
        runCatching {
            val pubKeyLen = packet.payload[0].toInt() and 0xFF
            val senderPubKey = packet.payload.copyOfRange(1, 1 + pubKeyLen)
            val ciphertext = packet.payload.copyOfRange(1 + pubKeyLen, packet.payload.size)

            // Verify that senderPubKey matches packet.senderId
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

            // Auto-register sender in peers map if not already present (e.g. across multi-hop chain)
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
        transmitPacketToAllPeers(forwarded)
    }

    private fun transmitPacketToAllPeers(packet: MeshPacket) {
        val raw = packet.toBytes()
        scope.launch {
            gattWriteMutex.withLock {
                readyGattClients.values.forEach { gatt ->
                    val service = gatt.getService(MESH_SERVICE_UUID)
                    val char = service?.getCharacteristic(PACKET_CHAR_UUID)
                    if (char != null) {
                        runCatching {
                            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                                gatt.writeCharacteristic(
                                    char,
                                    raw,
                                    BluetoothGattCharacteristic.WRITE_TYPE_DEFAULT
                                )
                            } else {
                                @Suppress("DEPRECATION")
                                char.value = raw
                                char.writeType = BluetoothGattCharacteristic.WRITE_TYPE_DEFAULT
                                @Suppress("DEPRECATION")
                                gatt.writeCharacteristic(char)
                            }
                        }
                    }
                }
                val serverChar = gattServer
                    ?.getService(MESH_SERVICE_UUID)
                    ?.getCharacteristic(PACKET_CHAR_UUID)
                if (serverChar != null) {
                    connectedServerDevices.values.forEach { device ->
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
                }
                delay(65L)
            }
        }
    }

    private val advertiseCallback = object : AdvertiseCallback() {
        override fun onStartSuccess(settingsInEffect: AdvertiseSettings?) {
            isAdvertising = true
            _meshStats.value = "Online as $myNickname"
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
                            flushOutboxToPeers()
                        }
                    } else {
                        broadcastKeyAnnouncement()
                        flushOutboxToPeers()
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
            flushOutboxToPeers()
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
                    flushOutboxToPeers()
                }
            } else if (newState == BluetoothProfile.STATE_DISCONNECTED) {
                connectedServerDevices.remove(device.address)
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
                flushOutboxToPeers()
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
