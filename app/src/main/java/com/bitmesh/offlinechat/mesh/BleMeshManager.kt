package com.bitmesh.offlinechat.mesh

import android.Manifest
import android.annotation.SuppressLint
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
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
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
 * Dual-Role Android BLE Mesh Engine (GATT Server + GATT Client + Multi-Hop Relay + Store-and-Forward).
 *
 * Fixes included in v1.1:
 * 1. Removed android:maxSdkVersion="30" from BLUETOOTH & BLUETOOTH_ADMIN in AndroidManifest.xml so
 *    BluetoothAdapter.isEnabled never throws a SecurityException or returns false on Android 12+ OEM ROMs.
 * 2. Listens to BluetoothAdapter.ACTION_STATE_CHANGED and runs a 3-second watchdog coroutine so
 *    the mesh starts automatically as soon as permissions are granted or Bluetooth is toggled on.
 * 3. Includes standard Client Characteristic Configuration Descriptor (0x2902) for reliable bidirectional GATT notifications.
 */
@SuppressLint("MissingPermission")
class BleMeshManager(
    private val context: Context,
    val cryptoEngine: CryptoEngine,
    var myNickname: String = "Node-${cryptoEngine.shortFingerprintHex().take(4)}"
) {
    companion object {
        private const val TAG = "BleMeshManager"
        val MESH_SERVICE_UUID: UUID = UUID.fromString("b17c4a70-0001-4000-8000-00805f9b34fb")
        val PACKET_CHAR_UUID: UUID = UUID.fromString("b17c4a70-0002-4000-8000-00805f9b34fb")
        val CCCD_UUID: UUID = UUID.fromString("00002902-0000-1000-8000-00805f9b34fb")
        private const val MAX_SEEN_PACKETS = 2048
        private const val MAX_OUTBOX_PACKETS = 256
    }

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

    private val storeAndForwardOutbox = ConcurrentHashMap<Long, MeshPacket>()
    private val activeGattClients = ConcurrentHashMap<String, BluetoothGatt>()
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
        registerBluetoothStateReceiver()
        // Watchdog loop: automatically recovers as soon as permissions are granted or Bluetooth turns on
        scope.launch {
            while (isActive) {
                if (!isAdvertising || !isScanning || gattServer == null) {
                    ensureMeshRunning()
                } else {
                    broadcastKeyAnnouncement()
                }
                delay(4_000L)
            }
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

        _meshStats.value = "Online • ID ${cryptoEngine.shortFingerprintHex().take(6)}"
        broadcastKeyAnnouncement()
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
            .build()

        scanner?.startScan(listOf(filter), settings, scanCallback)
        isScanning = true
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
            ttl = 3,
            packetId = secureRandom.nextLong(),
            senderId = cryptoEngine.myNodeId,
            recipientId = MeshPacket.BROADCAST_RECIPIENT,
            payload = payload
        )
        seenPacketIds.add(packet.packetId)
        transmitPacketToAllPeers(packet)
    }

    fun sendChannelMessage(channel: String, text: String, channelPassword: String = "") {
        val packetId = secureRandom.nextLong()
        val tempPacket = MeshPacket(
            packetType = MeshPacket.TYPE_CHANNEL_MSG,
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

    fun sendDirectEncryptedMessage(recipientId: Long, text: String) {
        val peer = _peers.value[recipientId] ?: return
        val sharedKey = cryptoEngine.deriveSharedAeadKey(peer.publicKeyBytes)
        val packetId = secureRandom.nextLong()

        val tempPacket = MeshPacket(
            packetType = MeshPacket.TYPE_DIRECT_E2EE_MSG,
            packetId = packetId,
            senderId = cryptoEngine.myNodeId,
            recipientId = recipientId,
            payload = ByteArray(0)
        )
        val plaintext = "$myNickname|$text".toByteArray(Charsets.UTF_8)
        val encryptedPayload = cryptoEngine.encryptAead(
            plaintext,
            sharedKey,
            tempPacket.aeadHeaderBytes()
        )
        val finalPacket = tempPacket.copy(payload = encryptedPayload)

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
            return
        }

        when (packet.packetType) {
            MeshPacket.TYPE_KEY_ANNOUNCE -> {
                handleKeyAnnouncement(packet, rssi)
                relayPacketIfTtlAllows(packet)
            }
            MeshPacket.TYPE_CHANNEL_MSG -> {
                handleChannelMessage(packet)
                relayPacketIfTtlAllows(packet)
            }
            MeshPacket.TYPE_DIRECT_E2EE_MSG -> {
                if (packet.recipientId == cryptoEngine.myNodeId) {
                    handleDirectMessageForMe(packet)
                    sendDeliveryAck(packet.packetId, packet.senderId)
                } else {
                    if (storeAndForwardOutbox.size < MAX_OUTBOX_PACKETS) {
                        storeAndForwardOutbox[packet.packetId] = packet
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
                val updated = _peers.value.toMutableMap()
                updated[verifiedId] = PeerNode(
                    nodeId = verifiedId,
                    nickname = nickname,
                    publicKeyBytes = pubKeyBytes,
                    rssi = rssi,
                    hops = packet.hopCount.toInt() + 1,
                    lastSeenMs = System.currentTimeMillis()
                )
                _peers.value = updated

                storeAndForwardOutbox.values.forEach { queued ->
                    transmitPacketToAllPeers(queued)
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
                _messages.value = _messages.value + ChatMessage(
                    packetId = packet.packetId,
                    senderId = packet.senderId,
                    senderName = parts[1],
                    recipientId = MeshPacket.BROADCAST_RECIPIENT,
                    channel = parts[0],
                    text = parts[2],
                    hops = packet.hopCount.toInt() + 1,
                    timestampSec = packet.timestampSec,
                    isDirectEncrypted = false,
                    isOutgoing = false
                )
            }
        }
    }

    private fun handleDirectMessageForMe(packet: MeshPacket) {
        val senderPeer = _peers.value[packet.senderId] ?: return
        runCatching {
            val sharedKey = cryptoEngine.deriveSharedAeadKey(senderPeer.publicKeyBytes)
            val decrypted = String(
                cryptoEngine.decryptAead(packet.payload, sharedKey, packet.aeadHeaderBytes()),
                Charsets.UTF_8
            )
            val parts = decrypted.split("|", limit = 2)
            val senderName = if (parts.size == 2) parts[0] else senderPeer.nickname
            val text = if (parts.size == 2) parts[1] else decrypted
            _messages.value = _messages.value + ChatMessage(
                packetId = packet.packetId,
                senderId = packet.senderId,
                senderName = senderName,
                recipientId = cryptoEngine.myNodeId,
                channel = "DM:$senderName",
                text = text,
                hops = packet.hopCount.toInt() + 1,
                timestampSec = packet.timestampSec,
                isDirectEncrypted = true,
                isOutgoing = false
            )
        }
    }

    private fun sendDeliveryAck(originalPacketId: Long, originalSenderId: Long) {
        val ackPayload = java.nio.ByteBuffer.allocate(8).putLong(originalPacketId).array()
        val ackPacket = MeshPacket(
            packetType = MeshPacket.TYPE_DELIVERY_ACK,
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
        activeGattClients.values.forEach { gatt ->
            val service = gatt.getService(MESH_SERVICE_UUID)
            val char = service?.getCharacteristic(PACKET_CHAR_UUID)
            if (char != null) {
                char.value = raw
                char.writeType = BluetoothGattCharacteristic.WRITE_TYPE_NO_RESPONSE
                runCatching { gatt.writeCharacteristic(char) }
            }
        }
        val serverChar = gattServer
            ?.getService(MESH_SERVICE_UUID)
            ?.getCharacteristic(PACKET_CHAR_UUID)
        if (serverChar != null) {
            serverChar.value = raw
            connectedServerDevices.values.forEach { device ->
                runCatching { gattServer?.notifyCharacteristicChanged(device, serverChar, false) }
            }
        }
    }

    private val advertiseCallback = object : AdvertiseCallback() {
        override fun onStartSuccess(settingsInEffect: AdvertiseSettings?) {
            isAdvertising = true
            _meshStats.value = "Online • ID ${cryptoEngine.shortFingerprintHex().take(6)}"
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
            val address = device.address ?: return
            if (!activeGattClients.containsKey(address)) {
                val gatt = device.connectGatt(context, false, gattClientCallback, BluetoothDevice.TRANSPORT_LE)
                if (gatt != null) {
                    activeGattClients[address] = gatt
                }
            }
        }

        override fun onScanFailed(errorCode: Int) {
            isScanning = false
            Log.w(TAG, "BLE Scan failed: $errorCode")
        }
    }

    private val gattClientCallback = object : BluetoothGattCallback() {
        override fun onConnectionStateChange(gatt: BluetoothGatt, status: Int, newState: Int) {
            if (newState == BluetoothProfile.STATE_CONNECTED) {
                gatt.requestMtu(512)
            } else if (newState == BluetoothProfile.STATE_DISCONNECTED) {
                activeGattClients.remove(gatt.device.address)
                gatt.close()
            }
        }

        override fun onMtuChanged(gatt: BluetoothGatt, mtu: Int, status: Int) {
            gatt.discoverServices()
        }

        override fun onServicesDiscovered(gatt: BluetoothGatt, status: Int) {
            if (status == BluetoothGatt.GATT_SUCCESS) {
                val char = gatt.getService(MESH_SERVICE_UUID)?.getCharacteristic(PACKET_CHAR_UUID)
                if (char != null) {
                    gatt.setCharacteristicNotification(char, true)
                    val descriptor = char.getDescriptor(CCCD_UUID)
                    if (descriptor != null) {
                        descriptor.value = BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
                        runCatching { gatt.writeDescriptor(descriptor) }
                    }
                    broadcastKeyAnnouncement()
                }
            }
        }

        override fun onCharacteristicChanged(
            gatt: BluetoothGatt,
            characteristic: BluetoothGattCharacteristic
        ) {
            if (characteristic.uuid == PACKET_CHAR_UUID) {
                onPacketReceived(characteristic.value ?: return, -60)
            }
        }
    }

    private val gattServerCallback = object : BluetoothGattServerCallback() {
        override fun onConnectionStateChange(device: BluetoothDevice, status: Int, newState: Int) {
            if (newState == BluetoothProfile.STATE_CONNECTED) {
                connectedServerDevices[device.address] = device
                broadcastKeyAnnouncement()
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
