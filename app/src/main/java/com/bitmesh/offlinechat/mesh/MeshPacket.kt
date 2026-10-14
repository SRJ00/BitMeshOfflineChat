package com.bitmesh.offlinechat.mesh

import java.nio.ByteBuffer

data class MeshPacket(
    val version: Byte = 1,
    val packetType: Byte,
    val ttl: Byte = MAX_TTL,
    val hopCount: Byte = 0,
    val packetId: Long,
    val senderId: Long,
    val recipientId: Long,
    val timestampSec: Long = System.currentTimeMillis() / 1000L,
    val payload: ByteArray
) {
    companion object {
        const val HEADER_SIZE = 36
        const val MAX_TTL: Byte = 64

        const val TYPE_KEY_ANNOUNCE: Byte = 0x01
        const val TYPE_CHANNEL_MSG: Byte = 0x02
        const val TYPE_DIRECT_E2EE_MSG: Byte = 0x03
        const val TYPE_DELIVERY_ACK: Byte = 0x04

        const val BROADCAST_RECIPIENT: Long = 0L

        fun fromBytes(bytes: ByteArray): MeshPacket? {
            if (bytes.size < HEADER_SIZE) return null
            val buf = ByteBuffer.wrap(bytes)
            val version = buf.get()
            val packetType = buf.get()
            val ttl = buf.get()
            val hopCount = buf.get()
            val packetId = buf.long
            val senderId = buf.long
            val recipientId = buf.long
            val timestampSec = buf.long
            val payload = ByteArray(bytes.size - HEADER_SIZE)
            buf.get(payload)
            return MeshPacket(
                version = version,
                packetType = packetType,
                ttl = ttl,
                hopCount = hopCount,
                packetId = packetId,
                senderId = senderId,
                recipientId = recipientId,
                timestampSec = timestampSec,
                payload = payload
            )
        }
    }

    fun toBytes(): ByteArray {
        val buf = ByteBuffer.allocate(HEADER_SIZE + payload.size)
        buf.put(version)
        buf.put(packetType)
        buf.put(ttl)
        buf.put(hopCount)
        buf.putLong(packetId)
        buf.putLong(senderId)
        buf.putLong(recipientId)
        buf.putLong(timestampSec)
        buf.put(payload)
        return buf.array()
    }

    fun aeadHeaderBytes(): ByteArray {
        val buf = ByteBuffer.allocate(25)
        buf.put(packetType)
        buf.putLong(packetId)
        buf.putLong(senderId)
        buf.putLong(recipientId)
        return buf.array()
    }

    fun nextHop(): MeshPacket? {
        if (ttl <= 1) return null
        return copy(
            ttl = (ttl - 1).toByte(),
            hopCount = (hopCount + 1).toByte()
        )
    }
}
