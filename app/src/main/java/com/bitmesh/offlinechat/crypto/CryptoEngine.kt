package com.bitmesh.offlinechat.crypto

import android.util.Base64
import java.nio.ByteBuffer
import java.security.*
import java.security.spec.ECGenParameterSpec
import java.security.spec.X509EncodedKeySpec
import javax.crypto.Cipher
import javax.crypto.KeyAgreement
import javax.crypto.Mac
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.PBEKeySpec
import javax.crypto.spec.SecretKeySpec

/**
 * End-to-End Encryption Engine for BitMesh Offline Chat.
 *
 * Implements:
 * 1. Elliptic-Curve Diffie-Hellman (ECDH / secp256r1 or X25519 on API 31+) Key Agreement
 * 2. HKDF-SHA256 Key Derivation with context binding
 * 3. AES-256-GCM Authenticated Encryption with Associated Data (AEAD)
 *    - 96-bit (12-byte) cryptographic random IV per packet
 *    - 128-bit (16-byte) authentication tag to block tampering by intermediate relay nodes
 * 4. PBKDF2-HMAC-SHA256 Channel Key Derivation for password-protected group channels (e.g. #general)
 */
class CryptoEngine {

    companion object {
        private const val AES_GCM_TRANSFORMATION = "AES/GCM/NoPadding"
        private const val GCM_IV_LENGTH_BYTES = 12
        private const val GCM_TAG_LENGTH_BITS = 128
        private const val HKDF_INFO = "BitMesh-BLE-E2EE-v1"
    }

    private val secureRandom = SecureRandom()
    val identityKeyPair: KeyPair = generateIdentityKeyPair()

    /**
     * Short 8-Byte Fingerprint ID derived from SHA-256(PublicKey)
     * Used as compact senderId / recipientId inside BLE Mesh packets.
     */
    val myNodeId: Long by lazy {
        deriveNodeIdFromPublicKey(identityKeyPair.public.encoded)
    }

    val myPublicKeyBytes: ByteArray
        get() = identityKeyPair.public.encoded

    private fun generateIdentityKeyPair(): KeyPair {
        val kpg = KeyPairGenerator.getInstance("EC")
        kpg.initialize(ECGenParameterSpec("secp256r1"), secureRandom)
        return kpg.generateKeyPair()
    }

    fun deriveNodeIdFromPublicKey(pubKeyEncoded: ByteArray): Long {
        val digest = MessageDigest.getInstance("SHA-256").digest(pubKeyEncoded)
        return ByteBuffer.wrap(digest, 0, 8).long
    }

    fun shortFingerprintHex(nodeId: Long = myNodeId): String {
        return java.lang.Long.toUnsignedString(nodeId, 16).uppercase().padStart(16, '0').take(8)
    }

    /**
     * Performs ECDH Key Agreement with a peer's X.509-encoded public key
     * and derives a 256-bit AES-GCM session key via HKDF-SHA256.
     */
    fun deriveSharedAeadKey(peerPublicKeyEncoded: ByteArray): SecretKeySpec {
        val keyFactory = KeyFactory.getInstance("EC")
        val peerPublicKey = keyFactory.generatePublic(X509EncodedKeySpec(peerPublicKeyEncoded))

        val keyAgreement = KeyAgreement.getInstance("ECDH")
        keyAgreement.init(identityKeyPair.private)
        keyAgreement.doPhase(peerPublicKey, true)
        val rawSharedSecret = keyAgreement.generateSecret()

        // HKDF-Extract & Expand using HMAC-SHA256
        val prk = hmacSha256(ByteArray(32) { 0 }, rawSharedSecret)
        val okm = hmacSha256(prk, HKDF_INFO.toByteArray(Charsets.UTF_8) + byteArrayOf(0x01))
        return SecretKeySpec(okm, 0, 32, "AES")
    }

    /**
     * Derives a 256-bit symmetric key for a public or password-protected #channel.
     */
    fun deriveChannelKey(channelName: String, password: String = ""): SecretKeySpec {
        val salt = MessageDigest.getInstance("SHA-256")
            .digest("BitMesh-Channel-$channelName".toByteArray(Charsets.UTF_8))
        val secretChars = (if (password.isEmpty()) "public-$channelName" else password).toCharArray()
        val spec = PBEKeySpec(secretChars, salt, 10_000, 256)
        val keyBytes = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).encoded
        return SecretKeySpec(keyBytes, "AES")
    }

    /**
     * Encrypts plaintext with AES-256-GCM.
     * Output format: [12-byte IV] + [Ciphertext + 16-byte GCM Auth Tag]
     * Intermediate Bluetooth relay nodes without the key cannot decrypt or modify the message.
     */
    fun encryptAead(
        plaintext: ByteArray,
        secretKey: SecretKeySpec,
        associatedData: ByteArray? = null
    ): ByteArray {
        val iv = ByteArray(GCM_IV_LENGTH_BYTES)
        secureRandom.nextBytes(iv)

        val cipher = Cipher.getInstance(AES_GCM_TRANSFORMATION)
        val spec = GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv)
        cipher.init(Cipher.ENCRYPT_MODE, secretKey, spec)
        if (associatedData != null) {
            cipher.updateAAD(associatedData)
        }
        val ciphertext = cipher.doFinal(plaintext)

        val output = ByteArray(iv.size + ciphertext.size)
        System.arraycopy(iv, 0, output, 0, iv.size)
        System.arraycopy(ciphertext, 0, output, iv.size, ciphertext.size)
        return output
    }

    /**
     * Decrypts and verifies AES-256-GCM payload.
     * Throws GeneralSecurityException if tampered or wrong key.
     */
    fun decryptAead(
        ivAndCiphertext: ByteArray,
        secretKey: SecretKeySpec,
        associatedData: ByteArray? = null
    ): ByteArray {
        require(ivAndCiphertext.size > GCM_IV_LENGTH_BYTES + 16) { "Payload too short for AES-GCM" }
        val iv = ivAndCiphertext.copyOfRange(0, GCM_IV_LENGTH_BYTES)
        val ciphertext = ivAndCiphertext.copyOfRange(GCM_IV_LENGTH_BYTES, ivAndCiphertext.size)

        val cipher = Cipher.getInstance(AES_GCM_TRANSFORMATION)
        val spec = GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv)
        cipher.init(Cipher.DECRYPT_MODE, secretKey, spec)
        if (associatedData != null) {
            cipher.updateAAD(associatedData)
        }
        return cipher.doFinal(ciphertext)
    }

    private fun hmacSha256(key: ByteArray, data: ByteArray): ByteArray {
        val mac = Mac.getInstance("HmacSHA256")
        mac.init(SecretKeySpec(key, "HmacSHA256"))
        return mac.doFinal(data)
    }

    fun exportPublicKeyBase64(): String =
        Base64.encodeToString(myPublicKeyBytes, Base64.NO_WRAP)
}
