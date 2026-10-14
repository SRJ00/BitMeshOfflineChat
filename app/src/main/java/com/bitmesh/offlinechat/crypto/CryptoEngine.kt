package com.bitmesh.offlinechat.crypto

import android.content.Context
import android.util.Base64
import java.nio.ByteBuffer
import java.security.*
import java.security.spec.ECGenParameterSpec
import java.security.spec.PKCS8EncodedKeySpec
import java.security.spec.X509EncodedKeySpec
import javax.crypto.Cipher
import javax.crypto.KeyAgreement
import javax.crypto.Mac
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.PBEKeySpec
import javax.crypto.spec.SecretKeySpec

class CryptoEngine(context: Context? = null) {

    companion object {
        private const val PREFS_NAME = "bitmesh_crypto_prefs"
        private const val KEY_PUB = "identity_pub_b64"
        private const val KEY_PRIV = "identity_priv_b64"
        private const val AES_GCM_TRANSFORMATION = "AES/GCM/NoPadding"
        private const val GCM_IV_LENGTH_BYTES = 12
        private const val GCM_TAG_LENGTH_BITS = 128
        private const val HKDF_INFO = "BitMesh-BLE-E2EE-v1"
    }

    private val secureRandom = SecureRandom()
    val identityKeyPair: KeyPair = loadOrGenerateIdentityKeyPair(context)

    val myNodeId: Long by lazy {
        deriveNodeIdFromPublicKey(identityKeyPair.public.encoded)
    }

    val myPublicKeyBytes: ByteArray
        get() = identityKeyPair.public.encoded

    private fun loadOrGenerateIdentityKeyPair(context: Context?): KeyPair {
        if (context != null) {
            runCatching {
                val prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
                val pubB64 = prefs.getString(KEY_PUB, null)
                val privB64 = prefs.getString(KEY_PRIV, null)
                if (pubB64 != null && privB64 != null) {
                    val kf = KeyFactory.getInstance("EC")
                    val pub = kf.generatePublic(X509EncodedKeySpec(Base64.decode(pubB64, Base64.NO_WRAP)))
                    val priv = kf.generatePrivate(PKCS8EncodedKeySpec(Base64.decode(privB64, Base64.NO_WRAP)))
                    return KeyPair(pub, priv)
                }
            }
        }
        val kpg = KeyPairGenerator.getInstance("EC")
        kpg.initialize(ECGenParameterSpec("secp256r1"), secureRandom)
        val kp = kpg.generateKeyPair()
        if (context != null) {
            runCatching {
                context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE).edit()
                    .putString(KEY_PUB, Base64.encodeToString(kp.public.encoded, Base64.NO_WRAP))
                    .putString(KEY_PRIV, Base64.encodeToString(kp.private.encoded, Base64.NO_WRAP))
                    .apply()
            }
        }
        return kp
    }

    fun deriveNodeIdFromPublicKey(pubKeyEncoded: ByteArray): Long {
        val digest = MessageDigest.getInstance("SHA-256").digest(pubKeyEncoded)
        return ByteBuffer.wrap(digest, 0, 8).long
    }

    fun shortFingerprintHex(nodeId: Long = myNodeId): String {
        return java.lang.Long.toUnsignedString(nodeId, 16).uppercase().padStart(16, '0').take(8)
    }

    fun deriveSharedAeadKey(peerPublicKeyEncoded: ByteArray): SecretKeySpec {
        val keyFactory = KeyFactory.getInstance("EC")
        val peerPublicKey = keyFactory.generatePublic(X509EncodedKeySpec(peerPublicKeyEncoded))

        val keyAgreement = KeyAgreement.getInstance("ECDH")
        keyAgreement.init(identityKeyPair.private)
        keyAgreement.doPhase(peerPublicKey, true)
        val rawSharedSecret = keyAgreement.generateSecret()

        val prk = hmacSha256(ByteArray(32) { 0 }, rawSharedSecret)
        val okm = hmacSha256(prk, HKDF_INFO.toByteArray(Charsets.UTF_8) + byteArrayOf(0x01))
        return SecretKeySpec(okm, 0, 32, "AES")
    }

    fun deriveChannelKey(channelName: String, password: String = ""): SecretKeySpec {
        val salt = MessageDigest.getInstance("SHA-256")
            .digest("BitMesh-Channel-$channelName".toByteArray(Charsets.UTF_8))
        val secretChars = (if (password.isEmpty()) "public-$channelName" else password).toCharArray()
        val spec = PBEKeySpec(secretChars, salt, 10_000, 256)
        val keyBytes = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).encoded
        return SecretKeySpec(keyBytes, "AES")
    }

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
}
