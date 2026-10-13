package com.bitmesh.offlinechat.mesh

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Intent
import android.os.Binder
import android.os.IBinder
import com.bitmesh.offlinechat.crypto.CryptoEngine

class MeshForegroundService : Service() {

    inner class LocalBinder : Binder() {
        val meshManager: BleMeshManager
            get() = this@MeshForegroundService.bleMeshManager
    }

    private val binder = LocalBinder()
    lateinit var bleMeshManager: BleMeshManager
        private set

    override fun onCreate() {
        super.onCreate()
        val channelId = "bitmesh_service_channel"
        val channel = NotificationChannel(
            channelId,
            "BitMesh Offline BLE Relay",
            NotificationManager.IMPORTANCE_LOW
        )
        val nm = getSystemService(NotificationManager::class.java)
        nm.createNotificationChannel(channel)

        val notification = Notification.Builder(this, channelId)
            .setContentTitle("BitMesh Offline Mesh Active")
            .setContentText("Relaying E2EE Bluetooth mesh packets (no internet required)")
            .setSmallIcon(android.R.drawable.stat_sys_data_bluetooth)
            .build()

        startForeground(1001, notification)

        val crypto = CryptoEngine()
        bleMeshManager = BleMeshManager(applicationContext, crypto)
        bleMeshManager.startMesh()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (::bleMeshManager.isInitialized) {
            bleMeshManager.startMesh()
        }
        return START_STICKY
    }

    override fun onBind(intent: Intent?): IBinder = binder

    override fun onDestroy() {
        bleMeshManager.stopMesh()
        super.onDestroy()
    }
}
