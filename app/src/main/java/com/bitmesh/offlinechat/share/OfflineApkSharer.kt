package com.bitmesh.offlinechat.share

import android.content.Context
import android.content.Intent
import androidx.core.content.FileProvider
import java.io.File

/**
 * Solves the problem: "What if the other person's phone doesn't have the app installed
 * and we have no internet?"
 *
 * Every installed Android app keeps its base APK at [context.applicationInfo.sourceDir].
 * This utility copies the running APK into a shareable cache file and launches Android's
 * native Bluetooth / Quick Share sheet so you can beam the installer `.apk` directly
 * to another Android phone completely offline!
 */
object OfflineApkSharer {

    fun shareInstalledApkOffline(context: Context) {
        val sourceApk = File(context.applicationInfo.sourceDir)
        val exportDir = File(context.cacheDir, "apk").apply { mkdirs() }
        val targetApk = File(exportDir, "BitMesh-Offline-Chat.apk")
        sourceApk.copyTo(targetApk, overwrite = true)

        val apkUri = FileProvider.getUriForFile(
            context,
            "${context.packageName}.fileprovider",
            targetApk
        )

        val shareIntent = Intent(Intent.ACTION_SEND).apply {
            type = "application/vnd.android.package-archive"
            putExtra(Intent.EXTRA_STREAM, apkUri)
            addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
        }
        context.startActivity(
            Intent.createChooser(shareIntent, "Beam BitMesh APK via Bluetooth / Quick Share")
                .addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
        )
    }
}
