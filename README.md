# BitMesh Offline v1.1 — Android BLE Mesh & E2EE Chat App

A complete native Android (Kotlin + Jetpack Compose) offline peer-to-peer encrypted chat application inspired by [bitchat-android](https://github.com/permissionlesstech/bitchat-android).

## What's Fixed in v1.1
1. **Fixed False "Bluetooth is disabled" Status on Android 12+**: Removed `android:maxSdkVersion="30"` from `BLUETOOTH` and `BLUETOOTH_ADMIN` in `AndroidManifest.xml` (some Android 12+ OEM ROMs like Samsung One UI require the base `BLUETOOTH` permission alongside `BLUETOOTH_CONNECT` to read `BluetoothAdapter.isEnabled`), added a live `BluetoothAdapter.ACTION_STATE_CHANGED` receiver, and added a 4-second auto-recovery loop so the mesh starts the instant permissions are granted.
2. **Fixed Top Bar Button Layout**: Added `Modifier.weight(1f)` to the status column in `MainActivity.kt` so the **Refresh** and **Beam APK** buttons never get squished on narrow screens.
