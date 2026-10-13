# BitMesh Offline — Android BLE Mesh & E2EE Chat App

A complete native Android (Kotlin + Jetpack Compose) offline peer-to-peer encrypted chat application inspired by [bitchat-android](https://github.com/permissionlesstech/bitchat-android).

## How to Get the Installable `.apk` File in 2 Minutes

### Method 1: Zero-Setup Cloud APK Build (No PC or Android Studio Needed)
1. Create a free repository on GitHub (you can do this from your Android phone browser).
2. Upload the files from this project archive into the repository.
3. Click the **Actions** tab in your GitHub repository -> select **Build Android APK**.
4. GitHub's cloud servers will automatically run `.github/workflows/build-apk.yml` and produce **`BitMesh-Offline-Chat-APK` (`app-debug.apk`)** ready to download and install on your Android phone!

### Method 2: Local Build with Android Studio
1. Open this folder in Android Studio (or run `./gradlew assembleDebug` in terminal).
2. Install the generated APK at `app/build/outputs/apk/debug/app-debug.apk` onto your Android phone.
3. Once installed on your phone, tap the **"Beam APK"** button inside the app to send the `.apk` directly to friends' Android phones over Bluetooth or Quick Share with zero internet!
