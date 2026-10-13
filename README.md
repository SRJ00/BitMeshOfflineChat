# BitMesh Offline v1.3 — Android BLE Mesh & E2EE Chat App

A complete native Android (Kotlin + Jetpack Compose) offline peer-to-peer encrypted chat application inspired by [bitchat-android](https://github.com/permissionlesstech/bitchat-android).

## What's Fixed in v1.3 (Two-Way BLE Delivery & Clear Status Labels)
1. **Fixed One-Way BLE Delivery**: When two Android phones connect over the same physical BLE link, the second phone's `requestMtu(512)` call can fail to trigger `onMtuChanged` because MTU was already negotiated by the first phone. v1.3 adds an immediate + 1200ms fallback to `gatt.discoverServices()`, waits for `onDescriptorWrite()` before transmitting packets, uses a `Mutex` queue (`gattWriteMutex`) so consecutive packets never collide, and uses the Android 13/14/15 (`API 33+`) byte-array GATT write/notify APIs.
2. **Full Outbox Replay for `#general` & Direct Messages**: Both `#general` broadcast messages and `E2EE-DM` messages are now cached in `storeAndForwardOutbox` and replayed automatically on connection.
3. **Clearer Delivery Badges**: Replaced the confusing `(local)` tag on outgoing messages with `(You • Sent ⏳)` and `(You • Delivered ✓✓)`.
