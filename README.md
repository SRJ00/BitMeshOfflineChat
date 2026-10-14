# BitMesh Offline v1.5 — Bluetooth Headphone A2DP Audio-Safe BLE Mesh Chat

A complete native Android (Kotlin + Jetpack Compose) offline peer-to-peer encrypted chat application inspired by [bitchat-android](https://github.com/permissionlesstech/bitchat-android).

## What's Fixed in v1.5 (Bluetooth Headphone Music Playback & Persistent Outbox)
1. **Fixed Bluetooth Headphone Audio Stuttering / Gaps**: Replaced continuous `SCAN_MODE_LOW_LATENCY` (which monopolized 100% of the shared 2.4 GHz Bluetooth radio antenna and starved A2DP music streams) with **Windowed Duty-Cycle Scanning** (scans for 3.2s, then rests for 10s–15s) and automatic **Bluetooth Audio Coexistence Detection** (`SCAN_MODE_LOW_POWER` + `ADVERTISE_MODE_LOW_POWER` whenever Bluetooth headphones are connected or music is playing).
2. **Eliminated 4-Second Packet Re-Broadcast Storms**: Added `sentPacketIdsByAddress` so each queued packet is only transmitted once to each connected peer instead of re-flooding the Bluetooth radio every 4 seconds.
3. **Persistent Disk Outbox & Rolling `#general` History**: Undelivered Private Messages and the last 50 `#general` posts are now saved in `SharedPreferences` across phone reboots.
