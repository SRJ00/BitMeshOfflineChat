# BitMesh Offline v1.4 — Long-Distance Multi-Hop Android BLE Mesh Chat

A complete native Android (Kotlin + Jetpack Compose) offline peer-to-peer encrypted chat application inspired by [bitchat-android](https://github.com/permissionlesstech/bitchat-android).

## What's New in v1.4
1. **Generalized 64-Hop Long-Distance Jumping (1 km to 5+ km with intermediate nodes)**: Increased `MAX_TTL` from `7` to `64` hops across all packet types (`KEY_ANNOUNCE`, `CHANNEL_MSG`, `DIRECT_E2EE_MSG`, `DELIVERY_ACK`), added `peerAnnouncementCache` so intermediate nodes propagate distant users' public keys and names across the entire chain, and embedded the sender's public key in Direct Message envelopes so multi-hop DMs always decrypt even if they arrive ahead of a key announcement.
2. **Custom Personal Display Name (`Name` Button / Tap Header)**: Tap your name in the top bar or the **Name** button to change `Node-XXXX` to your personal name (e.g., `Mansi`, `Rahul`). Saved persistently in `SharedPreferences` and broadcast live across the mesh.
3. **Clean Message Input Placeholder**: Replaced technical crypto text with `"Type your message here..."`.
4. **High-Priority Incoming Message Notifications**: Added a `NotificationManager.IMPORTANCE_HIGH` channel (`bitmesh_incoming_messages`) with sound, vibration, and heads-up banner alerts whenever a `#general` or private message arrives.
5. **Custom Cyber-Emerald Mesh App Launcher Icon**: Added adaptive vector icons (`mipmap-anydpi-v26`) and high-DPI PNG launcher icons (`mipmap-mdpi` to `mipmap-xxxhdpi`).
