package com.bitmesh.offlinechat.ui

import android.Manifest
import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothManager
import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.ServiceConnection
import android.os.Build
import android.os.Bundle
import android.os.IBinder
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.bitmesh.offlinechat.mesh.BleMeshManager
import com.bitmesh.offlinechat.mesh.MeshForegroundService
import com.bitmesh.offlinechat.mesh.PeerNode
import com.bitmesh.offlinechat.share.OfflineApkSharer

class MainActivity : ComponentActivity() {

    private var meshManager by mutableStateOf<BleMeshManager?>(null)
    private var isBound = false

    private val connection = object : ServiceConnection {
        override fun onServiceConnected(name: ComponentName?, service: IBinder?) {
            val binder = service as? MeshForegroundService.LocalBinder
            meshManager = binder?.meshManager
            meshManager?.startMesh()
            isBound = true
        }
        override fun onServiceDisconnected(name: ComponentName?) {
            meshManager = null
            isBound = false
        }
    }

    private val enableBtLauncher = registerForActivityResult(
        ActivityResultContracts.StartActivityForResult()
    ) {
        meshManager?.startMesh()
    }

    private val permissionLauncher = registerForActivityResult(
        ActivityResultContracts.RequestMultiplePermissions()
    ) {
        startAndBindMeshService()
        promptEnableBluetoothIfNeeded()
        meshManager?.startMesh()
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        requestBlePermissions()

        setContent {
            MaterialTheme(
                colorScheme = darkColorScheme(
                    primary = Color(0xFF10B981),
                    surface = Color(0xFF111827),
                    background = Color(0xFF0B0F19)
                )
            ) {
                Surface(modifier = Modifier.fillMaxSize(), color = MaterialTheme.colorScheme.background) {
                    val manager = meshManager
                    if (manager == null) {
                        Box(Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                            Text("Starting Offline BLE Mesh & E2EE Engine...", color = Color.White)
                        }
                    } else {
                        BitMeshScreen(
                            manager = manager,
                            onRefreshMesh = {
                                requestBlePermissions()
                                manager.startMesh()
                            },
                            onShareApk = { OfflineApkSharer.shareInstalledApkOffline(this) }
                        )
                    }
                }
            }
        }
    }

    override fun onResume() {
        super.onResume()
        meshManager?.startMesh()
    }

    private fun promptEnableBluetoothIfNeeded() {
        runCatching {
            val bm = getSystemService(Context.BLUETOOTH_SERVICE) as BluetoothManager
            val adapter = bm.adapter
            if (adapter != null && !adapter.isEnabled) {
                enableBtLauncher.launch(Intent(BluetoothAdapter.ACTION_REQUEST_ENABLE))
            }
        }
    }

    private fun requestBlePermissions() {
        val perms = mutableListOf(
            Manifest.permission.ACCESS_FINE_LOCATION,
            Manifest.permission.ACCESS_COARSE_LOCATION
        )
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
            perms.add(Manifest.permission.BLUETOOTH_SCAN)
            perms.add(Manifest.permission.BLUETOOTH_ADVERTISE)
            perms.add(Manifest.permission.BLUETOOTH_CONNECT)
        }
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            perms.add(Manifest.permission.POST_NOTIFICATIONS)
        }
        permissionLauncher.launch(perms.toTypedArray())
    }

    private fun startAndBindMeshService() {
        val intent = Intent(this, MeshForegroundService::class.java)
        startForegroundService(intent)
        bindService(intent, connection, Context.BIND_AUTO_CREATE)
    }

    override fun onDestroy() {
        if (isBound) unbindService(connection)
        super.onDestroy()
    }
}

@Composable
fun BitMeshScreen(
    manager: BleMeshManager,
    onRefreshMesh: () -> Unit,
    onShareApk: () -> Unit
) {
    val peers by manager.peers.collectAsState()
    val messages by manager.messages.collectAsState()
    val stats by manager.meshStats.collectAsState()

    var selectedPeer by remember { mutableStateOf<PeerNode?>(null) }
    var inputText by remember { mutableStateOf("") }

    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(12.dp)
    ) {
        // Top Status Card with weighted text column so buttons never get squished
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .background(Color(0xFF1F2937), RoundedCornerShape(10.dp))
                .padding(12.dp)
        ) {
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically
            ) {
                Column(modifier = Modifier.weight(1f)) {
                    Text(
                        text = "BITMESH // OFFLINE BLE E2EE",
                        color = Color(0xFF10B981),
                        fontFamily = FontFamily.Monospace,
                        fontWeight = FontWeight.Bold,
                        fontSize = 14.sp
                    )
                    Spacer(modifier = Modifier.height(2.dp))
                    Text(
                        text = "$stats • Peers: ${peers.size}",
                        color = Color(0xFFD1D5DB),
                        fontFamily = FontFamily.Monospace,
                        fontSize = 11.sp,
                        maxLines = 2,
                        overflow = TextOverflow.Ellipsis
                    )
                }
                Spacer(modifier = Modifier.width(8.dp))
                Row(horizontalArrangement = Arrangement.spacedBy(6.dp)) {
                    OutlinedButton(
                        onClick = onRefreshMesh,
                        contentPadding = PaddingValues(horizontal = 10.dp, vertical = 6.dp)
                    ) {
                        Text("Refresh", color = Color(0xFF34D399), fontSize = 11.sp)
                    }
                    Button(
                        onClick = onShareApk,
                        colors = ButtonDefaults.buttonColors(containerColor = Color(0xFF2563EB)),
                        contentPadding = PaddingValues(horizontal = 10.dp, vertical = 6.dp)
                    ) {
                        Text("Beam APK", color = Color.White, fontSize = 11.sp)
                    }
                }
            }
        }

        Spacer(modifier = Modifier.height(8.dp))

        // Nearby & Multi-Hop Peers Selector Strip
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            FilterChip(
                selected = selectedPeer == null,
                onClick = { selectedPeer = null },
                label = { Text("#general (Broadcast)", color = Color.White) }
            )
            peers.values.forEach { peer ->
                FilterChip(
                    selected = selectedPeer?.nodeId == peer.nodeId,
                    onClick = { selectedPeer = peer },
                    label = {
                        Text(
                            "🔒 ${peer.nickname} (${peer.hops} hop${if (peer.hops > 1) "s" else ""})",
                            color = Color.White
                        )
                    }
                )
            }
        }

        Spacer(modifier = Modifier.height(8.dp))

        // Encrypted Chat Feed
        LazyColumn(
            modifier = Modifier
                .weight(1f)
                .fillMaxWidth()
                .background(Color(0xFF111827), RoundedCornerShape(10.dp))
                .padding(10.dp),
            verticalArrangement = Arrangement.spacedBy(6.dp)
        ) {
            items(messages) { msg ->
                val badge = if (msg.isDirectEncrypted) "🔒 E2EE-DM" else msg.channel
                val hopTag = if (msg.hops == 0) "local" else "${msg.hops}-hop relay"
                val ackTag = if (msg.isDeliveredAck) " ✓✓ ACK" else ""
                Column(
                    modifier = Modifier
                        .fillMaxWidth()
                        .background(
                            if (msg.isOutgoing) Color(0xFF064E3B) else Color(0xFF1F2937),
                            RoundedCornerShape(8.dp)
                        )
                        .padding(8.dp)
                ) {
                    Text(
                        text = "[$badge] <${msg.senderName}> ($hopTag)$ackTag",
                        color = Color(0xFF34D399),
                        fontFamily = FontFamily.Monospace,
                        fontSize = 11.sp,
                        fontWeight = FontWeight.Bold
                    )
                    Text(
                        text = msg.text,
                        color = Color.White,
                        fontFamily = FontFamily.Monospace,
                        fontSize = 13.sp
                    )
                }
            }
        }

        Spacer(modifier = Modifier.height(8.dp))

        // Message Composer
        Row(
            modifier = Modifier.fillMaxWidth(),
            verticalAlignment = Alignment.CenterVertically
        ) {
            OutlinedTextField(
                value = inputText,
                onValueChange = { inputText = it },
                modifier = Modifier.weight(1f),
                colors = OutlinedTextFieldDefaults.colors(
                    focusedTextColor = Color.White,
                    unfocusedTextColor = Color.White,
                    cursorColor = Color(0xFF10B981),
                    focusedBorderColor = Color(0xFF10B981),
                    unfocusedBorderColor = Color(0xFF374151)
                ),
                placeholder = {
                    Text(
                        if (selectedPeer == null) "Encrypted message to #general..."
                        else "ECDH + AES-256-GCM DM to ${selectedPeer!!.nickname}...",
                        color = Color(0xFF9CA3AF)
                    )
                },
                singleLine = true
            )
            Spacer(modifier = Modifier.width(8.dp))
            Button(
                onClick = {
                    val trimmed = inputText.trim()
                    if (trimmed.isNotEmpty()) {
                        val target = selectedPeer
                        if (target == null) {
                            manager.sendChannelMessage("#general", trimmed)
                        } else {
                            manager.sendDirectEncryptedMessage(target.nodeId, trimmed)
                        }
                        inputText = ""
                    }
                }
            ) {
                Text("Send")
            }
        }
    }
}
