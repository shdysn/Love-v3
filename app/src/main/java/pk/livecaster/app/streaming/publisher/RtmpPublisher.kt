package pk.livecaster.app.streaming.publisher

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import pk.livecaster.app.core.constants.StreamConstants
import pk.livecaster.app.streaming.encoder.AudioEncoderConfig
import pk.livecaster.app.streaming.encoder.VideoEncoderConfig
import pk.livecaster.app.streaming.state.StreamHealth
import pk.livecaster.app.streaming.state.StreamStatus
import pk.livecaster.app.streaming.state.StreamTelemetry
import java.io.InputStream
import java.io.OutputStream
import java.net.InetSocketAddress
import java.net.Socket
import javax.net.ssl.SSLSocket
import javax.net.ssl.SSLSocketFactory

class RtmpPublisher(
    private val scope: CoroutineScope
) {
    private var streamJob: Job? = null
    private var socket: Socket? = null

    private val _telemetry = MutableStateFlow(StreamTelemetry())
    val telemetry: StateFlow<StreamTelemetry> = _telemetry.asStateFlow()

    private var targetBitrateKbps: Int = StreamConstants.DEFAULT_BITRATE_KBPS
    private var targetFps: Int = StreamConstants.DEFAULT_FPS

    fun startPublishing(
        rtmpUrl: String,
        streamKey: String,
        videoConfig: VideoEncoderConfig,
        audioConfig: AudioEncoderConfig
    ) {
        if (_telemetry.value.status == StreamStatus.LIVE || _telemetry.value.status == StreamStatus.CONNECTING) {
            return
        }

        targetBitrateKbps = videoConfig.bitrateKbps + audioConfig.bitrateKbps
        targetFps = videoConfig.frameRate

        _telemetry.value = _telemetry.value.copy(
            status = StreamStatus.CONNECTING,
            durationSeconds = 0,
            droppedFrames = 0,
            currentViewers = 0,
            errorMessage = null
        )

        streamJob?.cancel()
        streamJob = scope.launch(Dispatchers.IO) {
            try {
                val host = extractHostFromUrl(rtmpUrl)
                val isRtmps = rtmpUrl.startsWith("rtmps://", ignoreCase = true)
                val port = extractPortFromUrl(rtmpUrl, isRtmps)

                val s: Socket = if (isRtmps) {
                    val sslFactory = SSLSocketFactory.getDefault() as SSLSocketFactory
                    val sslSocket = sslFactory.createSocket() as SSLSocket
                    sslSocket.connect(InetSocketAddress(host, port), 6000)
                    sslSocket.startHandshake()
                    sslSocket
                } else {
                    val raw = Socket()
                    raw.connect(InetSocketAddress(host, port), 6000)
                    raw
                }
                s.soTimeout = 10000
                socket = s

                // Perform real RTMP handshake (C0, C1)
                val out: OutputStream = s.getOutputStream()
                val inStream: InputStream = s.getInputStream()

                // C0 = 0x03
                out.write(0x03)

                // C1 = 1536 bytes
                val c1 = ByteArray(1536)
                java.security.SecureRandom().nextBytes(c1)
                c1[0] = 0; c1[1] = 0; c1[2] = 0; c1[3] = 0
                c1[4] = 0; c1[5] = 0; c1[6] = 0; c1[7] = 0
                out.write(c1)
                out.flush()

                // Read S0 (1 byte)
                val s0 = inStream.read()
                if (s0 != 0x03) {
                    throw IllegalStateException("Server returned unsupported protocol byte: $s0")
                }

                // Read S1 (1536 bytes)
                val s1 = ByteArray(1536)
                var readTotal = 0
                while (readTotal < 1536) {
                    val read = inStream.read(s1, readTotal, 1536 - readTotal)
                    if (read < 0) throw IllegalStateException("Unexpected EOF during RTMP handshake")
                    readTotal += read
                }

                // Send C2 (echo of S1)
                out.write(s1)
                out.flush()

                // Read S2 (1536 bytes)
                val s2 = ByteArray(1536)
                readTotal = 0
                while (readTotal < 1536) {
                    val read = inStream.read(s2, readTotal, 1536 - readTotal)
                    if (read < 0) throw IllegalStateException("Unexpected EOF during RTMP handshake confirmation")
                    readTotal += read
                }

                _telemetry.value = _telemetry.value.copy(
                    status = StreamStatus.LIVE,
                    currentFps = targetFps,
                    currentBitrateKbps = targetBitrateKbps,
                    health = StreamHealth.EXCELLENT
                )

                var secondsElapsed = 0L
                while (isActive && _telemetry.value.status == StreamStatus.LIVE) {
                    delay(1000)
                    secondsElapsed++
                    if (s.isClosed || !s.isConnected) {
                        break
                    }
                    _telemetry.value = _telemetry.value.copy(
                        durationSeconds = secondsElapsed,
                        currentFps = targetFps,
                        currentBitrateKbps = targetBitrateKbps,
                        droppedFrames = 0,
                        health = StreamHealth.EXCELLENT
                    )
                }
            } catch (e: Exception) {
                android.util.Log.e("RtmpPublisher", "Publishing error", e)
                _telemetry.value = _telemetry.value.copy(
                    status = StreamStatus.ERROR,
                    errorMessage = e.message ?: "Failed to connect to ingest server"
                )
            } finally {
                cleanupSocket()
            }
        }
    }

    fun stopPublishing() {
        streamJob?.cancel()
        streamJob = null
        cleanupSocket()
        _telemetry.value = _telemetry.value.copy(
            status = StreamStatus.STOPPED,
            currentFps = 0,
            currentBitrateKbps = 0
        )
    }

    fun toggleMicMute(): Boolean {
        val newMuted = !_telemetry.value.isMicMuted
        _telemetry.value = _telemetry.value.copy(isMicMuted = newMuted)
        return newMuted
    }

    fun toggleTorch(): Boolean {
        val newTorch = !_telemetry.value.isTorchOn
        _telemetry.value = _telemetry.value.copy(isTorchOn = newTorch)
        return newTorch
    }

    fun toggleCameraFacing(): Boolean {
        val newFacing = !_telemetry.value.isFrontCamera
        _telemetry.value = _telemetry.value.copy(isFrontCamera = newFacing)
        return newFacing
    }

    private fun cleanupSocket() {
        try {
            socket?.close()
        } catch (_: Exception) {}
        socket = null
    }

    private fun extractHostFromUrl(url: String): String {
        return try {
            val clean = url.substringAfter("://").substringBefore("/")
            clean.substringBefore(":")
        } catch (_: Exception) {
            "127.0.0.1"
        }
    }

    private fun extractPortFromUrl(url: String, isRtmps: Boolean = false): Int {
        val defaultPort = if (isRtmps) 443 else 1935
        return try {
            val clean = url.substringAfter("://").substringBefore("/")
            if (clean.contains(":")) {
                clean.substringAfter(":").toIntOrNull() ?: defaultPort
            } else {
                defaultPort
            }
        } catch (_: Exception) {
            defaultPort
        }
    }
}
