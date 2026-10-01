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
import pk.livecaster.app.streaming.encoder.AudioMediaCodecEncoder
import pk.livecaster.app.streaming.encoder.VideoEncoderConfig
import pk.livecaster.app.streaming.encoder.VideoMediaCodecEncoder
import pk.livecaster.app.streaming.rtmp.RtmpConnection
import pk.livecaster.app.streaming.state.StreamHealth
import pk.livecaster.app.streaming.state.StreamStatus
import pk.livecaster.app.streaming.state.StreamTelemetry

class RtmpPublisher(
    private val scope: CoroutineScope
) {
    private var streamJob: Job? = null
    private var rtmpConnection: RtmpConnection? = null
    private var videoEncoder: VideoMediaCodecEncoder? = null
    private var audioEncoder: AudioMediaCodecEncoder? = null

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
                val conn = RtmpConnection()
                rtmpConnection = conn

                // 1. Establish RTMP/RTMPS handshake, connect, createStream, publish and metadata
                conn.connect(
                    rtmpUrl = rtmpUrl,
                    streamKey = streamKey,
                    width = videoConfig.width,
                    height = videoConfig.height,
                    fps = videoConfig.frameRate,
                    videoBitrateKbps = videoConfig.bitrateKbps
                )

                // 2. Start hardware video and audio encoders
                val vEnc = VideoMediaCodecEncoder(
                    rtmpConnection = conn,
                    width = videoConfig.width,
                    height = videoConfig.height,
                    fps = videoConfig.frameRate,
                    bitrateKbps = videoConfig.bitrateKbps
                )
                videoEncoder = vEnc
                vEnc.start()

                val aEnc = AudioMediaCodecEncoder(
                    rtmpConnection = conn,
                    sampleRate = audioConfig.sampleRate,
                    channelCount = audioConfig.channelCount,
                    bitrate = audioConfig.bitrateKbps * 1000
                )
                audioEncoder = aEnc
                aEnc.start()

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
                stopPublishing()
            }
        }
    }

    fun encodeVideoFrame(yuvBytes: ByteArray) {
        if (_telemetry.value.status == StreamStatus.LIVE) {
            videoEncoder?.encodeYuv(yuvBytes)
        }
    }

    fun stopPublishing() {
        streamJob?.cancel()
        streamJob = null

        audioEncoder?.stop()
        audioEncoder = null

        videoEncoder?.stop()
        videoEncoder = null

        rtmpConnection?.close()
        rtmpConnection = null

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
}

