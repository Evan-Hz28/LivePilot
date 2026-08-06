export type RealtimeProviderConfig = {
  provider: 'mock'
  adapter: 'loopback'
  data_channel_label: string
  ice_servers: RTCIceServer[]
  token_redeem_path: string
}

export type RealtimeProviderEvent = {
  type: string
  text?: string
  playback_id?: string
  turn_id?: string
}

export type RealtimeConnectionState = RTCPeerConnectionState

type RealtimeHandlers = {
  onConnectionState: (state: RealtimeConnectionState) => void
  onProviderEvent: (event: RealtimeProviderEvent) => void
  onRemoteStream: (stream: MediaStream | null) => void
  onFirstAudioPacket: () => void
}

export type MockRealtimeConnection = {
  connect: (microphoneStream: MediaStream) => Promise<void>
  close: () => void
}

function providerEvent(payload: unknown): RealtimeProviderEvent | null {
  if (!payload || typeof payload !== 'object') return null
  const event = payload as Record<string, unknown>
  if (typeof event.type !== 'string' || !event.type.startsWith('realtime.')) return null
  return {
    type: event.type,
    text: typeof event.text === 'string' ? event.text : undefined,
    playback_id: typeof event.playback_id === 'string' ? event.playback_id : undefined,
    turn_id: typeof event.turn_id === 'string' ? event.turn_id : undefined,
  }
}

function sendMockEvent(channel: RTCDataChannel, event: RealtimeProviderEvent) {
  if (channel.readyState === 'open') channel.send(JSON.stringify(event))
}

export function createMockRealtimeConnection(
  config: RealtimeProviderConfig,
  handlers: RealtimeHandlers,
): MockRealtimeConnection {
  let browserPeer: RTCPeerConnection | null = null
  let providerPeer: RTCPeerConnection | null = null
  let microphoneStream: MediaStream | null = null
  let audioContext: AudioContext | null = null
  let oscillator: OscillatorNode | null = null
  let closed = false

  function dispose(state: RealtimeConnectionState) {
    if (closed) return
    closed = true
    oscillator?.stop()
    oscillator = null
    void audioContext?.close()
    audioContext = null
    browserPeer?.close()
    providerPeer?.close()
    browserPeer = null
    providerPeer = null
    microphoneStream?.getTracks().forEach((track) => track.stop())
    microphoneStream = null
    handlers.onRemoteStream(null)
    handlers.onConnectionState(state)
  }

  return {
    async connect(stream) {
      if (closed) throw new Error('Realtime connection is closed')
      if (!globalThis.RTCPeerConnection || !globalThis.AudioContext) {
        throw new Error('当前浏览器不支持 WebRTC 音频')
      }
      const microphoneTrack = stream.getAudioTracks()[0]
      if (!microphoneTrack) throw new Error('未检测到麦克风音频轨')

      microphoneStream = stream
      browserPeer = new RTCPeerConnection({ iceServers: config.ice_servers })
      providerPeer = new RTCPeerConnection({ iceServers: config.ice_servers })
      const pendingForBrowser: RTCIceCandidate[] = []
      const pendingForProvider: RTCIceCandidate[] = []
      let browserHasRemoteDescription = false
      let providerHasRemoteDescription = false
      const addCandidate = (target: RTCPeerConnection, candidate: RTCIceCandidate) => {
        void target.addIceCandidate(candidate).catch(() => dispose('failed'))
      }
      browserPeer.onicecandidate = (event) => {
        if (!event.candidate) return
        if (providerHasRemoteDescription) addCandidate(providerPeer!, event.candidate)
        else pendingForProvider.push(event.candidate)
      }
      providerPeer.onicecandidate = (event) => {
        if (!event.candidate) return
        if (browserHasRemoteDescription) addCandidate(browserPeer!, event.candidate)
        else pendingForBrowser.push(event.candidate)
      }
      browserPeer.onconnectionstatechange = () => {
        const state = browserPeer?.connectionState
        if (!state) return
        handlers.onConnectionState(state)
        if (state === 'disconnected' || state === 'failed') dispose(state)
      }
      browserPeer.ontrack = (event) => {
        const remoteStream = event.streams[0] ?? new MediaStream([event.track])
        handlers.onFirstAudioPacket()
        handlers.onRemoteStream(remoteStream)
      }

      const browserChannel = browserPeer.createDataChannel(config.data_channel_label)
      browserChannel.onmessage = (event) => {
        try {
          const parsed = providerEvent(JSON.parse(String(event.data)))
          if (parsed) handlers.onProviderEvent(parsed)
        } catch {
          // Invalid provider messages are transient and do not affect the session.
        }
      }
      providerPeer.ondatachannel = (event) => {
        event.channel.onopen = () => {
          sendMockEvent(event.channel, {
            type: 'realtime.connection.changed',
          })
        }
      }

      audioContext = new AudioContext()
      const destination = audioContext.createMediaStreamDestination()
      const gain = audioContext.createGain()
      gain.gain.value = 0.012
      oscillator = audioContext.createOscillator()
      oscillator.frequency.value = 440
      oscillator.connect(gain)
      gain.connect(destination)
      oscillator.start()
      await audioContext.resume()
      if (closed) return

      browserPeer.addTrack(microphoneTrack, stream)
      const remoteTrack = destination.stream.getAudioTracks()[0]
      if (!remoteTrack) {
        dispose('failed')
        throw new Error('无法创建 mock 远端音频轨')
      }
      providerPeer.addTrack(remoteTrack, destination.stream)

      const offer = await browserPeer.createOffer()
      if (closed) return
      await browserPeer.setLocalDescription(offer)
      if (closed) return
      await providerPeer.setRemoteDescription(offer)
      if (closed) return
      providerHasRemoteDescription = true
      pendingForProvider.forEach((candidate) => addCandidate(providerPeer!, candidate))
      pendingForProvider.length = 0
      const answer = await providerPeer.createAnswer()
      if (closed) return
      await providerPeer.setLocalDescription(answer)
      if (closed) return
      await browserPeer.setRemoteDescription(answer)
      if (closed) return
      browserHasRemoteDescription = true
      pendingForBrowser.forEach((candidate) => addCandidate(browserPeer!, candidate))
      pendingForBrowser.length = 0
    },
    close() {
      dispose('closed')
    },
  }
}
