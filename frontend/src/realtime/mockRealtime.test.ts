import { afterEach, describe, expect, it, vi } from 'vitest'

import { createMockRealtimeConnection } from './mockRealtime'

class FakeDataChannel {
  readyState: RTCDataChannelState = 'open'
  onmessage: ((event: MessageEvent) => void) | null = null
  onopen: ((event: Event) => void) | null = null
  peer: FakeDataChannel | null = null
  sent: string[] = []

  send(payload: string) {
    this.sent.push(payload)
    this.peer?.onmessage?.({ data: payload } as MessageEvent)
  }
}

class FakePeerConnection {
  static peers: FakePeerConnection[] = []
  static remoteStream: MediaStream

  connectionState: RTCPeerConnectionState = 'connected'
  onicecandidate: ((event: RTCPeerConnectionIceEvent) => void) | null = null
  onconnectionstatechange: ((event: Event) => void) | null = null
  ontrack: ((event: RTCTrackEvent) => void) | null = null
  ondatachannel: ((event: RTCDataChannelEvent) => void) | null = null
  outgoingChannel: FakeDataChannel | null = null
  tracks: MediaStreamTrack[] = []

  constructor() {
    FakePeerConnection.peers.push(this)
  }

  addTrack(track: MediaStreamTrack) {
    this.tracks.push(track)
    return {} as RTCRtpSender
  }

  createDataChannel() {
    const local = new FakeDataChannel()
    const remote = new FakeDataChannel()
    local.peer = remote
    remote.peer = local
    this.outgoingChannel = local
    return local as unknown as RTCDataChannel
  }

  async createOffer() {
    return { type: 'offer', sdp: 'offer' } as RTCSessionDescriptionInit
  }

  async createAnswer() {
    return { type: 'answer', sdp: 'answer' } as RTCSessionDescriptionInit
  }

  async setLocalDescription() {}

  async setRemoteDescription(description: RTCSessionDescriptionInit) {
    if (description.type === 'offer') {
      const browserPeer = FakePeerConnection.peers[0]
      const channel = browserPeer.outgoingChannel?.peer
      if (channel) {
        this.ondatachannel?.({ channel } as unknown as RTCDataChannelEvent)
        channel.onopen?.(new Event('open'))
      }
    }
    if (description.type === 'answer') {
      this.ontrack?.({
        streams: [FakePeerConnection.remoteStream],
        track: FakePeerConnection.remoteStream.getAudioTracks()[0],
      } as unknown as RTCTrackEvent)
    }
  }

  async addIceCandidate() {}

  close() {}
}

class FakeAudioContext {
  static instances: FakeAudioContext[] = []
  destinationTrack = { kind: 'audio', stop: vi.fn() } as unknown as MediaStreamTrack
  resume = vi.fn(async () => undefined)
  close = vi.fn(async () => undefined)

  constructor() {
    FakeAudioContext.instances.push(this)
  }

  createMediaStreamDestination() {
    return {
      stream: {
        getAudioTracks: () => [this.destinationTrack],
      },
    } as MediaStreamAudioDestinationNode
  }

  createGain() {
    return {
      gain: { value: 0 },
      connect: vi.fn(),
    } as unknown as GainNode
  }

  createOscillator() {
    return {
      frequency: { value: 0 },
      connect: vi.fn(),
      start: vi.fn(),
      stop: vi.fn(),
    } as unknown as OscillatorNode
  }
}

afterEach(() => {
  vi.unstubAllGlobals()
  FakePeerConnection.peers = []
  FakeAudioContext.instances = []
})

describe('mock realtime connection', () => {
  it('forwards the microphone, receives remote audio, emits data events, and cleans up', async () => {
    const microphoneTrack = { kind: 'audio', stop: vi.fn() } as unknown as MediaStreamTrack
    const remoteTrack = { kind: 'audio', stop: vi.fn() } as unknown as MediaStreamTrack
    const microphoneStream = {
      getAudioTracks: () => [microphoneTrack],
      getTracks: () => [microphoneTrack],
    } as unknown as MediaStream
    const remoteStream = {
      getAudioTracks: () => [remoteTrack],
    } as unknown as MediaStream
    FakePeerConnection.remoteStream = remoteStream
    vi.stubGlobal('RTCPeerConnection', FakePeerConnection)
    vi.stubGlobal('AudioContext', FakeAudioContext)

    const remoteStreams: Array<MediaStream | null> = []
    const events: string[] = []
    const connection = createMockRealtimeConnection(
      {
        provider: 'mock',
        adapter: 'loopback',
        data_channel_label: 'livepilot.realtime',
        ice_servers: [],
        token_redeem_path: '/token/redeem',
      },
      {
        onConnectionState: vi.fn(),
        onProviderEvent: (event) => events.push(event.type),
        onRemoteStream: (stream) => remoteStreams.push(stream),
        onFirstAudioPacket: vi.fn(),
      },
    )

    await connection.connect(microphoneStream)

    expect(FakePeerConnection.peers[0].tracks).toContain(microphoneTrack)
    expect(remoteStreams).toEqual([remoteStream])
    expect(events).toContain('realtime.connection.changed')

    expect(connection.cancel('playback-1')).toBe(true)
    expect(FakePeerConnection.peers[0].outgoingChannel?.sent).toContain(
      JSON.stringify({ type: 'response.cancel', playback_id: 'playback-1' }),
    )
    expect(events).toContain('realtime.response.cancelled')

    connection.close()

    expect(microphoneTrack.stop).toHaveBeenCalledOnce()
    expect(remoteStreams.at(-1)).toBeNull()
    expect(FakeAudioContext.instances[0].close).toHaveBeenCalledOnce()
  })
})
