import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import './App.css'
import {
  createMockRealtimeConnection,
  type RealtimeConnectionState,
  type RealtimeProviderConfig,
} from './realtime/mockRealtime'
import { itineraryItems, type ItineraryContent } from './itinerary'

type Session = {
  session_id: string
  status: string
  context_version: number
  realtime_connection_epoch: number
  last_event_seq: number
  locale: string
  timezone: string
  created_at: string
}

type Preference = {
  preference_id: string
  version: number
  status: string
  payload: Record<string, unknown>
}

type Turn = {
  turn_id: string
  sequence_no: number
  kind: string
  status: string
  context_version: number
  text?: string | null
  reply_context?: {
    message?: string
    tool_results?: Array<Record<string, unknown>>
  } | null
}

type Task = {
  task_id: string
  turn_id: string | null
  task_type: string
  status: string
  context_version: number | null
  target_itinerary_version: number | null
  result?: Record<string, unknown> | null
  error_message?: string | null
}

type ToolCall = {
  tool_call_id: string
  task_id: string
  tool_name: string
  status: string
  context_version: number
  target_preference_version: number
  output?: Record<string, unknown> | null
  error_message?: string | null
}

type Itinerary = {
  itinerary_id?: string
  status: string
  version: number
  context_version: number
  preference_version?: number
  content?: ItineraryContent
  source_task_ids?: string[]
}

type Snapshot = {
  session: Session
  active_preference: Preference | null
  turns: Turn[]
  tasks: Task[]
  tool_calls: ToolCall[]
  itinerary: Itinerary
  missed_events: Array<Record<string, unknown>>
  after_event_seq: number
}

type WireMessage = {
  type?: string
  payload?: Snapshot | Record<string, unknown>
  snapshot?: Snapshot
  event_seq?: number
}

type RealtimeTokenResponse = {
  token: string
  device_id: string
  connection_epoch: number
  expires_at: string
  provider_config: RealtimeProviderConfig
}

type ResumeResponse = RealtimeTokenResponse & { snapshot: Snapshot }

type VoiceState = 'idle' | 'requesting' | 'connecting' | 'live' | 'error'

const API_BASE = (
  import.meta.env.VITE_API_BASE_URL || import.meta.env.VITE_API_URL || 'http://localhost:8000'
).replace(/\/$/, '')
const SESSION_STORAGE_KEY = 'livepilot.session_id'
const DEVICE_STORAGE_KEY = 'livepilot.device_id'
const ACCESS_TOKEN_STORAGE_KEY = 'livepilot.access_token'

function traceparent() {
  const bytes = crypto.getRandomValues(new Uint8Array(24))
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('')
  return `00-${hex.slice(0, 32)}-${hex.slice(32)}-01`
}

function currentAccessToken() {
  return sessionStorage.getItem(ACCESS_TOKEN_STORAGE_KEY)?.trim() ?? ''
}

async function api<T>(path: string, options?: RequestInit): Promise<T> {
  const accessToken = currentAccessToken()
  if (!accessToken) throw new Error('需要登录凭据')
  const response = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${accessToken}`,
      traceparent: traceparent(),
      ...(options?.headers ?? {}),
    },
  })
  if (!response.ok) {
    const detail = await response.text()
    throw new Error(detail || `Request failed (${response.status})`)
  }
  return response.json() as Promise<T>
}

function websocketUrl(sessionId: string, cursor: number) {
  const url = new URL(`${API_BASE}/v1/sessions/${sessionId}/events`)
  url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'
  url.searchParams.set('after_event_seq', String(cursor))
  return url.toString()
}

function taskLabel(status: string) {
  return {
    queued: '排队中',
    running: '查询中',
    cancel_requested: '正在取消',
    cancelled: '已取消',
    succeeded: '已完成',
    failed: '失败',
    discarded: '已过期',
  }[status] ?? status
}

function toolLabel(status: string) {
  return {
    pending: '等待调用',
    running: '查询中',
    succeeded: '已完成',
    failed: '失败',
    timed_out: '超时',
    cancelled: '已取消',
    discarded: '已过期',
  }[status] ?? status
}

function voiceLabel(state: VoiceState) {
  return {
    idle: '未连接',
    requesting: '请求麦克风',
    connecting: '建立音频链路',
    live: '语音已连接',
    error: '连接失败',
  }[state]
}

function App() {
  const [sessionId, setSessionId] = useState(() => localStorage.getItem(SESSION_STORAGE_KEY))
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null)
  const [destination, setDestination] = useState('')
  const [accessToken, setAccessToken] = useState(() => currentAccessToken())
  const [draft, setDraft] = useState('')
  const [loading, setLoading] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [connection, setConnection] = useState<'offline' | 'connecting' | 'live'>('offline')
  const [voiceState, setVoiceState] = useState<VoiceState>('idle')
  const [partialTranscript, setPartialTranscript] = useState<string | null>(null)
  const [voiceNotice, setVoiceNotice] = useState<string | null>(null)
  const [deviceId] = useState(() => {
    const saved = localStorage.getItem(DEVICE_STORAGE_KEY)
    if (saved) return saved
    const created = crypto.randomUUID()
    localStorage.setItem(DEVICE_STORAGE_KEY, created)
    return created
  })
  const reconnectTimer = useRef<number | null>(null)
  const cursorRef = useRef(0)
  const realtimeRef = useRef<ReturnType<typeof createMockRealtimeConnection> | null>(null)
  const microphoneStreamRef = useRef<MediaStream | null>(null)
  const remoteAudioRef = useRef<HTMLAudioElement>(null)
  const activePlaybackIdRef = useRef<string | null>(null)
  const voiceAttemptRef = useRef(0)

  const cursor = snapshot?.session.last_event_seq ?? 0
  useEffect(() => { cursorRef.current = cursor }, [cursor])
  const latestReply = useMemo(
    () => [...(snapshot?.turns ?? [])].reverse().find((turn) => turn.kind === 'agent_reply'),
    [snapshot?.turns],
  )
  const activeTask = useMemo(
    () => [...(snapshot?.tasks ?? [])].reverse().find(
      (task) => task.turn_id && ['queued', 'running', 'cancel_requested'].includes(task.status),
    ),
    [snapshot?.tasks],
  )
  const confirmedItineraryItems = useMemo(
    () => itineraryItems(snapshot?.itinerary.content),
    [snapshot?.itinerary.content],
  )

  const loadSnapshot = useCallback(async (id: string, after = 0) => {
    const result = await api<Snapshot>(
      `/v1/sessions/${id}/snapshot?after_event_seq=${Math.max(0, after)}`,
    )
    setSnapshot(result)
    const preferenceDestination = result.active_preference?.payload.destination
    if (typeof preferenceDestination === 'string') setDestination(preferenceDestination)
    return result
  }, [])

  useEffect(() => {
    if (!sessionId) return
    const loadTimer = window.setTimeout(() => {
      void loadSnapshot(sessionId, 0).catch((reason: unknown) => {
        setError(reason instanceof Error ? reason.message : '无法恢复会话')
        localStorage.removeItem(SESSION_STORAGE_KEY)
        setSnapshot(null)
        setSessionId(null)
      })
    }, 0)
    return () => window.clearTimeout(loadTimer)
  }, [loadSnapshot, sessionId])

  useEffect(() => () => {
    realtimeRef.current?.close()
    realtimeRef.current = null
    microphoneStreamRef.current?.getTracks().forEach((track) => track.stop())
    microphoneStreamRef.current = null
  }, [])

  useEffect(() => {
    if (!sessionId) return
    localStorage.setItem(SESSION_STORAGE_KEY, sessionId)
    let stopped = false
    let socket: WebSocket | null = null

    const connect = () => {
      if (stopped) return
      setConnection('connecting')
      const token = currentAccessToken()
      if (!token) {
        setConnection('offline')
        setError('需要登录凭据')
        return
      }
      socket = new WebSocket(
        websocketUrl(sessionId, cursorRef.current),
        ['livepilot', `bearer.${token}`],
      )
      socket.onopen = () => setConnection('live')
      socket.onmessage = (message) => {
        try {
          const data = JSON.parse(message.data) as WireMessage
          if (data.type === 'session.snapshot') {
            const next = (data.payload ?? data.snapshot) as Snapshot
            if (next?.session) setSnapshot(next)
            return
          }
          if (typeof data.event_seq === 'number') {
            void loadSnapshot(sessionId, data.event_seq).catch(() => undefined)
          }
        } catch {
          setError('收到无法识别的会话事件')
        }
      }
      socket.onclose = () => {
        setConnection('offline')
        if (!stopped) reconnectTimer.current = window.setTimeout(connect, 1200)
      }
      socket.onerror = () => setConnection('offline')
    }

    connect()
    return () => {
      stopped = true
      if (reconnectTimer.current !== null) window.clearTimeout(reconnectTimer.current)
      socket?.close()
    }
  }, [loadSnapshot, sessionId])

  async function createSession() {
    setLoading(true)
    setError(null)
    try {
      const created = await api<{ session_id: string }>('/v1/sessions', {
        method: 'POST',
        body: JSON.stringify({ preference: destination.trim() ? { destination: destination.trim() } : {} }),
      })
      localStorage.setItem(SESSION_STORAGE_KEY, created.session_id)
      setSessionId(created.session_id)
    } catch (reason: unknown) {
      setError(reason instanceof Error ? reason.message : '无法创建会话')
    } finally {
      setLoading(false)
    }
  }

  function updateAccessToken(value: string) {
    setAccessToken(value)
    if (value.trim()) sessionStorage.setItem(ACCESS_TOKEN_STORAGE_KEY, value.trim())
    else sessionStorage.removeItem(ACCESS_TOKEN_STORAGE_KEY)
  }

  const submitTurnText = useCallback(async (text: string) => {
    if (!sessionId || !text.trim()) return
    setSubmitting(true)
    setError(null)
    try {
      const result = await api<{ event_seq: number }>(`/v1/sessions/${sessionId}/turns`, {
        method: 'POST',
        body: JSON.stringify({ text: text.trim(), client_event_id: crypto.randomUUID() }),
      })
      setDraft('')
      await loadSnapshot(sessionId, result.event_seq)
    } catch (reason: unknown) {
      setError(reason instanceof Error ? reason.message : '文本提交失败')
    } finally {
      setSubmitting(false)
    }
  }, [loadSnapshot, sessionId])

  async function submitTurn(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault()
    await submitTurnText(draft)
  }

  function stopVoice() {
    const playbackId = activePlaybackIdRef.current
    const turnId = activeTask?.turn_id
    const realtime = realtimeRef.current
    voiceAttemptRef.current += 1
    const audio = remoteAudioRef.current
    if (audio) {
      audio.pause()
      audio.srcObject = null
    }
    activePlaybackIdRef.current = null
    setPartialTranscript(null)
    setVoiceNotice(null)
    setVoiceState('idle')

    realtime?.cancel(playbackId ?? undefined)
    if (sessionId && turnId) {
      void api<{ event_seq: number }>(`/v1/sessions/${sessionId}/interrupt`, {
        method: 'POST',
        body: JSON.stringify({
          turn_id: turnId,
          playback_id: playbackId,
          reason: 'voice_stopped',
          client_event_id: crypto.randomUUID(),
          occurred_at: new Date().toISOString(),
        }),
      })
        .then((result) => loadSnapshot(sessionId, result.event_seq))
        .catch((reason: unknown) => {
          setError(reason instanceof Error ? reason.message : '打断请求失败')
        })
    }

    realtime?.close()
    realtimeRef.current = null
    microphoneStreamRef.current?.getTracks().forEach((track) => track.stop())
    microphoneStreamRef.current = null
  }

  async function startVoice() {
    if (!sessionId || voiceState === 'live') return
    const attempt = voiceAttemptRef.current + 1
    voiceAttemptRef.current = attempt
    setError(null)
    setVoiceNotice(null)
    setVoiceState('requesting')
    let stream: MediaStream | null = null
    try {
      if (!navigator.mediaDevices?.getUserMedia) throw new Error('当前浏览器不支持麦克风')
      stream = await navigator.mediaDevices.getUserMedia({
        audio: {
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: true,
          channelCount: 1,
        },
        video: false,
      })
      if (attempt !== voiceAttemptRef.current) {
        stream.getTracks().forEach((track) => track.stop())
        return
      }
      microphoneStreamRef.current = stream
      const previousConnectionEpoch = snapshot?.session.realtime_connection_epoch ?? 0
      let grant: RealtimeTokenResponse
      let resumedSnapshot: Snapshot | null = null
      if (previousConnectionEpoch > 0) {
        const resumed = await api<ResumeResponse>(`/v1/sessions/${sessionId}/resume`, {
          method: 'POST',
          body: JSON.stringify({
            after_event_seq: cursorRef.current,
            previous_connection_epoch: previousConnectionEpoch,
            device_id: deviceId,
          }),
        })
        grant = resumed
        resumedSnapshot = resumed.snapshot
      } else {
        grant = await api<RealtimeTokenResponse>(`/v1/sessions/${sessionId}/realtime-token`, {
          method: 'POST',
          body: JSON.stringify({ device_id: deviceId }),
        })
      }
      if (attempt !== voiceAttemptRef.current) return
      if (resumedSnapshot) setSnapshot(resumedSnapshot)
      else {
        setSnapshot((current) => current
          ? { ...current, session: { ...current.session, realtime_connection_epoch: grant.connection_epoch } }
          : current)
      }
      const redeemed = await api<Omit<RealtimeTokenResponse, 'token' | 'device_id'>>(
        grant.provider_config.token_redeem_path,
        {
          method: 'POST',
          body: JSON.stringify({ device_id: deviceId, token: grant.token }),
        },
      )
      if (attempt !== voiceAttemptRef.current) return
      setVoiceState('connecting')
      const realtime = createMockRealtimeConnection(redeemed.provider_config, {
        onConnectionState: (state: RealtimeConnectionState) => {
          if (attempt !== voiceAttemptRef.current) return
          if (state === 'connected') setVoiceState('live')
          else if (state === 'failed' || state === 'disconnected') setVoiceState('error')
          else if (state !== 'closed') setVoiceState('connecting')
        },
        onProviderEvent: (event) => {
          if (attempt !== voiceAttemptRef.current) return
          if (event.type === 'realtime.response.started') {
            activePlaybackIdRef.current = event.playback_id ?? null
          }
          if (event.type === 'realtime.response.cancelled') {
            activePlaybackIdRef.current = null
          }
          if (event.type === 'realtime.transcript.partial') setPartialTranscript(event.text ?? null)
          if (event.type === 'realtime.transcript.final' && event.text) {
            setPartialTranscript(null)
            void submitTurnText(event.text)
          }
        },
        onRemoteStream: (remoteStream) => {
          if (attempt !== voiceAttemptRef.current) return
          const audio = remoteAudioRef.current
          if (!audio) return
          if (!remoteStream) {
            audio.pause()
            audio.srcObject = null
            return
          }
          audio.srcObject = remoteStream
          void audio.play().catch(() => setVoiceNotice('浏览器阻止了自动播放'))
        },
        onFirstAudioPacket: () => {
          if (attempt === voiceAttemptRef.current) {
            performance.mark('livepilot.realtime_audio_first_packet')
          }
        },
      })
      realtimeRef.current = realtime
      microphoneStreamRef.current = null
      await realtime.connect(stream)
    } catch (reason: unknown) {
      if (attempt !== voiceAttemptRef.current) return
      realtimeRef.current?.close()
      realtimeRef.current = null
      stream?.getTracks().forEach((track) => track.stop())
      microphoneStreamRef.current = null
      setVoiceState('error')
      setError(reason instanceof Error ? reason.message : '语音连接失败')
    }
  }

  function startNewSession() {
    stopVoice()
    localStorage.removeItem(SESSION_STORAGE_KEY)
    setSessionId(null)
    setSnapshot(null)
    setDraft('')
    setConnection('offline')
  }

  if (!snapshot) {
    return (
      <main className="shell landing-shell">
        <header className="topbar">
          <div className="brand-mark" aria-hidden="true">LP</div>
          <div>
            <p className="eyebrow">LIVEPILOT / SESSION CONTROL</p>
            <h1>你的下一段旅程，从这里开始。</h1>
          </div>
          <span className="build-tag">STAGE 08</span>
        </header>
        <section className="create-band">
          <div className="create-copy">
            <span className="section-kicker">READY WHEN YOU ARE</span>
            <h2>把想法交给一个可恢复的会话。</h2>
            <p>目的地、偏好和每一次建议都会留在同一个上下文里。</p>
          </div>
          <div className="create-form">
            <label htmlFor="access-token">访问令牌</label>
            <input
              id="access-token"
              type="password"
              autoComplete="off"
              value={accessToken}
              onChange={(event) => updateAccessToken(event.target.value)}
            />
            <label htmlFor="destination">目的地 <span>可选</span></label>
            <input
              id="destination"
              value={destination}
              onChange={(event) => setDestination(event.target.value)}
              placeholder="例如：京都"
              onKeyDown={(event) => {
                if (event.key === 'Enter') void createSession()
              }}
            />
            <button type="button" className="primary-button" onClick={() => void createSession()} disabled={loading || !accessToken.trim()}>
              {loading ? '正在创建…' : '创建会话'} <span aria-hidden="true">→</span>
            </button>
          </div>
        </section>
        {error && <p className="error-banner">{error}</p>}
        <footer className="landing-footer">
          <span>权威状态：PostgreSQL</span>
          <span>实时控制：WebSocket</span>
          <span>模型音频：后续阶段接入</span>
        </footer>
      </main>
    )
  }

  const preference = snapshot.active_preference?.payload ?? {}
  const preferenceEntries = Object.entries(preference).filter(([, value]) => value !== '')

  return (
    <main className="shell session-shell">
      <header className="topbar">
        <div className="brand-mark" aria-hidden="true">LP</div>
        <div className="session-title">
          <p className="eyebrow">LIVEPILOT / SESSION CONTROL</p>
          <h1>{typeof preference.destination === 'string' ? preference.destination : '未命名旅程'}</h1>
        </div>
        <div className={`connection-state ${connection}`}><i />{connection === 'live' ? '会话在线' : connection === 'connecting' ? '正在连接' : '已离线'}</div>
        <button type="button" className="quiet-button" onClick={startNewSession} title="开始一个新的会话">新会话</button>
      </header>

      <section className="status-strip">
        <div><span>CONTEXT</span><strong>v{snapshot.session.context_version}</strong></div>
        <div><span>PREFERENCE</span><strong>v{snapshot.active_preference?.version ?? '—'}</strong></div>
        <div><span>EVENT CURSOR</span><strong>#{cursor}</strong></div>
        <div><span>SESSION</span><strong>{snapshot.session.status}</strong></div>
      </section>

      {error && <p className="error-banner">{error}</p>}

      <section className="workspace-grid">
        <div className="conversation-panel panel">
          <div className="panel-heading"><div><span className="section-kicker">CONVERSATION</span><h2>对话记录</h2></div><span className="record-count">{snapshot.turns.length} turns</span></div>
          <div className="turn-list">
            {snapshot.turns.length === 0 && <div className="empty-state">说说你想去哪里，或者想要怎样的一段旅行。</div>}
            {snapshot.turns.map((turn) => (
              <article className={`turn ${turn.kind === 'agent_reply' ? 'agent' : 'user'}`} key={turn.turn_id}>
                <div className="turn-meta"><span>{turn.kind === 'agent_reply' ? 'LIVEPILOT' : 'YOU'}</span><span>v{turn.context_version}</span></div>
                <p>{turn.kind === 'agent_reply' ? turn.reply_context?.message ?? '正在整理建议…' : turn.text}</p>
              </article>
            ))}
          </div>
          <form className="composer" onSubmit={(event) => void submitTurn(event)}>
            <textarea value={draft} onChange={(event) => setDraft(event.target.value)} placeholder="写下你的下一个想法…" rows={3} />
            <div className="composer-actions"><span>文本回合 · 自动保存上下文</span><button type="submit" className="primary-button" disabled={submitting || !draft.trim()}>{submitting ? '发送中…' : '发送'} <span aria-hidden="true">↗</span></button></div>
          </form>
        </div>

        <aside className="insight-column">
          <section className="panel voice-panel">
            <div className="panel-heading"><div><span className="section-kicker">VOICE LINK</span><h2>语音链路</h2></div><span className={`voice-state ${voiceState}`}>{voiceLabel(voiceState)}</span></div>
            <div className="voice-controls">
              <button
                type="button"
                className={voiceState === 'live' || voiceState === 'connecting' || voiceState === 'requesting' ? 'quiet-button' : 'primary-button'}
                onClick={() => (voiceState === 'live' || voiceState === 'connecting' || voiceState === 'requesting' ? stopVoice() : void startVoice())}
              >
                {voiceState === 'live' ? '关闭语音' : voiceState === 'connecting' || voiceState === 'requesting' ? '取消连接' : '开始语音'}
              </button>
              <span className="voice-epoch">epoch {snapshot.session.realtime_connection_epoch}</span>
            </div>
            {partialTranscript && <p className="partial-transcript">{partialTranscript}</p>}
            {voiceNotice && <p className="voice-notice">{voiceNotice}</p>}
            <audio ref={remoteAudioRef} autoPlay playsInline aria-label="远端语音" onPlaying={() => performance.mark('livepilot.speech_first_playout')} />
          </section>
          <section className="panel preference-panel">
            <div className="panel-heading"><div><span className="section-kicker">ACTIVE CONTEXT</span><h2>当前偏好</h2></div><span className="version-badge">v{snapshot.active_preference?.version ?? '—'}</span></div>
            {preferenceEntries.length === 0 ? <p className="muted">还没有额外偏好。</p> : <dl className="preference-list">{preferenceEntries.map(([key, value]) => <div key={key}><dt>{key.replaceAll('_', ' ')}</dt><dd>{typeof value === 'object' ? JSON.stringify(value) : String(value)}</dd></div>)}</dl>}
          </section>
          <section className="panel task-panel">
            <div className="panel-heading"><div><span className="section-kicker">ASYNC WORK</span><h2>任务进度</h2></div><span className="record-count">{snapshot.tasks.length}</span></div>
            {snapshot.tasks.length === 0 ? <p className="muted">提交文本后，异步任务会出现在这里。</p> : <div className="task-list">{snapshot.tasks.map((task) => <div className="task-row" key={task.task_id}><div><strong>{task.task_type}</strong><small>context v{task.context_version ?? '—'}</small></div><span className={`task-status ${task.status}`}>{taskLabel(task.status)}</span></div>)}</div>}
          </section>
          <section className="panel tool-panel">
            <div className="panel-heading"><div><span className="section-kicker">TOOL AUDIT</span><h2>工具调用</h2></div><span className="record-count">{snapshot.tool_calls.length}</span></div>
            {snapshot.tool_calls.length === 0 ? <p className="muted">工具调用会在 Worker 开始查询后记录在这里。</p> : <div className="task-list">{snapshot.tool_calls.map((toolCall) => <div className="task-row" key={toolCall.tool_call_id}><div><strong>{toolCall.tool_name}</strong><small>context v{toolCall.context_version} · preference v{toolCall.target_preference_version}</small></div><span className={`task-status ${toolCall.status}`}>{toolLabel(toolCall.status)}</span></div>)}</div>}
          </section>
          <section className="panel itinerary-panel">
            <div className="panel-heading"><div><span className="section-kicker">CONFIRMED ITINERARY</span><h2>当前行程</h2></div><span className="version-badge">v{snapshot.itinerary.version}</span></div>
            {snapshot.itinerary.status === 'not_created' ? <p className="muted">完成工具查询后，权威行程会在这里恢复。</p> : <div className="itinerary-body"><p className="itinerary-destination">{snapshot.itinerary.content?.destination ?? '当前目的地'}</p><p className="itinerary-meta">context v{snapshot.itinerary.context_version} · preference v{snapshot.itinerary.preference_version ?? '—'}</p>{confirmedItineraryItems.length === 0 ? <p className="muted">当前行程还没有可展示的项目。</p> : <ul className="itinerary-list">{confirmedItineraryItems.map((item, index) => <li key={`${item.name ?? 'item'}-${index}`}><strong>{item.name ?? '未命名推荐'}</strong><span>{item.type ?? 'activity'}</span></li>)}</ul>}</div>}
          </section>
          <section className="reply-panel">
            <span className="section-kicker">LATEST REPLY</span>
            <h2>{latestReply ? '建议已准备好' : '等待你的第一句话'}</h2>
            <p>{latestReply?.reply_context?.message ?? '完成一个文本回合后，Agent 回复会在这里持续可恢复地呈现。'}</p>
          </section>
        </aside>
      </section>
      <footer className="session-footer"><span>{sessionId}</span><span>cursor #{cursor} · {snapshot.session.locale} · {snapshot.session.timezone}</span></footer>
    </main>
  )
}

export default App
