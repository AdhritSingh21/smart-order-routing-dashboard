import type { ConnectionStatus } from '../types/market'

export type MessageListener = (payload: unknown) => void
export type StatusListener = (status: ConnectionStatus) => void

const BASE_RECONNECT_DELAY_MS = 800
const MAX_RECONNECT_DELAY_MS = 8_000

function websocketUrl(): string {
  const configured = import.meta.env.VITE_WS_URL?.trim()
  if (configured) return configured

  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${protocol}//${window.location.host}/ws`
}

/**
 * One WebSocket shared by every panel. Predictions and venue reports arrive
 * on the same `/ws` stream; consumers subscribe here and filter by shape.
 * Connects lazily with the first subscriber, reconnects with exponential
 * backoff, and closes when the last subscriber leaves.
 */
class SharedMarketSocket {
  private messageListeners = new Set<MessageListener>()
  private statusListeners = new Set<StatusListener>()
  private socket: WebSocket | null = null
  private reconnectTimer: number | null = null
  private reconnectAttempt = 0
  private status: ConnectionStatus = 'disconnected'

  getStatus(): ConnectionStatus {
    return this.status
  }

  subscribe(onMessage: MessageListener, onStatus?: StatusListener): () => void {
    this.messageListeners.add(onMessage)
    if (onStatus) {
      this.statusListeners.add(onStatus)
      onStatus(this.status)
    }
    this.ensureConnected()

    return () => {
      this.messageListeners.delete(onMessage)
      if (onStatus) this.statusListeners.delete(onStatus)
      if (this.messageListeners.size === 0) this.disconnect()
    }
  }

  private setStatus(status: ConnectionStatus): void {
    if (this.status === status) return
    this.status = status
    this.statusListeners.forEach((listener) => listener(status))
  }

  private ensureConnected(): void {
    if (this.messageListeners.size === 0) return
    if (
      this.socket &&
      (this.socket.readyState === WebSocket.OPEN || this.socket.readyState === WebSocket.CONNECTING)
    ) {
      return
    }

    this.setStatus(this.reconnectAttempt === 0 ? 'connecting' : 'reconnecting')
    const socket = new WebSocket(websocketUrl())
    this.socket = socket

    socket.onopen = () => {
      if (this.socket !== socket) return
      this.reconnectAttempt = 0
      this.setStatus('connected')
    }
    socket.onmessage = (event) => {
      if (this.socket !== socket) return
      let payload: unknown
      try {
        payload = JSON.parse(event.data)
      } catch {
        return // Ignore malformed frames; keep the stream alive.
      }
      this.messageListeners.forEach((listener) => listener(payload))
    }
    socket.onerror = () => socket.close()
    socket.onclose = () => {
      // Ignore a late close from a socket that has already been replaced.
      if (this.socket !== socket) return
      this.socket = null
      if (this.messageListeners.size === 0) {
        this.setStatus('disconnected')
        return
      }
      this.setStatus('reconnecting')
      this.reconnectAttempt += 1
      const delay = Math.min(
        BASE_RECONNECT_DELAY_MS * 2 ** (this.reconnectAttempt - 1),
        MAX_RECONNECT_DELAY_MS,
      )
      this.reconnectTimer = window.setTimeout(() => {
        this.reconnectTimer = null
        this.ensureConnected()
      }, delay)
    }
  }

  private disconnect(): void {
    if (this.reconnectTimer !== null) {
      window.clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
    this.reconnectAttempt = 0
    const socket = this.socket
    this.socket = null
    socket?.close()
    this.setStatus('disconnected')
  }
}

export const sharedMarketSocket = new SharedMarketSocket()
