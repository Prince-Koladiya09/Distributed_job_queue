import { useEffect, useRef, useState } from 'react';

/**
 * Subscribes to the API server's /ws endpoint and keeps a rolling log
 * of recent job events plus the latest broadcast queue-stats snapshot
 * (FR-6.3). Reconnects automatically with simple backoff if the
 * connection drops.
 */
export function useJobEvents({ maxEvents = 200 } = {}) {
  const [events, setEvents] = useState([]);
  const [stats, setStats] = useState(null);
  const [connected, setConnected] = useState(false);
  const retryDelay = useRef(1000);

  useEffect(() => {
    let socket;
    let closedByCleanup = false;
    let retryTimer;

    function connect() {
      const proto = window.location.protocol === 'https:' ? 'wss' : 'ws';
      const wsUrl = import.meta.env.VITE_WS_URL || `${proto}://${window.location.hostname}:3000/ws`;
      socket = new WebSocket(wsUrl);

      socket.onopen = () => {
        setConnected(true);
        retryDelay.current = 1000;
      };

      socket.onmessage = (msg) => {
        try {
          const data = JSON.parse(msg.data);
          if (data.type === 'stats') {
            setStats(data);
          } else if (data.type === 'connected') {
            // no-op, initial handshake message
          } else {
            setEvents((prev) => [data, ...prev].slice(0, maxEvents));
          }
        } catch {
          // ignore malformed frames
        }
      };

      socket.onclose = () => {
        setConnected(false);
        if (closedByCleanup) return;
        retryTimer = setTimeout(connect, retryDelay.current);
        retryDelay.current = Math.min(retryDelay.current * 2, 15000);
      };

      socket.onerror = () => {
        socket.close();
      };
    }

    connect();

    return () => {
      closedByCleanup = true;
      clearTimeout(retryTimer);
      socket?.close();
    };
  }, [maxEvents]);

  return { events, stats, connected };
}
