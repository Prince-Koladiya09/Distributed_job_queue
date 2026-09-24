import React, { useEffect, useState } from 'react';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts';

/**
 * Rolling client-side window of throughput samples, built from the
 * periodic "stats" WebSocket broadcasts (FR-6.3 / FR-6.4).
 */
export default function ThroughputChart({ stats }) {
  const [history, setHistory] = useState([]);

  useEffect(() => {
    if (!stats) return;
    setHistory((prev) => {
      const next = [
        ...prev,
        {
          time: new Date(stats.ts).toLocaleTimeString(),
          throughput: stats.throughputPerMin ?? 0,
        },
      ];
      return next.slice(-30);
    });
  }, [stats]);

  return (
    <div className="panel">
      <h2>Throughput (jobs/min)</h2>
      {history.length < 2 ? (
        <p className="muted">Collecting samples…</p>
      ) : (
        <ResponsiveContainer width="100%" height={220}>
          <LineChart data={history}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2a2f3a" />
            <XAxis dataKey="time" tick={{ fontSize: 11, fill: '#8b93a7' }} />
            <YAxis allowDecimals={false} tick={{ fontSize: 11, fill: '#8b93a7' }} />
            <Tooltip contentStyle={{ background: '#1c2029', border: '1px solid #2a2f3a' }} />
            <Line type="monotone" dataKey="throughput" stroke="#5b8cff" strokeWidth={2} dot={false} />
          </LineChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}
