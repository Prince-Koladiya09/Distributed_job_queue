import React from 'react';

const CARD_LABELS = [
  { key: 'high', label: 'High priority', group: 'queueDepth' },
  { key: 'normal', label: 'Normal priority', group: 'queueDepth' },
  { key: 'low', label: 'Low priority', group: 'queueDepth' },
  { key: 'delayed', label: 'Delayed', group: 'queueDepth' },
];

export default function QueueStats({ stats }) {
  if (!stats) {
    return <div className="panel">Waiting for live stats…</div>;
  }

  const depth = stats.queueDepth || {};
  const throughput = stats.throughputPerMin ?? 0;
  const failureRate = stats.failureRateLastHour ?? 0;
  const dlqCount = stats.dlqCount ?? 0;

  return (
    <div className="panel">
      <h2>Queue overview</h2>
      <div className="stat-grid">
        {CARD_LABELS.map(({ key, label }) => (
          <div className="stat-card" key={key}>
            <div className="stat-value">{depth[key] ?? 0}</div>
            <div className="stat-label">{label}</div>
          </div>
        ))}
        <div className="stat-card">
          <div className="stat-value">{throughput}</div>
          <div className="stat-label">Jobs / min</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{(failureRate * 100).toFixed(1)}%</div>
          <div className="stat-label">Failure rate (1h)</div>
        </div>
        <div className="stat-card stat-card--danger">
          <div className="stat-value">{dlqCount}</div>
          <div className="stat-label">Dead-letter queue</div>
        </div>
      </div>
    </div>
  );
}
