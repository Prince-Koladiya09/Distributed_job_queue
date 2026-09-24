import React, { useState } from 'react';
import { api, setApiKey } from './api.js';
import { useJobEvents } from './ws.js';
import QueueStats from './components/QueueStats.jsx';
import ThroughputChart from './components/ThroughputChart.jsx';
import JobList from './components/JobList.jsx';
import JobDetail from './components/JobDetail.jsx';
import DLQPage from './components/DLQPage.jsx';

const TABS = ['Overview', 'Dead-letter queue', 'Submit job'];

export default function App() {
  const [tab, setTab] = useState('Overview');
  const [selectedJobId, setSelectedJobId] = useState(null);
  const [refreshToken, setRefreshToken] = useState(0);
  const { events, stats, connected } = useJobEvents();

  function bump() {
    setRefreshToken((t) => t + 1);
  }

  return (
    <div className="app-shell">
      <header className="app-header">
        <h1>Distributed Job Queue</h1>
        <div className="conn-status">
          <span className={`dot ${connected ? 'dot--live' : 'dot--down'}`} />
          {connected ? 'Live' : 'Reconnecting…'}
        </div>
      </header>

      <nav className="tabs">
        {TABS.map((t) => (
          <button key={t} className={t === tab ? 'tab tab--active' : 'tab'} onClick={() => setTab(t)}>
            {t}
          </button>
        ))}
        <ApiKeyField />
      </nav>

      <main className="app-main">
        {tab === 'Overview' && (
          <div className="grid-2col">
            <div>
              <QueueStats stats={stats} />
              <ThroughputChart stats={stats} />
              <EventFeed events={events} onSelectJob={setSelectedJobId} />
            </div>
            <JobList onSelectJob={setSelectedJobId} refreshToken={refreshToken} />
          </div>
        )}

        {tab === 'Dead-letter queue' && (
          <DLQPage onSelectJob={setSelectedJobId} refreshToken={refreshToken} />
        )}

        {tab === 'Submit job' && <SubmitJobForm onSubmitted={bump} />}
      </main>

      <JobDetail jobId={selectedJobId} onClose={() => setSelectedJobId(null)} onChanged={bump} />
    </div>
  );
}

function ApiKeyField() {
  const [value, setValue] = useState(localStorage.getItem('jobqueue_api_key') || 'dev-key-123');
  return (
    <div className="api-key-field">
      <label>API key</label>
      <input
        value={value}
        onChange={(e) => {
          setValue(e.target.value);
          setApiKey(e.target.value);
        }}
      />
    </div>
  );
}

function EventFeed({ events, onSelectJob }) {
  return (
    <div className="panel">
      <h2>Live event feed</h2>
      <ul className="event-feed">
        {events.map((ev, i) => (
          <li key={`${ev.jobId}-${ev.ts}-${i}`} onClick={() => ev.jobId && onSelectJob(ev.jobId)}>
            <span className="mono">{new Date(ev.ts).toLocaleTimeString()}</span>{' '}
            <span className={`badge badge--status-${ev.status}`}>{ev.status}</span>{' '}
            <span className="mono truncate-id">{ev.jobId?.slice(0, 8)}</span>{' '}
            <span>{ev.message}</span>
          </li>
        ))}
        {events.length === 0 && <li className="muted">No events yet — submit a job to see it live.</li>}
      </ul>
    </div>
  );
}

function SubmitJobForm({ onSubmitted }) {
  const [type, setType] = useState('send_email');
  const [payload, setPayload] = useState('{\n  "to": "user@example.com",\n  "template": "welcome"\n}');
  const [priority, setPriority] = useState('normal');
  const [delay, setDelay] = useState(0);
  const [maxRetries, setMaxRetries] = useState(3);
  const [idempotencyKey, setIdempotencyKey] = useState('');
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);

  async function handleSubmit(e) {
    e.preventDefault();
    setError(null);
    setResult(null);
    let parsedPayload;
    try {
      parsedPayload = JSON.parse(payload);
    } catch {
      setError('Payload must be valid JSON.');
      return;
    }
    try {
      const res = await api.enqueueJob({
        type,
        payload: parsedPayload,
        priority,
        delay: Number(delay) || 0,
        maxRetries: Number(maxRetries),
        idempotencyKey: idempotencyKey || undefined,
      });
      setResult(res);
      onSubmitted?.();
    } catch (err) {
      setError(err.message);
    }
  }

  return (
    <div className="panel panel--narrow">
      <h2>Submit a job</h2>
      <form onSubmit={handleSubmit} className="job-form">
        <label>Type
          <input value={type} onChange={(e) => setType(e.target.value)} required />
        </label>
        <label>Payload (JSON)
          <textarea rows={6} value={payload} onChange={(e) => setPayload(e.target.value)} />
        </label>
        <div className="form-row">
          <label>Priority
            <select value={priority} onChange={(e) => setPriority(e.target.value)}>
              <option value="high">high</option>
              <option value="normal">normal</option>
              <option value="low">low</option>
            </select>
          </label>
          <label>Delay (ms)
            <input type="number" min="0" value={delay} onChange={(e) => setDelay(e.target.value)} />
          </label>
          <label>Max retries
            <input type="number" min="0" value={maxRetries} onChange={(e) => setMaxRetries(e.target.value)} />
          </label>
        </div>
        <label>Idempotency key (optional)
          <input value={idempotencyKey} onChange={(e) => setIdempotencyKey(e.target.value)} />
        </label>
        <button type="submit">Enqueue job</button>
      </form>
      {error && <div className="error">{error}</div>}
      {result && (
        <div className="success">
          Enqueued {result.deduped ? '(deduplicated — returned existing job)' : ''}: <span className="mono">{result.id}</span>
        </div>
      )}
    </div>
  );
}
