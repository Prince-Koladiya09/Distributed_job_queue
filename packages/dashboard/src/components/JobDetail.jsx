import React, { useEffect, useState } from 'react';
import { api } from '../api.js';

export default function JobDetail({ jobId, onClose, onChanged }) {
  const [job, setJob] = useState(null);
  const [error, setError] = useState(null);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    if (!jobId) return;
    let cancelled = false;
    api.getJob(jobId).then((res) => !cancelled && setJob(res)).catch((err) => !cancelled && setError(err.message));
    return () => { cancelled = true; };
  }, [jobId]);

  if (!jobId) return null;

  async function handleRetry() {
    setBusy(true);
    try {
      await api.retryJob(jobId);
      const refreshed = await api.getJob(jobId);
      setJob(refreshed);
      onChanged?.();
    } catch (err) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  }

  async function handleCancel() {
    setBusy(true);
    try {
      await api.cancelJob(jobId);
      const refreshed = await api.getJob(jobId);
      setJob(refreshed);
      onChanged?.();
    } catch (err) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="drawer">
      <div className="drawer-header">
        <h2>Job detail</h2>
        <button className="close-btn" onClick={onClose}>×</button>
      </div>
      {error && <div className="error">{error}</div>}
      {!job ? (
        <div className="muted">Loading…</div>
      ) : (
        <>
          <dl className="detail-list">
            <dt>ID</dt><dd className="mono">{job.id}</dd>
            <dt>Type</dt><dd>{job.type}</dd>
            <dt>Status</dt><dd><span className={`badge badge--status-${job.status}`}>{job.status}</span></dd>
            <dt>Priority</dt><dd><span className={`badge badge--${job.priority}`}>{job.priority}</span></dd>
            <dt>Attempt</dt><dd>{job.attempt} / {job.maxRetries}</dd>
            <dt>Worker</dt><dd>{job.workerId || '—'}</dd>
            <dt>Created</dt><dd>{new Date(job.createdAt).toLocaleString()}</dd>
            <dt>Updated</dt><dd>{new Date(job.updatedAt).toLocaleString()}</dd>
            {job.error && (<><dt>Last error</dt><dd className="error-text">{job.error}</dd></>)}
          </dl>

          <h3>Payload</h3>
          <pre className="code-block">{JSON.stringify(job.payload, null, 2)}</pre>

          <h3>Event history</h3>
          <ul className="event-list">
            {job.events?.map((ev) => (
              <li key={ev.id}>
                <span className="mono">{new Date(ev.created_at).toLocaleTimeString()}</span>{' '}
                <span className={`badge badge--status-${ev.status}`}>{ev.status}</span>{' '}
                <span>{ev.message}</span>
              </li>
            ))}
          </ul>

          <div className="actions">
            {(job.status === 'dead' || job.status === 'failed') && (
              <button disabled={busy} onClick={handleRetry}>Retry job</button>
            )}
            {(job.status === 'queued' || job.status === 'delayed') && (
              <button disabled={busy} className="danger" onClick={handleCancel}>Cancel job</button>
            )}
          </div>
        </>
      )}
    </div>
  );
}
