import React, { useEffect, useState } from 'react';
import { api } from '../api.js';

const STATUS_OPTIONS = ['', 'queued', 'delayed', 'active', 'completed', 'failed', 'dead'];

export default function JobList({ onSelectJob, refreshToken }) {
  const [jobs, setJobs] = useState([]);
  const [total, setTotal] = useState(0);
  const [status, setStatus] = useState('');
  const [type, setType] = useState('');
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const pageSize = 10;

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    api
      .listJobs({ status, type, page, pageSize })
      .then((res) => {
        if (cancelled) return;
        setJobs(res.jobs);
        setTotal(res.total);
        setError(null);
      })
      .catch((err) => !cancelled && setError(err.message))
      .finally(() => !cancelled && setLoading(false));
    return () => {
      cancelled = true;
    };
  }, [status, type, page, refreshToken]);

  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  return (
    <div className="panel">
      <h2>Jobs</h2>
      <div className="filters">
        <select value={status} onChange={(e) => { setStatus(e.target.value); setPage(1); }}>
          {STATUS_OPTIONS.map((s) => (
            <option key={s || 'all'} value={s}>{s || 'All statuses'}</option>
          ))}
        </select>
        <input
          placeholder="Filter by type…"
          value={type}
          onChange={(e) => { setType(e.target.value); setPage(1); }}
        />
      </div>

      {error && <div className="error">{error}</div>}
      {loading && <div className="muted">Loading…</div>}

      <table className="job-table">
        <thead>
          <tr>
            <th>Type</th>
            <th>Priority</th>
            <th>Status</th>
            <th>Attempt</th>
            <th>Created</th>
          </tr>
        </thead>
        <tbody>
          {jobs.map((job) => (
            <tr key={job.id} onClick={() => onSelectJob(job.id)} className="job-row">
              <td>{job.type}</td>
              <td><span className={`badge badge--${job.priority}`}>{job.priority}</span></td>
              <td><span className={`badge badge--status-${job.status}`}>{job.status}</span></td>
              <td>{job.attempt}/{job.maxRetries}</td>
              <td>{new Date(job.createdAt).toLocaleString()}</td>
            </tr>
          ))}
          {jobs.length === 0 && !loading && (
            <tr><td colSpan={5} className="muted">No jobs match this filter.</td></tr>
          )}
        </tbody>
      </table>

      <div className="pager">
        <button disabled={page <= 1} onClick={() => setPage((p) => p - 1)}>Prev</button>
        <span>Page {page} / {totalPages} ({total} jobs)</span>
        <button disabled={page >= totalPages} onClick={() => setPage((p) => p + 1)}>Next</button>
      </div>
    </div>
  );
}
