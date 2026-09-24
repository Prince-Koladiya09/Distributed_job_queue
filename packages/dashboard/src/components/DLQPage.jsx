import React, { useEffect, useState } from 'react';
import { api } from '../api.js';

export default function DLQPage({ onSelectJob, refreshToken }) {
  const [jobs, setJobs] = useState([]);
  const [error, setError] = useState(null);
  const [busyId, setBusyId] = useState(null);

  async function load() {
    try {
      const res = await api.listJobs({ status: 'dead', pageSize: 50 });
      setJobs(res.jobs);
    } catch (err) {
      setError(err.message);
    }
  }

  useEffect(() => { load(); }, [refreshToken]);

  async function retryAll() {
    for (const job of jobs) {
      setBusyId(job.id);
      // eslint-disable-next-line no-await-in-loop
      await api.retryJob(job.id).catch(() => {});
    }
    setBusyId(null);
    load();
  }

  return (
    <div className="panel">
      <div className="panel-header-row">
        <h2>Dead-letter queue ({jobs.length})</h2>
        {jobs.length > 0 && <button onClick={retryAll}>Retry all</button>}
      </div>
      {error && <div className="error">{error}</div>}
      <table className="job-table">
        <thead>
          <tr>
            <th>Type</th>
            <th>Attempts</th>
            <th>Last error</th>
            <th>Updated</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {jobs.map((job) => (
            <tr key={job.id} className="job-row">
              <td onClick={() => onSelectJob(job.id)}>{job.type}</td>
              <td>{job.attempt}/{job.maxRetries}</td>
              <td className="error-text truncate">{job.error}</td>
              <td>{new Date(job.updatedAt).toLocaleString()}</td>
              <td>
                <button
                  disabled={busyId === job.id}
                  onClick={async () => {
                    setBusyId(job.id);
                    await api.retryJob(job.id).catch((e) => setError(e.message));
                    setBusyId(null);
                    load();
                  }}
                >
                  Retry
                </button>
              </td>
            </tr>
          ))}
          {jobs.length === 0 && <tr><td colSpan={5} className="muted">DLQ is empty 🎉</td></tr>}
        </tbody>
      </table>
    </div>
  );
}
