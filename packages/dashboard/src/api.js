const API_BASE = import.meta.env.VITE_API_BASE || '/api';

function getApiKey() {
  return localStorage.getItem('jobqueue_api_key') || 'dev-key-123';
}

export function setApiKey(key) {
  localStorage.setItem('jobqueue_api_key', key);
}

async function request(path, options = {}) {
  const res = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': getApiKey(),
      ...(options.headers || {}),
    },
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    const err = new Error(body.message || `Request failed: ${res.status}`);
    err.status = res.status;
    err.body = body;
    throw err;
  }
  if (res.status === 204) return null;
  return res.json();
}

export const api = {
  getQueueStats: () => request('/queues/stats'),
  listJobs: (params = {}) => {
    const qs = new URLSearchParams(Object.entries(params).filter(([, v]) => v !== undefined && v !== ''));
    return request(`/jobs?${qs.toString()}`);
  },
  getJob: (id) => request(`/jobs/${id}`),
  retryJob: (id) => request(`/jobs/${id}/retry`, { method: 'POST' }),
  cancelJob: (id) => request(`/jobs/${id}`, { method: 'DELETE' }),
  enqueueJob: (payload) => request('/jobs', { method: 'POST', body: JSON.stringify(payload) }),
  listSchedules: () => request('/schedules'),
  createSchedule: (payload) => request('/schedules', { method: 'POST', body: JSON.stringify(payload) }),
  toggleSchedule: (id, enabled) => request(`/schedules/${id}`, { method: 'PATCH', body: JSON.stringify({ enabled }) }),
  deleteSchedule: (id) => request(`/schedules/${id}`, { method: 'DELETE' }),
};
