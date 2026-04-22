import http from 'k6/http';
import { check, sleep } from 'k6';

export let options = {
  vus: 10,
  duration: '30s',
  thresholds: {
    http_req_duration: ['p(99)<500'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

export default function () {
  let payload = {
    type: 'send_email',
    priority: 'high',
    payload: {
      to: 'test@example.com',
      subject: 'Load test email',
    },
    max_retries: 3,
  };

  let response = http.post(`${BASE_URL}/jobs`, JSON.stringify(payload), {
    headers: {
      'Content-Type': 'application/json',
    },
  });

  check(response, {
    'status is 200': (r) => r.status === 200,
    'response time < 100ms': (r) => r.timings.duration < 100,
  });

  sleep(0.1);
}