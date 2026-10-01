from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import unittest
from taskrelay import Queue, handle


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.queue = Queue(Path(self.temp.name) / 'queue.db')

    def test_concurrent_claims_are_unique(self):
        ids = {self.queue.enqueue(str(i), {'number': i}, now=0) for i in range(30)}
        with ThreadPoolExecutor(max_workers=8) as pool:
            claims = list(pool.map(lambda _: self.queue.claim(now=1), range(40)))
        claimed = [job['id'] for job in claims if job]
        self.assertEqual(set(claimed), ids)
        self.assertEqual(len(claimed), 30)

    def test_expired_worker_cannot_ack_reclaimed_job(self):
        job_id = self.queue.enqueue('a', {}, now=0)
        old = self.queue.claim(lease_seconds=5, now=0)
        self.assertFalse(self.queue.complete(job_id, old['token'], {}, now=5))
        new = self.queue.claim(now=5)
        self.assertNotEqual(old['token'], new['token'])
        self.assertFalse(self.queue.complete(job_id, old['token'], {}, now=6))
        self.assertTrue(self.queue.complete(job_id, new['token'], {'ok': True}, now=6))
        self.assertEqual(self.queue.inspect(job_id)['status'], 'done')

    def test_retry_delay_and_dead_letter(self):
        job_id = self.queue.enqueue('a', {}, max_attempts=2, now=0)
        job = self.queue.claim(now=0)
        self.assertTrue(self.queue.fail(job_id, job['token'], 'test failure', now=1))
        self.assertIsNone(self.queue.claim(now=2))
        job = self.queue.claim(now=3)
        self.queue.fail(job_id, job['token'], 'again', now=4)
        self.assertEqual(self.queue.inspect(job_id)['status'], 'dead')
        self.assertIsNone(self.queue.claim(now=100))

    def test_crashed_final_attempt_is_dead_lettered(self):
        job_id = self.queue.enqueue('a', {}, max_attempts=1, now=0)
        self.queue.claim(lease_seconds=1, now=0)
        self.assertIsNone(self.queue.claim(now=1))
        self.assertEqual(self.queue.inspect(job_id)['status'], 'dead')

    def test_idempotency_and_reopen(self):
        job_id = self.queue.enqueue('a', {'x': 1}, now=0)
        reopened = Queue(self.queue.path)
        self.assertEqual(reopened.enqueue('a', {'x': 1}), job_id)
        with self.assertRaises(ValueError):
            reopened.enqueue('a', {'x': 2})
        self.assertEqual(reopened.inspect()['pending'], 1)

    def test_heartbeat_and_handler(self):
        job_id = self.queue.enqueue('a', {}, now=0)
        job = self.queue.claim(lease_seconds=2, now=0)
        self.assertTrue(self.queue.heartbeat(job_id, job['token'], 10, now=1))
        self.assertIsNone(self.queue.claim(now=3))
        self.assertEqual(handle({'type': 'sha256', 'text': 'abc'})['sha256'],
                         'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad')

if __name__ == '__main__':
    unittest.main()
