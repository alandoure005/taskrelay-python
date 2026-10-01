"""SQLite job queue with expiring leases and fenced acknowledgements."""
import argparse
from contextlib import contextmanager
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import time
import uuid


class Queue:
    def __init__(self, path):
        self.path = str(path)
        with self.connect() as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.executescript('''
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY, key TEXT NOT NULL UNIQUE, payload TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('pending','running','done','dead')),
                    attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL,
                    available REAL NOT NULL, lease_until REAL, token TEXT,
                    result TEXT, error TEXT);
                CREATE INDEX IF NOT EXISTS ready_jobs ON jobs(status, available);
            ''')

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def enqueue(self, key, payload, max_attempts=3, now=None):
        if not isinstance(key, str) or not 1 <= len(key) <= 128:
            raise ValueError('key must have 1–128 characters')
        if type(max_attempts) is not int or not 1 <= max_attempts <= 100:
            raise ValueError('max_attempts must be an integer from 1 to 100')
        encoded = json.dumps(payload, sort_keys=True, allow_nan=False)
        if len(encoded.encode()) > 65536:
            raise ValueError('payload exceeds 64 KiB')
        now = time.time() if now is None else now
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM jobs WHERE key=?', (key,)).fetchone()
            if row:
                if row['payload'] != encoded or row['max_attempts'] != max_attempts:
                    raise ValueError('idempotency key reused with different parameters')
                return row['id']
            job_id = uuid.uuid4().hex
            db.execute('INSERT INTO jobs(id,key,payload,status,max_attempts,available) VALUES(?,?,?,\'pending\',?,?)',
                       (job_id, key, encoded, max_attempts, now))
            return job_id

    def claim(self, lease_seconds=30, now=None):
        if not math.isfinite(lease_seconds) or lease_seconds <= 0:
            raise ValueError('lease_seconds must be positive and finite')
        now = time.time() if now is None else now
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('''UPDATE jobs SET status=CASE WHEN attempts>=max_attempts THEN 'dead' ELSE 'pending' END,
                       token=NULL, lease_until=NULL, available=?, error='worker lease expired'
                       WHERE status='running' AND lease_until<=?''', (now, now))
            row = db.execute("SELECT * FROM jobs WHERE status='pending' AND available<=? ORDER BY available,id LIMIT 1", (now,)).fetchone()
            if row is None:
                return None
            token = uuid.uuid4().hex
            db.execute("UPDATE jobs SET status='running', attempts=attempts+1,token=?,lease_until=? WHERE id=?",
                       (token, now + lease_seconds, row['id']))
            return {'id': row['id'], 'token': token, 'payload': json.loads(row['payload']),
                    'attempt': row['attempts'] + 1, 'lease_until': now + lease_seconds}

    def complete(self, job_id, token, result, now=None):
        now = time.time() if now is None else now
        encoded = json.dumps(result, allow_nan=False)
        with self.connect() as db:
            changed = db.execute("""UPDATE jobs SET status='done',result=?,token=NULL,lease_until=NULL,error=NULL
                                WHERE id=? AND token=? AND status='running' AND lease_until>?""",
                                 (encoded, job_id, token, now)).rowcount
            return changed == 1

    def fail(self, job_id, token, error, now=None):
        now = time.time() if now is None else now
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute("SELECT * FROM jobs WHERE id=? AND token=? AND status='running' AND lease_until>?",
                             (job_id, token, now)).fetchone()
            if not row:
                return False
            status = 'dead' if row['attempts'] >= row['max_attempts'] else 'pending'
            delay = min(300, 2 ** min(row['attempts'], 9))
            db.execute('UPDATE jobs SET status=?,available=?,token=NULL,lease_until=NULL,error=? WHERE id=?',
                       (status, now + delay, str(error)[:1000], job_id))
            return True

    def heartbeat(self, job_id, token, lease_seconds=30, now=None):
        if not math.isfinite(lease_seconds) or lease_seconds <= 0:
            raise ValueError('lease_seconds must be positive and finite')
        now = time.time() if now is None else now
        with self.connect() as db:
            return db.execute("UPDATE jobs SET lease_until=? WHERE id=? AND token=? AND status='running' AND lease_until>?",
                              (now + lease_seconds, job_id, token, now)).rowcount == 1

    def inspect(self, job_id=None):
        with self.connect() as db:
            if job_id:
                row = db.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone()
                return dict(row) if row else None
            counts = {status: 0 for status in ('pending', 'running', 'done', 'dead')}
            counts.update({row['status']: row['n'] for row in db.execute('SELECT status,count(*) AS n FROM jobs GROUP BY status')})
            return counts


def handle(payload):
    if not isinstance(payload, dict) or payload.get('type') != 'sha256' or not isinstance(payload.get('text'), str):
        raise ValueError('supported job: {"type":"sha256","text":"..."}')
    return {'sha256': hashlib.sha256(payload['text'].encode()).hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', default='queue.db')
    sub = parser.add_subparsers(dest='command', required=True)
    enqueue = sub.add_parser('enqueue')
    enqueue.add_argument('--key', required=True)
    enqueue.add_argument('--text', required=True)
    worker = sub.add_parser('worker')
    worker.add_argument('--once', action='store_true', help='attempt one ready job, then exit')
    sub.add_parser('stats')
    show = sub.add_parser('show')
    show.add_argument('id')
    args = parser.parse_args()
    queue = Queue(args.db)
    if args.command == 'enqueue':
        print(queue.enqueue(args.key, {'type': 'sha256', 'text': args.text}))
    elif args.command in ('stats', 'show'):
        print(json.dumps(queue.inspect(getattr(args, 'id', None)), indent=2))
    else:
        try:
            while True:
                job = queue.claim()
                if job:
                    try:
                        result = handle(job['payload'])
                    except Exception as error:
                        accepted = queue.fail(job['id'], job['token'], str(error))
                        print(json.dumps({'id': job['id'], 'event': 'failed', 'accepted': accepted}), flush=True)
                    else:
                        accepted = queue.complete(job['id'], job['token'], result)
                        print(json.dumps({'id': job['id'], 'event': 'completed', 'accepted': accepted}), flush=True)
                if args.once:
                    break
                if job is None:
                    time.sleep(0.5)
        except KeyboardInterrupt:
            pass


if __name__ == '__main__':
    main()
