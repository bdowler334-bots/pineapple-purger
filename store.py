"""Serialized SQLite operations off the Discord event loop."""
import asyncio
import sqlite3
import secrets
from pathlib import Path


class Store:
    def __init__(self, path):
        self.path = str(path)
        self.lock = asyncio.Lock()

    async def call(self, operation):
        async with self.lock:
            def run():
                Path(self.path).parent.mkdir(parents=True, exist_ok=True)
                with sqlite3.connect(self.path, timeout=30) as conn:
                    conn.row_factory = sqlite3.Row
                    return operation(conn)
            # Shield the thread and wait for it even on cancellation: never release
            # the serialization lock while the transaction is still running.
            task = asyncio.create_task(asyncio.to_thread(run))
            try:
                return await asyncio.shield(task)
            except asyncio.CancelledError:
                await task
                raise

    async def initialize(self):
        await self.call(lambda c: c.executescript('''
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS media_messages (
                message_id INTEGER PRIMARY KEY, guild_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL, author_id INTEGER NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_guild_author
                ON media_messages(guild_id, author_id);
            CREATE TABLE IF NOT EXISTS cleanup_scope (
                guild_id INTEGER NOT NULL, channel_id INTEGER NOT NULL,
                PRIMARY KEY(guild_id, channel_id)
            );
            CREATE TABLE IF NOT EXISTS cleanup_jobs (
                guild_id INTEGER NOT NULL, author_id INTEGER NOT NULL,
                cutoff INTEGER NOT NULL, mode TEXT NOT NULL,
                requested_by INTEGER NOT NULL, next_try REAL NOT NULL,
                job_key TEXT NOT NULL,
                deleted INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(guild_id, author_id)
            );
        '''))

    async def scope(self, guild):
        return await self.call(lambda c: {int(r[0]) for r in c.execute(
            'SELECT channel_id FROM cleanup_scope WHERE guild_id=?', (guild,))})

    async def is_scoped(self, guild, channel, parent=None):
        allowed = await self.scope(guild)
        return channel in allowed or (parent is not None and parent in allowed)

    async def set_scope(self, guild, channel, enabled):
        def op(c):
            if enabled:
                c.execute('INSERT OR IGNORE INTO cleanup_scope VALUES (?,?)', (guild, channel))
            else:
                c.execute('DELETE FROM cleanup_scope WHERE guild_id=? AND channel_id=?',
                          (guild, channel))
        await self.call(op)

    async def save_many(self, messages):
        if messages:
            await self.call(lambda c: c.executemany('''
                INSERT OR IGNORE INTO media_messages VALUES (?, ?, ?, ?, ?)
            ''', messages))

    async def forget(self, ids):
        if ids:
            await self.call(lambda c: c.executemany(
                'DELETE FROM media_messages WHERE message_id=?', [(i,) for i in ids]))

    async def rows(self, guild, author, cutoff=None):
        return await self.call(lambda c: [dict(r) for r in c.execute('''
            SELECT * FROM media_messages WHERE guild_id=? AND author_id=?
                AND message_id<=? ORDER BY message_id
        ''', (guild, author, cutoff or 9223372036854775807))])

    async def count(self, guild):
        return await self.call(lambda c: c.execute(
            'SELECT COUNT(*) FROM media_messages WHERE guild_id=?', (guild,)).fetchone()[0])

    async def authors(self, guild):
        return await self.call(lambda c: [r[0] for r in c.execute(
            'SELECT DISTINCT author_id FROM media_messages WHERE guild_id=?', (guild,))])

    async def queue(self, guild, author, cutoff, mode, requester, due):
        # Existing jobs keep their exact snapshot and mode; no concurrent job
        # can silently expand a previously confirmed manual deletion.
        def op(c):
            cursor = c.execute('''
                INSERT OR IGNORE INTO cleanup_jobs
                (guild_id,author_id,cutoff,mode,requested_by,next_try,job_key)
                VALUES(?,?,?,?,?,?,?)
            ''', (guild, author, cutoff, mode, requester, due, secrets.token_hex(16)))
            return cursor.rowcount == 1
        return await self.call(op)

    async def jobs(self, guild, due=None):
        return await self.call(lambda c: [dict(r) for r in c.execute('''
            SELECT * FROM cleanup_jobs WHERE guild_id=? AND next_try<=?
            ORDER BY next_try
        ''', (guild, due if due is not None else 1e30))])

    async def cancel(self, guild, author, automatic_only=False, job_key=None):
        sql = 'DELETE FROM cleanup_jobs WHERE guild_id=? AND author_id=?'
        params = [guild, author]
        if automatic_only:
            sql += " AND mode='automatic'"
        if job_key is not None:
            sql += ' AND job_key=?'
            params.append(job_key)
        await self.call(lambda c: c.execute(sql, params))

    async def checkpoint_deleted(self, guild, author, message, job_key):
        # The successful deletion and job counter are recorded together.
        def op(c):
            c.execute('DELETE FROM media_messages WHERE message_id=?', (message,))
            c.execute('''UPDATE cleanup_jobs SET deleted=deleted+1
                WHERE guild_id=? AND author_id=? AND job_key=?''', (guild, author, job_key))
        await self.call(op)

    async def retry(self, guild, author, due, job_key=None):
        sql = 'UPDATE cleanup_jobs SET next_try=? WHERE guild_id=? AND author_id=?'
        params = [due, guild, author]
        if job_key is not None:
            sql += ' AND job_key=?'
            params.append(job_key)
        await self.call(lambda c: c.execute(sql, params))
