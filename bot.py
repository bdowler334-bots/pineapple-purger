"""Pineapple Media Cleaner: tracked media removal after departure."""
import asyncio
import logging
import os
import secrets
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, time as datetime_time, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path

import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv
from store import Store

log = logging.getLogger('pineapple-media-cleaner')


def id_set(value):
    return {int(x.strip()) for x in value.split(',') if x.strip()}


@dataclass(frozen=True)
class Config:
    guild_id: int
    log_channel_id: int = 0
    database: str = 'data/media_tracker.db'
    media_mode: str = 'images_videos'
    protected: frozenset = frozenset()
    excluded: frozenset = frozenset()
    auto_cleanup: bool = True
    grace_seconds: int = 60
    retry_seconds: int = 300
    batch_size: int = 50
    batch_delay: float = 2.0

    @classmethod
    def from_env(cls):
        mode = os.getenv('MEDIA_MODE', 'images_videos')
        if mode not in {'images_videos', 'attachments', 'attachments_embeds'}:
            raise ValueError('MEDIA_MODE must be images_videos, attachments, or attachments_embeds')
        gid = int(os.getenv('GUILD_ID', '0'))
        if gid <= 0:
            raise ValueError('GUILD_ID must be your numeric Discord server ID')
        return cls(guild_id=gid,
                   log_channel_id=int(os.getenv('LOG_CHANNEL_ID', '0')),
                   database=os.getenv('DATABASE_FILE', 'data/media_tracker.db'),
                   media_mode=mode,
                   protected=frozenset(id_set(os.getenv('PROTECTED_USER_IDS', ''))),
                   excluded=frozenset(id_set(os.getenv('EXCLUDED_CHANNEL_IDS', ''))),
                   auto_cleanup=os.getenv('AUTO_CLEANUP', 'true').lower() == 'true',
                   grace_seconds=max(0, int(os.getenv('DEPARTURE_GRACE_SECONDS', '60'))))


def next_daily_cleanup(now=None):
    now = now or datetime.now(timezone.utc)
    eastern = ZoneInfo('America/New_York')
    local = now.astimezone(eastern)
    due = datetime.combine(local.date(), datetime_time(13, 0), tzinfo=eastern)
    if due <= local:
        due += timedelta(days=1)
    return due.astimezone(timezone.utc).timestamp()


def contains_media(message, mode):
    if mode == 'attachments_embeds':
        return bool(message.attachments or message.embeds)
    if mode == 'attachments':
        return bool(message.attachments)
    for attachment in message.attachments:
        mime = (attachment.content_type or '').lower()
        if mime.startswith(('image/', 'video/')):
            return True
        # Only fall back to filename when Discord supplies no MIME type.
        if not mime and Path(attachment.filename).suffix.lower() in {
            '.png', '.jpg', '.jpeg', '.gif', '.webp', '.avif', '.heic',
            '.mp4', '.mov', '.webm', '.mkv', '.avi', '.m4v'}:
            return True
    return False


def excluded_channel(channel, exclusions):
    return (channel.id in exclusions or
            getattr(channel, 'parent_id', None) in exclusions or
            getattr(channel, 'category_id', None) in exclusions)


def record(message):
    return (message.id, message.guild.id, message.channel.id,
            message.author.id, message.created_at.isoformat())


class Cleaner(commands.Bot):
    def __init__(self, config):
        intents = discord.Intents.default()
        intents.members = True
        intents.message_content = True
        super().__init__(command_prefix='!', intents=intents,
                         allowed_mentions=discord.AllowedMentions.none())
        self.config = config
        self.store = Store(config.database)
        self.pending = {}
        self.scanning = False
        self.operation_lock = asyncio.Lock()

    async def setup_hook(self):
        await self.store.initialize()
        self.worker.start()
        self.reconcile_loop.start()

    async def close(self):
        self.worker.cancel()
        self.reconcile_loop.cancel()
        await super().close()

    async def on_ready(self):
        log.info('Connected as %s; configured server %s; mode %s; automatic=%s',
                 self.user, self.config.guild_id, self.config.media_mode,
                 self.config.auto_cleanup)
        if self.get_guild(self.config.guild_id) is None:
            log.error('Configured server is unavailable. Check GUILD_ID and bot installation.')

    async def track(self, message):
        if (message.guild is None or message.guild.id != self.config.guild_id or
                message.author.bot):
            return
        if excluded_channel(message.channel, self.config.excluded):
            return
        if not await self.store.is_scoped(self.config.guild_id, message.channel.id,
                                          getattr(message.channel, 'parent_id', None)):
            return
        if contains_media(message, self.config.media_mode):
            await self.store.save_many([record(message)])
        else:
            await self.store.forget([message.id])

    async def on_message(self, message):
        try:
            await self.track(message)
        except Exception:
            log.exception('Could not track message %s', message.id)
        await self.process_commands(message)

    async def on_raw_message_edit(self, payload):
        if payload.guild_id != self.config.guild_id:
            return
        try:
            channel = await self.resolve_channel(payload.channel_id)
            await self.track(await channel.fetch_message(payload.message_id))
        except discord.NotFound:
            await self.store.forget([payload.message_id])
        except discord.HTTPException:
            log.exception('Could not refresh edited message %s', payload.message_id)

    async def on_raw_message_delete(self, payload):
        if payload.guild_id == self.config.guild_id:
            await self.store.forget([payload.message_id])

    async def on_raw_bulk_message_delete(self, payload):
        if payload.guild_id == self.config.guild_id:
            await self.store.forget(payload.message_ids)

    async def on_raw_member_remove(self, payload):
        if payload.guild_id == self.config.guild_id and self.config.auto_cleanup:
            await self.queue_departure(payload.user.id)

    async def on_member_join(self, member):
        if member.guild.id == self.config.guild_id:
            await self.store.cancel(member.guild.id, member.id, automatic_only=True)

    async def queue_departure(self, author):
        if author in self.config.protected:
            return
        rows = await self.store.rows(self.config.guild_id, author)
        if rows:
            await self.store.queue(self.config.guild_id, author,
                                   max(r['message_id'] for r in rows), 'automatic', 0,
                                   max(time.time() + self.config.grace_seconds, next_daily_cleanup()))

    async def resolve_channel(self, channel_id):
        guild = self.get_guild(self.config.guild_id)
        channel = guild.get_channel_or_thread(channel_id) if guild else None
        channel = channel or await self.fetch_channel(channel_id)
        if getattr(getattr(channel, 'guild', None), 'id', None) != self.config.guild_id:
            raise RuntimeError('Channel belongs to a different server')
        return channel

    async def absent(self, guild, author):
        # A cache miss is never sufficient to authorize deletion.
        try:
            await guild.fetch_member(author)
            return False
        except discord.NotFound:
            return True

    async def reconcile(self):
        guild = self.get_guild(self.config.guild_id)
        if not guild or not self.config.auto_cleanup:
            return
        # Do not act on a partial member list. Exceptions abort the whole pass.
        present = {member.id async for member in guild.fetch_members(limit=None)}
        for author in await self.store.authors(guild.id):
            if author not in present:
                await self.queue_departure(author)
        for job in await self.store.jobs(guild.id):
            if job['mode'] == 'automatic' and job['author_id'] in present:
                await self.store.cancel(guild.id, job['author_id'], automatic_only=True)

    @tasks.loop(minutes=15)
    async def reconcile_loop(self):
        try:
            await self.reconcile()
        except Exception:
            log.exception('Membership reconciliation failed; no incomplete list used')

    @reconcile_loop.before_loop
    async def before_reconcile(self):
        await self.wait_until_ready()

    @tasks.loop(seconds=10)
    async def worker(self):
        if not self.is_ready():
            return
        # A scan and cleanup do not run together. Indexing never silently
        # broadens the cutoff of a previously confirmed job.
        if self.operation_lock.locked():
            return
        async with self.operation_lock:
            try:
                for job in await self.store.jobs(self.config.guild_id, time.time()):
                    try:
                        await self.clean(job)
                    except Exception:
                        log.exception('Cleanup job failed; records and job retained')
                        await self.store.retry(job['guild_id'], job['author_id'],
                                               time.time() + self.config.retry_seconds, job['job_key'])
            except Exception:
                log.exception('Worker pass failed; retry on next pass')

    @worker.before_loop
    async def before_worker(self):
        await self.wait_until_ready()

    async def clean(self, job):
        guild = self.get_guild(job['guild_id'])
        if guild is None:
            return
        author = job['author_id']
        if author in self.config.protected or (
                job['mode'] == 'automatic' and not self.config.auto_cleanup):
            await self.store.cancel(guild.id, author, job_key=job['job_key'])
            return
        # Cancelled jobs (e.g. rejoin) must not be resurrected by a stale snapshot.
        if not any(j['job_key'] == job['job_key'] for j in await self.store.jobs(guild.id)):
            return
        if job['mode'] == 'automatic' and not await self.absent(guild, author):
            await self.store.cancel(guild.id, author, automatic_only=True, job_key=job['job_key'])
            return
        deleted = job['deleted']
        failures = missing = skipped = 0
        rows = await self.store.rows(guild.id, author, job['cutoff'])
        for index, row in enumerate(rows):
            if index % self.config.batch_size == 0:
                if index:
                    await asyncio.sleep(self.config.batch_delay)
                if job['mode'] == 'automatic' and not await self.absent(guild, author):
                    await self.store.cancel(guild.id, author, automatic_only=True, job_key=job['job_key'])
                    await self.cleanup_log(author, deleted, failures, skipped, missing,
                                           'Cancelled: member rejoined')
                    return
            mid = row['message_id']
            # Respect current channel, category and thread-parent exclusions.
            if row['channel_id'] in self.config.excluded:
                skipped += 1
                continue
            try:
                channel = await self.resolve_channel(row['channel_id'])
                if (excluded_channel(channel, self.config.excluded) or
                        not await self.store.is_scoped(guild.id, channel.id,
                                                       getattr(channel, 'parent_id', None))):
                    skipped += 1
                    continue
                message = await channel.fetch_message(mid)
                if message.author.id != author:
                    log.error('Author mismatch for message %s; stale record removed', mid)
                    await self.store.forget([mid])
                    skipped += 1
                    continue
                if not contains_media(message, self.config.media_mode):
                    await self.store.forget([mid])
                    skipped += 1
                    continue
                if not any(j['job_key'] == job['job_key'] for j in await self.store.jobs(guild.id)):
                    return
                if job['mode'] == 'automatic' and not await self.absent(guild, author):
                    await self.store.cancel(guild.id, author, automatic_only=True, job_key=job['job_key'])
                    await self.cleanup_log(author, deleted, failures, skipped, missing,
                                           'Cancelled: member rejoined')
                    return
                reason = ('Departing member media cleanup' if job['mode'] == 'automatic'
                          else f'Manual media cleanup requested by {job["requested_by"]}')
                await message.delete()
                await self.store.checkpoint_deleted(guild.id, author, mid, job['job_key'])
                deleted += 1
            except discord.NotFound:
                await self.store.forget([mid])
                missing += 1
            except discord.HTTPException:
                failures += 1
                log.exception('Message %s retained for retry', mid)
        if failures:
            await self.store.retry(guild.id, author, time.time() + self.config.retry_seconds, job['job_key'])
            state = 'Pending retry in 5 minutes'
        else:
            await self.store.cancel(guild.id, author, job_key=job['job_key'])
            state = 'Complete'
        await self.cleanup_log(author, deleted, failures, skipped, missing, state)

    async def cleanup_log(self, author, deleted, failures, skipped, missing, state):
        log.info('Cleanup %s: %s; deleted total=%s, failed this pass=%s, skipped=%s, missing=%s',
                 author, state, deleted, failures, skipped, missing)
        if not self.config.log_channel_id:
            return
        embed = discord.Embed(title='🍍 Departing Member Media Cleanup',
                              description=state, colour=discord.Colour.orange(),
                              timestamp=datetime.now(timezone.utc))
        embed.add_field(name='Member', value=f'<@{author}> (`{author}`)', inline=False)
        embed.add_field(name='Deleted across job', value=str(deleted))
        embed.add_field(name='Failed this pass', value=str(failures))
        embed.add_field(name='Skipped this pass', value=str(skipped))
        embed.add_field(name='Already missing this pass', value=str(missing))
        try:
            channel = await self.resolve_channel(self.config.log_channel_id)
            await channel.send(embed=embed)
        except Exception:
            log.exception('Could not send cleanup report; see console logs')


async def admin_gate(ctx):
    if ctx.guild is None or ctx.guild.id != ctx.bot.config.guild_id:
        raise commands.CheckFailure('Use this command in the configured server.')
    if not ctx.author.guild_permissions.administrator:
        raise commands.MissingPermissions(['administrator'])
    return True


def install_commands(bot):
    @bot.command()
    @commands.check(admin_gate)
    async def media_scope(ctx, action: str = 'list',
                          channel: discord.TextChannel | discord.ForumChannel = None):
        action = action.lower()
        if action == 'add' and channel is not None:
            await bot.store.set_scope(ctx.guild.id, channel.id, True)
            await ctx.send(f'🍍 Added <#{channel.id}> to the approved cleanup scope.')
        elif action == 'remove' and channel is not None:
            await bot.store.set_scope(ctx.guild.id, channel.id, False)
            await ctx.send(f'🍍 Removed <#{channel.id}> from the approved cleanup scope.')
        elif action == 'list' and channel is None:
            ids = sorted(await bot.store.scope(ctx.guild.id))
            await ctx.send('🍍 Approved channels: ' +
                           (', '.join(f'<#{cid}>' for cid in ids) or
                            '**none** (cleanup is fail-closed).'))
        elif action == 'clear' and channel is None:
            for cid in await bot.store.scope(ctx.guild.id):
                await bot.store.set_scope(ctx.guild.id, cid, False)
            await ctx.send('🍍 Approved scope cleared; cleanup is now fail-closed.')
        else:
            await ctx.send('Usage: !media_scope list, !media_scope add #channel, ' +
                           '!media_scope remove #channel, or !media_scope clear.')

    @bot.command()
    @commands.check(admin_gate)
    async def media_doctor(ctx):
        scoped = await bot.store.scope(ctx.guild.id)
        jobs = await bot.store.jobs(ctx.guild.id)
        total = await bot.store.count(ctx.guild.id)
        checks = []
        try:
            await bot.store.call(lambda c: c.execute('SELECT 1').fetchone()[0])
            checks.append(('Database', True, bot.config.database))
        except Exception as exc:
            checks.append(('Database', False, type(exc).__name__))
        checks.append(('Approved cleanup scope', bool(scoped),
                       f'{len(scoped)} channel(s); add media and introduction channels with !media_scope add #channel'))
        checks.append(('Automatic cleanup', bot.config.auto_cleanup,
                       'enabled' if bot.config.auto_cleanup else 'disabled'))
        for channel_id in sorted(scoped)[:10]:
            channel = ctx.guild.get_channel_or_thread(channel_id)
            if channel is None:
                checks.append((f'Channel {channel_id}', False, 'not visible to the bot'))
                continue
            perms = channel.permissions_for(ctx.guild.me)
            ok = perms.view_channel and perms.read_message_history and perms.manage_messages
            checks.append((f'Channel {getattr(channel, "name", channel_id)} permissions', ok,
                           'View Channel, Read Message History, Manage Messages'))
        if bot.config.log_channel_id:
            report_channel = ctx.guild.get_channel(bot.config.log_channel_id)
            if report_channel is None:
                checks.append(('Cleanup log channel', False, 'missing or inaccessible'))
            else:
                checks.append(('Cleanup log channel', report_channel.permissions_for(ctx.guild.me).send_messages,
                               f'<#{bot.config.log_channel_id}>'))
        else:
            checks.append(('Cleanup log channel', False, 'LOG_CHANNEL_ID is unset'))
        lines = [f'{"✅" if ok else "⚠️"} **{name}:** {detail}' for name, ok, detail in checks]
        due = int(next_daily_cleanup())
        await ctx.send('🍍🩺 **Purger Troubleshooting**\n' +
                       '\n'.join(lines) +
                       f'\n**Tracked:** {total} | **Pending jobs:** {len(jobs)}' +
                       f'\n**Next automatic cleanup window:** <t:{due}:F>' +
                       '\nSee !media_jobs for retries. If permissions pass but deletion still fails, check Railway logs for the Discord API error.')

    @bot.command()
    @commands.check(admin_gate)
    async def media_stats(ctx):
        total = await bot.store.count(ctx.guild.id)
        jobs = await bot.store.jobs(ctx.guild.id)
        await ctx.send(f'🍍 Tracking **{total}** messages; **{len(jobs)}** pending cleanup jobs. '
                       f'Mode: `{bot.config.media_mode}`. Automatic: **{bot.config.auto_cleanup}**. Daily window: **1 p.m. Eastern**.')

    @bot.command()
    @commands.check(admin_gate)
    async def media_user(ctx, user_id: str):
        author = parse_user(user_id)
        rows = await bot.store.rows(ctx.guild.id, author)
        await ctx.send(f'🍍 <@{author}> has **{len(rows)}** tracked media messages.')

    @bot.command()
    @commands.check(admin_gate)
    async def media_preview(ctx, user_id: str):
        author = parse_user(user_id)
        rows = await bot.store.rows(ctx.guild.id, author)
        counts = Counter(r['channel_id'] for r in rows)
        lines = [f'• <#{cid}>: {count}' for cid, count in counts.most_common(12)]
        await ctx.send(f'🍍 **{len(rows)}** tracked candidates for <@{author}>.\n' +
                       '\n'.join(lines) + ('\nAdditional channels omitted.' if len(counts) > 12 else '') +
                       '\nCurrent media type and exclusions are checked again before deletion.')

    @bot.command()
    @commands.check(admin_gate)
    async def media_cleanup(ctx, user_id: str):
        author = parse_user(user_id)
        if author in bot.config.protected:
            await ctx.send('🛡️ This user is protected from automatic and manual cleanup.')
            return
        if any(j['author_id'] == author for j in await bot.store.jobs(ctx.guild.id)):
            await ctx.send('A cleanup job already exists for this user. See `!media_jobs`.')
            return
        rows = await bot.store.rows(ctx.guild.id, author)
        if not rows:
            await ctx.send('No tracked media messages found.')
            return
        # Purge expired prompts to bound memory usage.
        bot.pending = {k: v for k, v in bot.pending.items() if v['expires'] > time.time()}
        token = secrets.token_hex(4)
        bot.pending[token] = dict(author=author, requester=ctx.author.id,
                                  channel=ctx.channel.id, guild=ctx.guild.id,
                                  cutoff=max(r['message_id'] for r in rows),
                                  expires=time.time() + 60)
        await media_preview.callback(ctx, user_id)
        await ctx.send(f'This deletes entire qualifying messages, including captions. '
                       f'Confirm within **60 seconds** using `!media_confirm {token}`. '
                       'Only you can confirm, in this channel. Let it expire to cancel.')

    @bot.command()
    @commands.check(admin_gate)
    async def media_confirm(ctx, token: str):
        pending = bot.pending.get(token)
        if (not pending or pending['expires'] < time.time() or
                pending['requester'] != ctx.author.id or
                pending['channel'] != ctx.channel.id or pending['guild'] != ctx.guild.id):
            await ctx.send('Invalid, expired, or wrong-user confirmation.')
            return
        del bot.pending[token]
        if pending['author'] in bot.config.protected:
            await ctx.send('That user is protected.')
            return
        queued = await bot.store.queue(ctx.guild.id, pending['author'], pending['cutoff'],
                                       'manual', ctx.author.id, time.time())
        await ctx.send('🍍 Cleanup queued. Check `!media_jobs` and the cleanup log.' if queued
                       else 'A job already exists for that user; confirmation did not change it.')

    @bot.command()
    @commands.check(admin_gate)
    async def media_jobs(ctx):
        jobs = await bot.store.jobs(ctx.guild.id)
        lines = [f'• <@{j["author_id"]}>: {j["mode"]}, deleted={j["deleted"]}, '
                 f'next attempt <t:{int(j["next_try"])}:R>' for j in jobs[:12]]
        await ctx.send(f'🍍 **{len(jobs)}** pending jobs.\n' + '\n'.join(lines) +
                       ('\nAdditional jobs omitted.' if len(jobs) > 12 else ''))

    @bot.command()
    @commands.check(admin_gate)
    async def media_reconcile(ctx):
        await ctx.send('Checking the full server member list for missed departures…')
        await bot.reconcile()
        await ctx.send('🍍 Membership check finished. Absent tracked users were queued '
                       'if automatic cleanup is enabled.')

    @bot.command()
    @commands.check(admin_gate)
    async def media_cancel(ctx, user_id: str):
        author = parse_user(user_id)
        await bot.store.cancel(ctx.guild.id, author)
        await ctx.send('Pending job cancelled. A request already sent to Discord may finish. '
                       'For permanent protection, add this user to PROTECTED_USER_IDS and restart. '
                       'Absent users may be queued again at the next membership check.')

    @bot.command()
    @commands.check(admin_gate)
    async def media_scan(ctx, channel: discord.TextChannel | discord.ForumChannel = None,
                         limit: int = 0):
        if limit < 0:
            raise commands.BadArgument('Limit must be 0 (all messages from the last 12 months) or positive.')
        if bot.scanning:
            await ctx.send('A history scan is already running.')
            return
        bot.scanning = True
        status = await ctx.send('🍍 Waiting for current cleanup, then scanning approved channels from the last 12 months…')
        try:
            async with bot.operation_lock:
                await scan_history(bot, ctx, status, channel, limit)
        finally:
            bot.scanning = False

    @bot.event
    async def on_command_error(ctx, error):
        if isinstance(error, commands.CommandNotFound):
            return
        if isinstance(error, commands.MissingPermissions):
            await ctx.send('You need server Administrator permission.')
        elif isinstance(error, (commands.BadArgument, commands.MissingRequiredArgument,
                                commands.CheckFailure)):
            await ctx.send(str(error)[:1500])
        else:
            log.error('Command failed', exc_info=(type(error), error, error.__traceback__))
            await ctx.send('The command failed. Check console logs; tracked records are retained.')


def parse_user(value):
    value = value.strip().removeprefix('<@').removeprefix('!').removesuffix('>')
    if not value.isdecimal() or int(value) <= 0:
        raise commands.BadArgument('Use a member mention or numeric user ID.')
    return int(value)


async def scan_history(bot, ctx, status, selected, limit):
    config = bot.config
    allowed_ids = await bot.store.scope(ctx.guild.id)
    bases = ([selected] if selected else
             [c for c in ctx.guild.channels
              if (hasattr(c, 'history') or isinstance(c, discord.ForumChannel))
              and c.id in allowed_ids])
    channels = {c.id: c for c in bases if hasattr(c, 'history') and
                not excluded_channel(c, config.excluded)}
    errors = []
    try:
        active_threads = await ctx.guild.active_threads()
    except discord.HTTPException as exc:
        active_threads = ctx.guild.threads
        errors.append(f'Active thread listing failed ({exc.status}); used cached threads')
    for base in bases:
        if (excluded_channel(base, config.excluded) or
                not await bot.store.is_scoped(ctx.guild.id, base.id,
                                              getattr(base, 'parent_id', None))):
            continue
        for thread in active_threads:
            if thread.parent_id == base.id:
                channels[thread.id] = thread
        if hasattr(base, 'archived_threads'):
            # Public archives plus joined private archives; unjoined private
            # archives require Manage Threads and are reported if unavailable.
            variants = [dict(limit=None)]
            if isinstance(base, discord.TextChannel):
                variants += [dict(limit=None, private=True, joined=True),
                             dict(limit=None, private=True, joined=False)]
            for args in variants:
                try:
                    async for thread in base.archived_threads(**args):
                        channels[thread.id] = thread
                except discord.HTTPException as exc:
                    errors.append(f'{base.name}: archived thread listing failed ({exc.status})')
    scanned = found = 0
    last_update = time.monotonic()
    for channel in channels.values():
        if (excluded_channel(channel, config.excluded) or
                not await bot.store.is_scoped(ctx.guild.id, channel.id,
                                              getattr(channel, 'parent_id', None))):
            continue
        batch = []
        try:
            async for message in channel.history(limit=limit or None,
                                                    after=datetime.now(timezone.utc) - timedelta(days=365),
                                                    oldest_first=False):
                scanned += 1
                if not message.author.bot and contains_media(message, config.media_mode):
                    batch.append(record(message))
                    found += 1
                if len(batch) >= 100:
                    await bot.store.save_many(batch)
                    batch.clear()
                if time.monotonic() - last_update >= 10:
                    await status.edit(content=f'🍍 Scanned **{scanned}** messages; '
                                              f'**{found}** media candidates. Current: {channel.mention}')
                    last_update = time.monotonic()
        except discord.HTTPException as exc:
            errors.append(f'{channel.name}: history failed ({exc.status})')
        finally:
            await bot.store.save_many(batch)
    await status.edit(content=f'🍍 Scan finished: **{scanned}** messages checked, **{found}** '
                              f'media candidates indexed (existing records are not duplicated). '
                              f'**{len(errors)}** access/API issues. '
                              'Absent authors will be checked by the next membership reconciliation.')
    for error in errors:
        log.warning('Scan coverage: %s', error)
    if errors:
        await ctx.send('Some history could not be scanned:\n' + '\n'.join(errors[:8]) +
                       '\nSee console logs for the complete list.')


def main():
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
    token = os.getenv('DISCORD_TOKEN', '').strip()
    if not token or token == 'PUT_YOUR_BOT_TOKEN_HERE':
        raise RuntimeError('Set DISCORD_TOKEN in .env or your hosting environment variables')
    bot = Cleaner(Config.from_env())
    install_commands(bot)
    bot.run(token)


if __name__ == '__main__':
    main()
