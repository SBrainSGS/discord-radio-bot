"""Persistent guild preferences and radio-host features."""
import asyncio
import hashlib
import json
import logging
import math
import os
import shutil
import struct
import tempfile
import time
import wave
from collections import deque
from pathlib import Path

import discord
import edge_tts
from discord import app_commands

DEFAULTS = dict(volume=80, role_delay=25, interval_min=60, interval_max=300,
                pause=4, mute_enabled=True, mute_seconds=300, mute_cooldown=1800,
                mode="calm", voice="male", jingles=False, allowed_channels=[], roles={})
VOICES = {"male": "ru-RU-DmitryNeural", "female": "ru-RU-SvetlanaNeural"}
MODE_LINES = {
    "absurd": ("{a}, космический кабачок одобряет твоё присутствие в {channel}.",
               "В {channel} замечен {a}. Гравитация пока не возражает.",
               "{a}, министерство летающих тапочек передаёт привет.",
               "{a}, твоя заявка на превращение в радиоволну рассматривается."),
    "bold": ("{a}, не прячься, микрофон сам себя не включит.",
             "В {channel} снова {a}. Ну давай, удиви эфир.",
             "{a}, у нас тут радио, а не очередь за тишиной.",
             "{a}, легенда канала, твой выход."),
}


def atomic_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(name, path)
    finally:
        Path(name).unlink(missing_ok=True)


class Preferences:
    def __init__(self, path):
        self.path = path
        self.data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}

    def get(self, guild_id):
        return {**DEFAULTS, **self.data.get(str(guild_id), {})}

    def update(self, guild_id, **values):
        candidate = {**self.data, str(guild_id): {**self.get(guild_id), **values}}
        atomic_json(self.path, candidate)
        self.data = candidate


class CachedSpeech:
    def __init__(self, fallback, folder, ffmpeg):
        self.fallback, self.folder, self.ffmpeg = fallback, folder, ffmpeg
        folder.mkdir(parents=True, exist_ok=True)
        self.lock = asyncio.Lock()
        self.last_error = None

    async def synthesize(self, text, voice="male", jingle=False):
        async with self.lock:
            key = hashlib.sha256((voice + text).encode()).hexdigest()
            cached = self.folder / (key + ".mp3")
            if not cached.exists():
                fd, name = tempfile.mkstemp(suffix=".mp3")
                os.close(fd)
                temporary = Path(name)
                try:
                    if voice == "gtts":
                        temporary.unlink()
                        temporary = await self.fallback.synthesize(text)
                    else:
                        await asyncio.wait_for(edge_tts.Communicate(text, VOICES[voice]).save(str(temporary)), 35)
                    if temporary.stat().st_size == 0:
                        raise RuntimeError("TTS returned empty audio")
                    cache_fd, cache_name = tempfile.mkstemp(dir=self.folder, suffix=".tmp")
                    os.close(cache_fd)
                    try:
                        shutil.copyfile(temporary, cache_name)
                        os.replace(cache_name, cached)
                    finally:
                        Path(cache_name).unlink(missing_ok=True)
                    self.last_error = None
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.last_error = f"TTS: {type(exc).__name__}; резервный gTTS"
                    logging.warning(self.last_error)
                    temporary.unlink(missing_ok=True)
                    # Do not cache fallback under a male/female voice key.
                    return await self.fallback.synthesize(text)
                finally:
                    temporary.unlink(missing_ok=True)
            cached.touch()
            files = sorted(self.folder.glob("*.mp3"), key=lambda p: p.stat().st_mtime)
            total = sum(p.stat().st_size for p in files)
            while len(files) > 200 or total > 64 * 1024 * 1024:
                oldest = files.pop(0)
                total -= oldest.stat().st_size
                oldest.unlink(missing_ok=True)
            fd, name = tempfile.mkstemp(suffix=".mp3")
            os.close(fd)
            output = Path(name)
            shutil.copyfile(cached, output)
        if jingle:
            try:
                await self.prepend_jingle(output)
            except BaseException:
                output.unlink(missing_ok=True)
                raise
        return output

    async def prepend_jingle(self, output):
        tone = output.with_suffix(".wav")
        merged = output.with_suffix(".merged.mp3")
        try:
            with wave.open(str(tone), "wb") as f:
                f.setparams((1, 2, 24000, 0, "NONE", "not compressed"))
                frames = bytearray()
                for i in range(14400):
                    t = i / 24000
                    freq = (523, 659, 784)[min(2, int(t / .2))]
                    envelope = min(1, (t % .2) * 50, (.2 - t % .2) * 50)
                    frames.extend(struct.pack("<h", int(5000 * envelope * math.sin(2 * math.pi * freq * t))))
                f.writeframes(frames)
            proc = await asyncio.create_subprocess_exec(
                self.ffmpeg, "-v", "error", "-y", "-i", str(tone), "-i", str(output),
                "-filter_complex", "[0:a][1:a]concat=n=2:v=0:a=1", str(merged),
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
            try:
                await asyncio.wait_for(proc.wait(), 20)
            except BaseException:
                if proc.returncode is None:
                    proc.kill()
                await proc.wait()
                raise
            if proc.returncode:
                raise RuntimeError("Jingle encoding failed")
            os.replace(merged, output)
        finally:
            tone.unlink(missing_ok=True)
            merged.unlink(missing_ok=True)


class HostFeatures:
    def __init__(self, bot, core):
        self.bot, self.core = bot, core
        self.folder = bot.phrase_library.path.parent
        self.preferences = Preferences(self.folder / "settings.json")
        self.muted = {}
        self.departures = {}
        self.batches = {}
        self.tasks = set()
        self.last_backup = 0
        self.last_error = "Нет"
        self.recent_lines = {}
        self.register_commands()

    def settings(self, guild_id):
        return self.preferences.get(guild_id)

    def spawn(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.tasks.add(task)
        def done(t):
            self.tasks.discard(t)
            if not t.cancelled() and t.exception():
                self.last_error = type(t.exception()).__name__
                logging.error("Host task failed", exc_info=t.exception())
        task.add_done_callback(done)
        return task

    async def close(self):
        for t in list(self.tasks):
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    def allowed(self, guild_id, channel_id):
        ids = self.settings(guild_id)["allowed_channels"]
        return not ids or channel_id in ids

    def role_phrase(self, member, event):
        configured = self.settings(member.guild.id)["roles"]
        roles = sorted(member.roles, key=lambda r: r.position, reverse=True)
        for role in roles:
            values = configured.get(str(role.id), self.core.ROLE_DELAYED_ANNOUNCEMENTS.get(role.id))
            if values and values.get(event):
                text = values[event]
                state = self.bot.guild_states.get(member.guild.id)
                channel = state.voice_client.channel if state and state.voice_client else None
                return text.format(a=self.core.safe_display_name(member), channel=channel.name if channel else "канал")
        return None

    async def announce(self, state, member, channel, event, phrase):
        key = (state.guild.id, channel.id, event)
        context = ""
        now = time.monotonic()
        member_key = (state.guild.id, member.id)
        if event == "join":
            left = self.departures.pop(member_key, None)
            if left is not None and now - left >= 600:
                context = " С возвращением в эфир."
            elif len(self.core.get_human_members(channel)) == 1:
                context = " Первый слушатель на месте, открываем эфир."
        else:
            self.departures[member_key] = now
            if not self.core.get_human_members(channel):
                context = " Последний слушатель ушёл, эфир опустел."
        if key not in self.batches:
            self.batches[key] = []
            self.spawn(self.flush_batch(state, channel, event, key, state.generation))
        self.batches[key].append((self.core.safe_display_name(member), phrase + context))
        if len(self.departures) > 5000:
            self.departures = {k: v for k, v in self.departures.items() if now - v < 86400}

    def clear_guild(self, guild_id):
        # Generation checks make in-flight batch tasks harmless after a move.
        # Keep their keys until completion so an old task cannot erase a new batch.
        for key in list(self.muted):
            if key[0] == guild_id:
                self.muted.pop(key, None)

    async def flush_batch(self, state, channel, event, key, generation):
        try:
            await asyncio.sleep(3)
            batch = self.batches.pop(key, [])
            if not batch or state.generation != generation:
                return
            text = batch[0][1] if len(batch) == 1 else (
                self.core.join_names([n for n, _ in batch[:8]]) +
                (" вошли в эфир." if event == "join" else " покинули эфир."))
            await state.enqueue(self.core.SpeechRequest(text, event))
        finally:
            self.batches.pop(key, None)

    def style(self, text, settings):
        if settings["mode"] == "absurd":
            return random_prefix(("Говорит межгалактическая картошка. ", "В эфире бюро странностей. ")) + text
        if settings["mode"] == "bold":
            return random_prefix(("Так, внимание, народ. ", "Микрофон у меня, слушаем. ")) + text
        return text

    def radio_phrase(self, state, channel, humans):
        mode = self.settings(state.guild.id)["mode"]
        if mode == "calm":
            return self.core.build_radio_phrase(channel, humans, self.bot.phrase_library)
        recent = self.recent_lines.setdefault(state.guild.id, deque(maxlen=3))
        choices = [p for p in MODE_LINES[mode] if p not in recent] or list(MODE_LINES[mode])
        template = random_prefix(choices)
        recent.append(template)
        import random
        return template.format(a=self.core.safe_display_name(random.choice(humans)), channel=channel.name)

    def observe_voice(self, member, after):
        for key in list(self.muted):
            if key[0] == member.guild.id and key[2] == member.id:
                if not after.channel or after.channel.id != key[1] or not (after.self_mute or after.mute or after.self_deaf or after.deaf):
                    self.muted.pop(key, None)
        state = self.bot.guild_states.get(member.guild.id)
        if (not member.bot and after.channel and state and state.voice_client and state.voice_client.channel
                and after.channel.id == state.voice_client.channel.id
                and (after.self_mute or after.mute or after.self_deaf or after.deaf)):
            self.muted.setdefault((member.guild.id, after.channel.id, member.id), (time.monotonic(), -float("inf")))

    async def monitor(self):
        while True:
            try:
                await self.scan_mutes()
                if time.time() - self.last_backup > 86400:
                    self.backup()
                    self.last_backup = time.time()
            except Exception as exc:
                self.last_error = type(exc).__name__
                logging.exception("Host monitor failed")
            await asyncio.sleep(30)

    async def scan_mutes(self):
        now, present = time.monotonic(), set()
        for state in list(self.bot.guild_states.values()):
            client = state.voice_client
            if not client or not client.is_connected() or not client.channel:
                continue
            cfg = self.settings(state.guild.id)
            if not cfg["mute_enabled"]:
                continue
            for member in self.core.get_human_members(client.channel):
                v = member.voice
                if not v or not (v.self_mute or v.mute or v.self_deaf or v.deaf):
                    continue
                key = (state.guild.id, client.channel.id, member.id)
                present.add(key)
                start, last = self.muted.setdefault(key, (now, -float("inf")))
                if now - start >= cfg["mute_seconds"] and now - last >= cfg["mute_cooldown"]:
                    if state.queue.qsize() > 2:
                        continue
                    await state.enqueue(self.core.SpeechRequest(
                        f"{self.core.safe_display_name(member)}, микрофон выключен уже {int((now-start)//60)} минут. Мы тебя не слышим.", "mute"))
                    self.muted[key] = (start, now)
        self.muted = {k: v for k, v in self.muted.items() if k in present}

    def backup(self):
        folder = self.folder / "backups"
        folder.mkdir(exist_ok=True)
        stamp = time.strftime("%Y%m%d-%H%M%S")
        for path in (self.bot.phrase_library.path, self.preferences.path):
            if path.exists():
                shutil.copyfile(path, folder / (stamp + "-" + path.name))
        for path in sorted(folder.iterdir(), reverse=True)[14:]:
            path.unlink()

    def register_commands(self):
        tree, core = self.bot.tree, self.core

        @tree.command(name="settings", description="Настройки ведущего; без параметров показывает текущие")
        @app_commands.guild_only()
        @app_commands.default_permissions(manage_guild=True)
        @app_commands.checks.has_permissions(manage_guild=True)
        @app_commands.choices(mode=[app_commands.Choice(name=n, value=v) for n, v in [("Спокойный", "calm"), ("Абсурдный", "absurd"), ("Дерзкий", "bold")]], voice=[app_commands.Choice(name=n, value=v) for n, v in [("Дмитрий", "male"), ("Светлана", "female"), ("Старый gTTS", "gtts")]])
        async def settings(i: discord.Interaction, volume: app_commands.Range[int, 0, 150] | None = None,
                           role_delay: app_commands.Range[int, 0, 300] | None = None,
                           interval_min: app_commands.Range[int, 5, 900] | None = None,
                           interval_max: app_commands.Range[int, 5, 900] | None = None,
                           pause: app_commands.Range[int, 0, 30] | None = None,
                           mute_enabled: bool | None = None,
                           mute_seconds: app_commands.Range[int, 60, 3600] | None = None,
                           mode: str | None = None, voice: str | None = None, jingles: bool | None = None):
            values = {k: v for k, v in locals().items() if k in DEFAULTS and v is not None}
            cfg = {**self.settings(i.guild_id), **values}
            if cfg["interval_min"] > cfg["interval_max"]:
                await i.response.send_message("Минимальный интервал больше максимального.", ephemeral=True)
                return
            self.preferences.update(i.guild_id, **values)
            state = self.bot.guild_states.get(i.guild_id)
            if state:
                state.radio_interval_min, state.radio_interval_max = cfg["interval_min"], cfg["interval_max"]
                if ("interval_min" in values or "interval_max" in values) and state.radio_task and not state.radio_task.done():
                    await state.stop_radio()
                    await state.start_radio(cfg["interval_min"], cfg["interval_max"])
            await i.response.send_message("```json\n" + json.dumps({k: v for k, v in cfg.items() if k != "roles"}, ensure_ascii=False, indent=2) + "\n```", ephemeral=True)

        @tree.command(name="channel_access", description="Разрешить канал; reset снимает все ограничения")
        @app_commands.guild_only()
        @app_commands.checks.has_permissions(manage_guild=True)
        async def access(i: discord.Interaction, channel: discord.VoiceChannel | None = None, remove: bool = False, reset: bool = False):
            ids = list(self.settings(i.guild_id)["allowed_channels"])
            if reset:
                ids = []
            elif channel:
                if remove:
                    ids = [n for n in ids if n != channel.id]
                elif channel.id not in ids:
                    ids.append(channel.id)
            self.preferences.update(i.guild_id, allowed_channels=ids)
            state = self.bot.guild_states.get(i.guild_id)
            if state and state.voice_client and state.voice_client.channel and ids and state.voice_client.channel.id not in ids:
                await state.leave()
            await i.response.send_message("Разрешены: " + (", ".join(f"<#{n}>" for n in ids) or "все каналы"), ephemeral=True)

        @tree.command(name="role_phrase", description="Настроить роль: фразы join/leave, remove удаляет настройку")
        @app_commands.guild_only()
        @app_commands.checks.has_permissions(manage_guild=True)
        async def role_phrase(i: discord.Interaction, role: discord.Role, join: str = "", leave: str = "", remove: bool = False):
            roles = dict(self.settings(i.guild_id)["roles"])
            if remove:
                roles[str(role.id)] = {}
            elif join or leave:
                for text in (join, leave):
                    if text:
                        if len(text) > 500:
                            await i.response.send_message("До 500 символов на фразу.", ephemeral=True)
                            return
                        try:
                            core.validate_phrase("SOLO_TEMPLATES", text)
                        except ValueError as exc:
                            await i.response.send_message(str(exc), ephemeral=True)
                            return
                roles[str(role.id)] = {"join": join, "leave": leave}
            self.preferences.update(i.guild_id, roles=roles)
            await i.response.send_message(str(roles.get(str(role.id), core.ROLE_DELAYED_ANNOUNCEMENTS.get(role.id, {})))[:1800], ephemeral=True)

        @tree.command(name="phrases", description="Показать фразы категории с номерами")
        @app_commands.guild_only()
        @app_commands.choices(category=core.PHRASE_CATEGORY_CHOICES)
        async def phrases(i: discord.Interaction, category: str, page: app_commands.Range[int, 1, 1000] = 1):
            values = self.bot.phrase_library.get_section(category)
            start = (page - 1) * 5
            text = "\n".join(f"{n+1}. {p[:300]}" for n, p in enumerate(values) if start <= n < start + 5)
            await i.response.send_message(text or "Страница пуста.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())

        @tree.command(name="remove_phrase", description="Удалить фразу по номеру из /phrases")
        @app_commands.guild_only()
        @app_commands.checks.has_permissions(manage_guild=True)
        @app_commands.choices(category=core.PHRASE_CATEGORY_CHOICES)
        async def remove_phrase(i: discord.Interaction, category: str, number: app_commands.Range[int, 1, 10000], expected: str):
            try:
                core.remove_phrase(self.bot.phrase_library.path, category, number, expected)
            except ValueError as exc:
                await i.response.send_message(str(exc), ephemeral=True)
                return
            self.bot.phrase_library.reload_if_changed(force=True)
            await i.response.send_message("Удалено; предыдущее содержимое сохранено в .bak.", ephemeral=True)

        @tree.command(name="test_phrase", description="Проверить шаблон и послушать его в своём канале")
        @app_commands.guild_only()
        @app_commands.checks.has_permissions(manage_guild=True)
        @app_commands.choices(category=core.PHRASE_CATEGORY_CHOICES)
        async def test_phrase(i: discord.Interaction, category: str, text: str):
            try:
                core.validate_phrase(category, text)
                if len(text) > 500:
                    raise ValueError("До 500 символов")
            except ValueError as exc:
                await i.response.send_message(str(exc), ephemeral=True)
                return
            result = await self.bot.ensure_voice_state(i)
            if not result:
                await i.response.send_message("Сначала зайди в голосовой канал.", ephemeral=True)
                return
            await i.response.defer(ephemeral=True)
            state, channel = result
            await state.ensure_connected(channel)
            rendered = text.format(a=core.safe_display_name(i.user), b="Другой слушатель", c="Третий слушатель", group="Слушатели", channel=channel.name)
            await state.enqueue(core.SpeechRequest(rendered, "test"))
            await i.followup.send(rendered, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())

        @tree.command(name="status", description="Подключение, очередь, голос и последняя ошибка")
        @app_commands.guild_only()
        async def status(i: discord.Interaction):
            state = self.bot.guild_states.get(i.guild_id)
            client = state.voice_client if state else None
            cfg = self.settings(i.guild_id)
            error = self.bot.synthesizer.last_error or (state.last_error if state else None) or self.last_error
            await i.response.send_message(f"Канал: {client.channel.name if client and client.channel else 'не подключён'}\nОчередь: {state.queue.qsize() if state else 0}\nРадио: {bool(state and state.radio_task and not state.radio_task.done())}\nГолос: {cfg['voice']}; режим: {cfg['mode']}\nПоследняя ошибка: {error}", ephemeral=True)

        @tree.command(name="backup", description="Сделать резервную копию библиотеки и настроек")
        @app_commands.guild_only()
        @app_commands.checks.has_permissions(manage_guild=True)
        async def backup(i: discord.Interaction):
            self.backup()
            await i.response.send_message("Копия сохранена в data/backups (последние 7 пар копий).", ephemeral=True)


def random_prefix(values):
    import random
    return random.choice(values)
