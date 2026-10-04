import asyncio
import logging
import os
import random
import re
import tempfile
import shutil
import time
from collections import deque
from string import Formatter
from dataclasses import dataclass
from pathlib import Path

import discord
import imageio_ffmpeg
from discord import app_commands
from gtts import gTTS
from radio_features import CachedSpeech, HostFeatures

BASE_DIR = Path(__file__).resolve().parent
PHRASE_LIBRARY_PATH = Path(os.getenv("PHRASE_LIBRARY_PATH", str(BASE_DIR / "radio_phrases.txt")))

DISCORD_BOT_TOKEN_ENV = "DISCORD_BOT_TOKEN"
DISCORD_GUILD_ID_ENV = "DISCORD_GUILD_ID"

DISCORD_BOT_TOKEN = os.getenv(DISCORD_BOT_TOKEN_ENV, "").strip()
DISCORD_GUILD_ID_RAW = os.getenv(DISCORD_GUILD_ID_ENV, "").strip()
DISCORD_GUILD_ID = int(DISCORD_GUILD_ID_RAW) if DISCORD_GUILD_ID_RAW else None

GTTS_LANGUAGE = "ru"
GTTS_TLD = "com"

DEFAULT_RADIO_INTERVAL_MIN = 5
DEFAULT_RADIO_INTERVAL_MAX = 900
MIN_RADIO_INTERVAL = 5
MAX_RADIO_INTERVAL = 900
MAX_SAY_LENGTH = 500
MAX_QUEUE_SIZE = 25
EMPTY_CHANNEL_CHECK_INTERVAL_SECONDS = 300
ROLE_ANNOUNCEMENT_DELAY_SECONDS = 25
PLAYBACK_TIMEOUT_SECONDS = 120

REQUIRED_PHRASE_SECTION_NAMES = (
    "SOLO_TEMPLATES",
    "DUO_TEMPLATES",
    "GROUP_TEMPLATES",
    "RADIO_START_LINES",
    "JOIN_ANNOUNCEMENTS",
    "LEAVE_ANNOUNCEMENTS",
)

PHRASE_SECTION_NAMES = REQUIRED_PHRASE_SECTION_NAMES

CATEGORY_VARIABLES = {
    "SOLO_TEMPLATES": ("a", "channel"),
    "DUO_TEMPLATES": ("a", "b", "channel"),
    "GROUP_TEMPLATES": ("a", "b", "c", "group", "channel"),
    "RADIO_START_LINES": tuple(),
    "JOIN_ANNOUNCEMENTS": ("a", "b", "channel"),
    "LEAVE_ANNOUNCEMENTS": ("a", "b", "channel"),
}

NAME_SANITIZER = re.compile(r"[^0-9A-Za-zА-Яа-яЁё _.-]+")

ROLE_DELAYED_ANNOUNCEMENTS = {
    1426625952544194690: {
        "join": "ВНИМАНИЕ! Данил Коробкин сосёт пенисы!",
        "leave": "И вот он похоже их дососал!",
    },
    778686479312486420: {
        "join": "...!",
        "leave": "Досвидос, гитлер",
    },
    1319787160613687367: {
        "join": "Женский персонаж",
        "leave": "Женский персонаж покинул чат",
    },
}


@dataclass(slots=True)
class SpeechRequest:
    text: str
    author_name: str
    is_radio: bool = False
    channel_id: int | None = None
    generation: int = 0


class PhraseLibrary:
    def __init__(self, path: Path) -> None:
        self.path = path
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(BASE_DIR / "radio_phrases.example.txt", path)
        self._sections = {name: tuple() for name in PHRASE_SECTION_NAMES}
        self._mtime_ns: int | None = None
        self.recent = {}
        self.reload_if_changed(force=True, required=True)

    def pick(self, section_name: str, has_other_human: bool = True) -> str:
        templates = self.get_section(section_name)
        if not has_other_human:
            templates = tuple(t for t in templates if "{b}" not in t) or templates
        recent = self.recent.setdefault(section_name, deque(maxlen=5))
        available = [t for t in templates if t not in recent]
        if not available:
            available = [t for t in templates if not recent or t != recent[-1]] or list(templates)
        chosen = random.choice(available)
        recent.append(chosen)
        return chosen

    def reload_if_changed(self, force: bool = False, required: bool = False) -> None:
        try:
            stat = self.path.stat()
        except FileNotFoundError as exc:
            if required:
                raise RuntimeError(
                    f"Не найден файл фраз {self.path.name}. Верни его рядом с discord_radio_bot.py."
                ) from exc
            logging.exception("Не найден файл фраз %s. Оставляю предыдущие фразы.", self.path)
            return

        if not force and self._mtime_ns == stat.st_mtime_ns:
            return

        try:
            sections = self._parse_file(self.path)
        except Exception as exc:
            if required:
                raise RuntimeError(f"Не удалось загрузить фразы из {self.path.name}.") from exc
            logging.exception("Не удалось перечитать %s. Оставляю предыдущие фразы.", self.path)
            return

        self._sections = sections
        self._mtime_ns = stat.st_mtime_ns
        logging.info("Фразы бота обновлены из %s", self.path.name)

    def get_section(self, section_name: str) -> tuple[str, ...]:
        self.reload_if_changed()
        values = self._sections[section_name]
        if not values:
            raise RuntimeError(f"Секция {section_name} не загружена из {self.path.name}.")
        return values

    @staticmethod
    def _parse_file(path: Path) -> dict[str, tuple[str, ...]]:
        parsed: dict[str, list[str]] = {name: [] for name in PHRASE_SECTION_NAMES}
        current_section: str | None = None

        for line_number, raw_line in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), start=1):
            line = raw_line.strip()
            if not line or line.startswith("#") or line.startswith(";"):
                continue

            if line.startswith("[") and line.endswith("]"):
                section_name = line[1:-1].strip()
                if section_name not in parsed:
                    raise ValueError(f"Неизвестная секция {section_name!r} в строке {line_number}")
                current_section = section_name
                continue

            if current_section is None:
                raise ValueError(f"Фраза вне секции в строке {line_number}")

            validate_phrase(current_section, line)
            parsed[current_section].append(line)

        missing_sections = [name for name in REQUIRED_PHRASE_SECTION_NAMES if not parsed[name]]
        if missing_sections:
            joined = ", ".join(missing_sections)
            raise ValueError(f"В файле фраз пустые секции: {joined}")

        return {name: tuple(phrases) for name, phrases in parsed.items()}


class GTTSSpeechSynthesizer:
    def __init__(self) -> None:
        self.temp_dir = Path(tempfile.gettempdir()) / "discord_radio_bot_tts"
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    async def synthesize(self, text: str) -> Path:
        task = asyncio.create_task(asyncio.to_thread(self._synthesize, text))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            try:
                path = await task
                path.unlink(missing_ok=True)
            except Exception:
                pass
            raise

    def _synthesize(self, text: str) -> Path:
        handle, raw_path = tempfile.mkstemp(prefix="tts_gtts_", suffix=".mp3", dir=self.temp_dir)
        os.close(handle)
        output_path = Path(raw_path)
        try:
            gTTS(text=text, lang=GTTS_LANGUAGE, tld=GTTS_TLD, slow=False, timeout=(5, 20)).save(str(output_path))
        except BaseException:
            output_path.unlink(missing_ok=True)
            raise
        return output_path


def normalize_user_text(text: str) -> str:
    return " ".join(text.split()).strip()


def validate_phrase(category: str, phrase: str) -> None:
    if not phrase.strip() or phrase.strip().startswith(("[", "#", ";")) or "\n" in phrase or "\r" in phrase:
        raise ValueError("Phrase must be a non-empty template on one line")
    allowed = CATEGORY_VARIABLES[category]
    for _, field, spec, conversion in Formatter().parse(phrase):
        if field is not None and (field not in allowed or spec or conversion):
            raise ValueError(f"Invalid template field: {field!r}. Allowed: {allowed}")


def insert_phrase(path: Path, category: str, phrase: str) -> None:
    validate_phrase(category, phrase)
    lines = path.read_text(encoding="utf-8-sig").splitlines()
    section_header = f"[{category}]"

    start_index: int | None = None
    end_index = len(lines)

    for index, raw_line in enumerate(lines):
        if raw_line.strip() == section_header:
            start_index = index
            continue

        if start_index is not None:
            stripped = raw_line.strip()
            if stripped.startswith("[") and stripped.endswith("]"):
                end_index = index
                break

    if start_index is None:
        if lines and lines[-1].strip():
            lines.append("")
        lines.append(section_header)
        lines.append(phrase)
    else:
        insert_at = end_index
        while insert_at > start_index + 1 and not lines[insert_at - 1].strip():
            insert_at -= 1
        lines.insert(insert_at, phrase)

    shutil.copyfile(path, path.with_suffix(path.suffix + ".bak"))
    handle, raw_path = tempfile.mkstemp(dir=path.parent, prefix="phrases-", suffix=".tmp")
    temporary = Path(raw_path)
    try:
        with os.fdopen(handle, "w", encoding="utf-8-sig") as stream:
            stream.write("\n".join(lines) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def format_template_variables(category: str) -> str:
    variables = CATEGORY_VARIABLES.get(category, ())
    if not variables:
        return "(без переменных)"
    return ", ".join(f"{{{variable}}}" for variable in variables)


def remove_phrase(path: Path, category: str, number: int, expected: str) -> None:
    parsed = PhraseLibrary._parse_file(path)
    values = parsed[category]
    if number < 1 or number > len(values):
        raise ValueError("Нет фразы с таким номером.")
    if values[number - 1] != expected:
        raise ValueError("Фраза изменилась: скопируй полный текст из /phrases в expected.")
    if len(values) <= 1:
        raise ValueError("Нельзя удалить последнюю фразу обязательной категории.")
    lines = path.read_text(encoding="utf-8-sig").splitlines()
    current, count = None, 0
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            current = stripped[1:-1]
        elif current == category and stripped and not stripped.startswith(("#", ";")):
            count += 1
            if count == number:
                del lines[index]
                break
    shutil.copyfile(path, path.with_suffix(path.suffix + ".bak"))
    fd, raw = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8-sig") as stream:
            stream.write("\n".join(lines) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(raw, path)
    finally:
        Path(raw).unlink(missing_ok=True)


PHRASE_CATEGORY_CHOICES = [
    app_commands.Choice(
        name=f"{category} (Доступные переменные {format_template_variables(category)})",
        value=category,
    )
    for category in PHRASE_SECTION_NAMES
]


def build_phrase_help_text() -> str:
    lines = ["Доступные категории фраз:"]
    for category in PHRASE_SECTION_NAMES:
        lines.append(f"- `{category}`: {format_template_variables(category)}")
    return "\n".join(lines)


def safe_display_name(member: discord.Member) -> str:
    cleaned = NAME_SANITIZER.sub(" ", member.display_name)
    cleaned = " ".join(cleaned.split())
    return cleaned or "неустановленный гражданин"


def join_names(names: list[str]) -> str:
    if len(names) == 1:
        return names[0]
    if len(names) == 2:
        return f"{names[0]} и {names[1]}"
    return ", ".join(names[:-1]) + f" и {names[-1]}"


def get_human_members(
    channel: discord.VoiceChannel | discord.StageChannel,
    *,
    exclude_member_id: int | None = None,
) -> list[discord.Member]:
    humans: list[discord.Member] = []
    for member in channel.members:
        if member.bot:
            continue
        if exclude_member_id is not None and member.id == exclude_member_id:
            continue
        humans.append(member)
    return humans


def pick_announcement_template(
    section_name: str,
    phrase_library: PhraseLibrary,
    has_other_human: bool,
) -> str:
    if isinstance(phrase_library, PhraseLibrary):
        return phrase_library.pick(section_name, has_other_human)
    templates = list(phrase_library.get_section(section_name))
    if has_other_human:
        return random.choice(templates)

    templates_without_b = [template for template in templates if "{b}" not in template]
    return random.choice(templates_without_b or templates)


def pick_other_human_name(
    channel: discord.VoiceChannel | discord.StageChannel,
    member: discord.Member,
) -> str | None:
    other_humans = [safe_display_name(other_member) for other_member in get_human_members(channel, exclude_member_id=member.id)]
    if not other_humans:
        return None
    return random.choice(other_humans)


def resolve_member_delayed_announcement(member: discord.Member, event_type: str) -> str | None:
    matched_configs: list[tuple[int, dict[str, str]]] = []
    for role in member.roles:
        config = ROLE_DELAYED_ANNOUNCEMENTS.get(role.id)
        if config:
            matched_configs.append((role.position, config))

    if not matched_configs:
        return None

    matched_configs.sort(key=lambda item: item[0], reverse=True)
    text = matched_configs[0][1].get(event_type, "").strip()
    return text or None


def build_radio_phrase(
    channel: discord.VoiceChannel | discord.StageChannel,
    humans: list[discord.Member],
    phrase_library: PhraseLibrary,
) -> str:
    names = [safe_display_name(member) for member in humans]
    channel_name = NAME_SANITIZER.sub(" ", channel.name).strip() or "секретный канал"

    available_template_types = ["solo"]
    if len(names) >= 2:
        available_template_types.append("duo")
    if len(names) >= 3:
        available_template_types.append("group")

    template_type = random.choice(available_template_types)

    if template_type == "solo":
        template = phrase_library.pick("SOLO_TEMPLATES")
        return template.format(a=random.choice(names), channel=channel_name)

    if template_type == "duo":
        first, second = random.sample(names, 2)
        template = phrase_library.pick("DUO_TEMPLATES")
        return template.format(a=first, b=second, channel=channel_name)

    chosen = random.sample(names, k=min(3, len(names)))
    template = phrase_library.pick("GROUP_TEMPLATES")
    return template.format(
        group=join_names(chosen),
        a=chosen[0],
        b=chosen[1] if len(chosen) > 1 else chosen[0],
        c=chosen[2] if len(chosen) > 2 else chosen[-1],
        channel=channel_name,
    )


def build_join_announcement(
    member: discord.Member,
    channel: discord.VoiceChannel | discord.StageChannel,
    phrase_library: PhraseLibrary,
) -> str:
    other_human_name = pick_other_human_name(channel, member)
    template = pick_announcement_template("JOIN_ANNOUNCEMENTS", phrase_library, other_human_name is not None)
    member_name = safe_display_name(member)
    channel_name = NAME_SANITIZER.sub(" ", channel.name).strip() or "секретный канал"
    return template.format(a=member_name, b=other_human_name or member_name, channel=channel_name)


def build_leave_announcement(
    member: discord.Member,
    channel: discord.VoiceChannel | discord.StageChannel,
    phrase_library: PhraseLibrary,
) -> str:
    other_human_name = pick_other_human_name(channel, member)
    template = pick_announcement_template("LEAVE_ANNOUNCEMENTS", phrase_library, other_human_name is not None)
    member_name = safe_display_name(member)
    channel_name = NAME_SANITIZER.sub(" ", channel.name).strip() or "секретный канал"
    return template.format(a=member_name, b=other_human_name or member_name, channel=channel_name)


class GuildAudioState:
    def __init__(self, bot: "RadioAnnouncerBot", guild: discord.Guild) -> None:
        self.bot = bot
        self.guild = guild
        self.queue: asyncio.Queue[SpeechRequest] = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        self.worker_task = asyncio.create_task(self.player_loop(), name=f"speech-worker-{guild.id}")
        self.radio_task: asyncio.Task[None] | None = None
        self.radio_interval_min = DEFAULT_RADIO_INTERVAL_MIN
        self.radio_interval_max = DEFAULT_RADIO_INTERVAL_MAX
        self.connection_lock = asyncio.Lock()
        self.generation = 0
        self.last_error = None
        self.last_finished = 0
        self.recent_requests = deque(maxlen=8)
        if hasattr(bot, "features"):
            cfg = bot.features.settings(guild.id)
            self.radio_interval_min, self.radio_interval_max = cfg["interval_min"], cfg["interval_max"]

    @property
    def voice_client(self) -> discord.VoiceClient | None:
        return self.guild.voice_client

    async def ensure_connected(self, channel: discord.VoiceChannel | discord.StageChannel) -> discord.VoiceClient:
        async with self.connection_lock:
            return await self._ensure_connected(channel)

    async def _ensure_connected(self, channel: discord.VoiceChannel | discord.StageChannel) -> discord.VoiceClient:
        if hasattr(self.bot, "features") and not self.bot.features.allowed(self.guild.id, channel.id):
            raise app_commands.CheckFailure("Этот канал не разрешён в /channel_access")
        current = self.voice_client
        if current and current.is_connected():
            if current.channel and current.channel.id != channel.id:
                self.generation += 1
                await self.clear_queue()
                self.bot.cancel_guild_announcements(self.guild.id)
                if current.is_playing():
                    current.stop()
                await current.move_to(channel)
            return current
        self.generation += 1
        return await channel.connect(self_deaf=True)

    async def enqueue(self, request: SpeechRequest) -> int:
        client = self.voice_client
        if not client or not client.is_connected() or not client.channel:
            raise RuntimeError("Bot is not connected to voice")
        request.channel_id = client.channel.id
        request.generation = self.generation
        if self.queue.full():
            raise asyncio.QueueFull
        if request.author_name not in ("test", "say"):
            now = time.monotonic()
            if any(text == request.text and now - stamp < 120 for text, stamp in self.recent_requests):
                return self.queue.qsize()
            self.recent_requests.append((request.text, now))
        self.queue.put_nowait(request)
        position = self.queue.qsize()
        if self.voice_client and self.voice_client.is_playing():
            position += 1
        return position

    async def clear_queue(self) -> int:
        cleared = 0
        while True:
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            else:
                self.queue.task_done()
                cleared += 1
        return cleared

    async def stop_radio(self) -> bool:
        if not self.radio_task:
            return False
        self.radio_task.cancel()
        try:
            await self.radio_task
        except asyncio.CancelledError:
            pass
        except Exception:
            logging.exception("Radio task failed in guild=%s", self.guild.id)
        self.radio_task = None
        return True

    async def start_radio(self, min_interval_seconds: int, max_interval_seconds: int) -> bool:
        self.radio_interval_min = min_interval_seconds
        self.radio_interval_max = max_interval_seconds
        if self.radio_task and not self.radio_task.done():
            return False
        self.radio_task = asyncio.create_task(self.radio_loop(), name=f"radio-loop-{self.guild.id}")
        return True

    async def leave(self) -> tuple[bool, int]:
        async with self.connection_lock:
            return await self._leave()

    async def _leave(self) -> tuple[bool, int]:
        self.generation += 1
        self.bot.cancel_guild_announcements(self.guild.id)
        await self.stop_radio()
        cleared = await self.clear_queue()
        client = self.voice_client
        if client and client.is_connected():
            if client.is_playing():
                client.stop()
            await client.disconnect(force=True)
            return True, cleared
        return False, cleared

    async def shutdown(self) -> None:
        await self.leave()
        self.worker_task.cancel()
        try:
            await self.worker_task
        except asyncio.CancelledError:
            pass

    async def player_loop(self) -> None:
        while True:
            request = await self.queue.get()
            audio_path: Path | None = None
            source = None
            try:
                client = self.voice_client
                if not self.request_is_current(request, client):
                    continue

                cfg = self.bot.features.settings(self.guild.id) if hasattr(self.bot, "features") else None
                if cfg:
                    await asyncio.sleep(max(0, cfg["pause"] - (time.monotonic() - self.last_finished)))
                    if not self.request_is_current(request, self.voice_client):
                        continue
                    text = self.bot.features.style(request.text, cfg) if request.author_name not in ("say", "test") else request.text
                    audio_path = await self.bot.synthesizer.synthesize(text, voice=cfg["voice"], jingle=cfg["jingles"] and request.is_radio)
                else:
                    audio_path = await self.bot.synthesizer.synthesize(request.text)
                if not self.request_is_current(request, self.voice_client):
                    continue
                source = discord.FFmpegOpusAudio(
                    source=str(audio_path),
                    executable=self.bot.ffmpeg_path,
                    bitrate=96,
                    options=f"-af volume={cfg['volume'] / 100}" if cfg else None,
                )

                finished = self.bot.main_loop.create_future()

                def after_playback(error: Exception | None) -> None:
                    def finish() -> None:
                        if not finished.done():
                            finished.set_result(error)
                    self.bot.main_loop.call_soon_threadsafe(finish)

                client.play(source, after=after_playback)
                maybe_error = await asyncio.wait_for(finished, PLAYBACK_TIMEOUT_SECONDS)
                if maybe_error:
                    raise maybe_error
            except asyncio.TimeoutError:
                client.stop()
                self.last_error = "Playback timeout"
                logging.warning("Playback timed out in guild=%s", self.guild.id)
            except Exception as exc:
                self.last_error = type(exc).__name__
                logging.exception("Ошибка воспроизведения в guild=%s", self.guild.id)
            finally:
                if source:
                    source.cleanup()
                if audio_path:
                    audio_path.unlink(missing_ok=True)
                self.last_finished = time.monotonic()
                self.queue.task_done()

    def request_is_current(self, request: SpeechRequest, client: discord.VoiceClient | None) -> bool:
        return bool(client and client.is_connected() and client.channel
                    and client.channel.id == request.channel_id and self.generation == request.generation)

    async def radio_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(random.randint(self.radio_interval_min, self.radio_interval_max))
                client = self.voice_client
                if not client or not client.is_connected() or not client.channel:
                    continue
                if client.is_playing() or self.queue.qsize() > 2:
                    continue

                humans = get_human_members(client.channel)
                if not humans:
                    continue

                try:
                    phrase = self.bot.features.radio_phrase(self, client.channel, humans) if hasattr(self.bot, "features") else build_radio_phrase(client.channel, humans, self.bot.phrase_library)
                    await self.enqueue(SpeechRequest(text=phrase, author_name="radio", is_radio=True))
                except Exception:
                    logging.exception("Radio iteration failed in guild=%s", self.guild.id)
        except asyncio.CancelledError:
            raise


class RadioAnnouncerBot(discord.Client):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        intents.guilds = True
        intents.voice_states = True
        intents.members = True

        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()
        self.phrase_library = PhraseLibrary(PHRASE_LIBRARY_PATH)
        self.synthesizer = GTTSSpeechSynthesizer()
        self.synthesizer = CachedSpeech(self.synthesizer, PHRASE_LIBRARY_PATH.parent / "tts-cache", self.ffmpeg_path)
        self.guild_states: dict[int, GuildAudioState] = {}
        self.empty_channel_monitor_task: asyncio.Task[None] | None = None
        self.delayed_announcement_tasks: set[asyncio.Task[None]] = set()
        self.pending_announcements: dict[tuple[int, int], asyncio.Task[None]] = {}
        self.health_path = Path(tempfile.gettempdir()) / "discord-radio-ready"
        self.main_loop: asyncio.AbstractEventLoop | None = None
        self.register_commands()
        import sys
        self.features = HostFeatures(self, sys.modules[__name__])

    async def setup_hook(self) -> None:
        self.main_loop = asyncio.get_running_loop()
        self.empty_channel_monitor_task = asyncio.create_task(
            self.empty_channel_monitor_loop(),
            name="empty-channel-monitor",
        )
        self.features.spawn(self.features.monitor())
        logging.info("TTS: Edge Dmitry by default, gTTS fallback")
        logging.info("Phrase hot reload is enabled for %s", self.phrase_library.path.name)

        guild_id = DISCORD_GUILD_ID
        if guild_id:
            test_guild = discord.Object(id=guild_id)
            self.tree.copy_global_to(guild=test_guild)
            await self.tree.sync(guild=test_guild)
            logging.info("Slash-команды синхронизированы для guild %s", guild_id)
        else:
            await self.tree.sync()
            logging.info("Slash-команды синхронизированы глобально")

    async def on_ready(self) -> None:
        self.health_path.touch()
        logging.info("READY: %s (%s), guilds=%s", self.user, self.user.id if self.user else "?", len(self.guilds))

    def schedule_delayed_role_announcement(
        self,
        *,
        guild_id: int,
        channel_id: int,
        member_id: int,
        event_type: str,
        text: str,
    ) -> None:
        task = asyncio.create_task(
            self.delayed_role_announcement(
                guild_id=guild_id,
                channel_id=channel_id,
                member_id=member_id,
                event_type=event_type,
                text=text,
            ),
            name=f"role-announcement-{event_type}-{guild_id}-{member_id}",
        )
        self.delayed_announcement_tasks.add(task)
        task.add_done_callback(self.delayed_announcement_tasks.discard)
        key = (guild_id, member_id)
        old = self.pending_announcements.get(key)
        if old:
            old.cancel()
        self.pending_announcements[key] = task
        def clear_pending(done: asyncio.Task[None]) -> None:
            if self.pending_announcements.get(key) is done:
                self.pending_announcements.pop(key, None)
            if not done.cancelled() and done.exception():
                logging.error("Delayed announcement failed", exc_info=done.exception())
        task.add_done_callback(clear_pending)

    def cancel_guild_announcements(self, guild_id: int) -> None:
        if hasattr(self, "features"):
            self.features.clear_guild(guild_id)
        for key, task in list(self.pending_announcements.items()):
            if key[0] == guild_id:
                task.cancel()
                self.pending_announcements.pop(key, None)

    async def delayed_role_announcement(
        self,
        *,
        guild_id: int,
        channel_id: int,
        member_id: int,
        event_type: str,
        text: str,
    ) -> None:
        delay = self.features.settings(guild_id)["role_delay"] if hasattr(self, "features") else ROLE_ANNOUNCEMENT_DELAY_SECONDS
        await asyncio.sleep(delay)

        state = self.guild_states.get(guild_id)
        if state is None:
            return

        client = state.voice_client
        if not client or not client.is_connected() or not client.channel or client.channel.id != channel_id:
            return

        member = state.guild.get_member(member_id)
        if event_type == "join":
            if member is None or member.voice is None or member.voice.channel is None or member.voice.channel.id != channel_id:
                return
        elif event_type == "leave":
            if member is not None and member.voice is not None and member.voice.channel is not None and member.voice.channel.id == channel_id:
                return

        try:
            await state.enqueue(SpeechRequest(text=text, author_name=f"role-{event_type}", is_radio=False))
        except asyncio.QueueFull:
            logging.warning(
                "Очередь переполнена, пропускаю delayed role announcement для %s в guild=%s",
                member_id,
                guild_id,
            )
        except Exception:
            logging.exception(
                "Не удалось озвучить delayed role announcement для %s в guild=%s",
                member_id,
                guild_id,
            )

    async def empty_channel_monitor_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(EMPTY_CHANNEL_CHECK_INTERVAL_SECONDS)
                if self.is_ready():
                    self.health_path.touch()
                else:
                    self.health_path.unlink(missing_ok=True)
                for state in list(self.guild_states.values()):
                    client = state.voice_client
                    if not client or not client.is_connected() or not client.channel:
                        continue
                    if get_human_members(client.channel):
                        continue

                    logging.info(
                        "В канале %s не осталось людей, отключаюсь в guild=%s",
                        client.channel.id,
                        state.guild.id,
                    )
                    try:
                        await state.leave()
                    except Exception:
                        logging.exception("Auto-leave failed in guild=%s", state.guild.id)
        except asyncio.CancelledError:
            raise

    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ) -> None:
        if hasattr(self, "features") and member.guild is not None:
            self.features.observe_voice(member, after)
        if before.channel == after.channel or member.guild is None:
            return

        if member.bot:
            if self.user and member.id == self.user.id:
                state = self.guild_states.get(member.guild.id)
                if state:
                    state.generation += 1
                    self.cancel_guild_announcements(member.guild.id)
                    await state.clear_queue()
                    if state.voice_client and state.voice_client.is_playing():
                        state.voice_client.stop()
            return

        state = self.guild_states.get(member.guild.id)
        if state is None:
            return

        pending = self.pending_announcements.pop((member.guild.id, member.id), None)
        if pending:
            pending.cancel()

        client = state.voice_client
        if not client or not client.is_connected() or not client.channel:
            return

        joined_tracked_channel = after.channel is not None and after.channel.id == client.channel.id
        left_tracked_channel = before.channel is not None and before.channel.id == client.channel.id

        if not joined_tracked_channel and not left_tracked_channel:
            return

        try:
            if joined_tracked_channel and after.channel is not None:
                delayed_text = self.features.role_phrase(member, "join") if hasattr(self, "features") else resolve_member_delayed_announcement(member, "join")
                if delayed_text:
                    self.schedule_delayed_role_announcement(
                        guild_id=member.guild.id,
                        channel_id=after.channel.id,
                        member_id=member.id,
                        event_type="join",
                        text=delayed_text,
                    )
                phrase = build_join_announcement(member, after.channel, self.phrase_library)
                if hasattr(self, "features"):
                    await self.features.announce(state, member, after.channel, "join", phrase)
                else:
                    await state.enqueue(SpeechRequest(text=phrase, author_name="join", is_radio=False))
            elif left_tracked_channel and before.channel is not None:
                delayed_text = self.features.role_phrase(member, "leave") if hasattr(self, "features") else resolve_member_delayed_announcement(member, "leave")
                if delayed_text:
                    self.schedule_delayed_role_announcement(
                        guild_id=member.guild.id,
                        channel_id=before.channel.id,
                        member_id=member.id,
                        event_type="leave",
                        text=delayed_text,
                    )
                phrase = build_leave_announcement(member, before.channel, self.phrase_library)
                if hasattr(self, "features"):
                    await self.features.announce(state, member, before.channel, "leave", phrase)
                else:
                    await state.enqueue(SpeechRequest(text=phrase, author_name="leave", is_radio=False))
        except asyncio.QueueFull:
            logging.warning(
                "Очередь переполнена, пропускаю озвучку смены канала для %s в guild=%s",
                member.id,
                member.guild.id,
            )
        except Exception:
            logging.exception(
                "Не удалось озвучить смену канала участника %s в guild=%s",
                member.id,
                member.guild.id,
            )

    def get_state(self, guild: discord.Guild) -> GuildAudioState:
        state = self.guild_states.get(guild.id)
        if state is None:
            state = GuildAudioState(self, guild)
            self.guild_states[guild.id] = state
        return state

    async def close(self) -> None:
        if hasattr(self, "features"):
            await self.features.close()
        self.health_path.unlink(missing_ok=True)
        for task in list(self.delayed_announcement_tasks):
            task.cancel()
        for task in list(self.delayed_announcement_tasks):
            try:
                await task
            except asyncio.CancelledError:
                pass
        self.delayed_announcement_tasks.clear()
        if self.empty_channel_monitor_task:
            self.empty_channel_monitor_task.cancel()
            try:
                await self.empty_channel_monitor_task
            except asyncio.CancelledError:
                pass
            self.empty_channel_monitor_task = None
        for state in list(self.guild_states.values()):
            await state.shutdown()
        await super().close()

    def register_commands(self) -> None:
        @self.tree.command(name="join", description="Бот заходит в твой голосовой канал")
        @app_commands.checks.cooldown(1, 10, key=lambda i: (i.guild_id, i.user.id))
        @app_commands.guild_only()
        async def join(interaction: discord.Interaction) -> None:
            resolved = await self.ensure_voice_state(interaction)
            if resolved is None:
                await interaction.response.send_message(
                    "Сначала зайди в голосовой канал, а потом вызывай `/join`.",
                    ephemeral=True,
                )
                return

            state, channel = resolved
            await interaction.response.defer(ephemeral=True, thinking=True)
            try:
                await state.ensure_connected(channel)
            except Exception as exc:
                await interaction.followup.send(f"Не удалось подключиться к каналу: {exc}", ephemeral=True)
                return

            await interaction.followup.send(f"Подключился к `{channel.name}`. Диктор на позиции.", ephemeral=True)

        @self.tree.command(name="leave", description="Бот выходит из голосового канала")
        @app_commands.checks.has_permissions(manage_guild=True)
        @app_commands.guild_only()
        async def leave(interaction: discord.Interaction) -> None:
            if interaction.guild is None:
                await interaction.response.send_message("Эта команда работает только на сервере.", ephemeral=True)
                return

            state = self.get_state(interaction.guild)
            disconnected, cleared = await state.leave()
            if disconnected:
                await interaction.response.send_message(
                    f"Покинул канал, радио остановлено, из очереди убрано {cleared} реплик.",
                    ephemeral=True,
                )
            else:
                await interaction.response.send_message("Я и так сейчас не нахожусь в голосовом канале.", ephemeral=True)

        @self.tree.command(name="say", description="Озвучить текст в голосовом канале")
        @app_commands.checks.cooldown(1, 5, key=lambda i: (i.guild_id, i.user.id))
        @app_commands.describe(text="Текст для озвучки")
        @app_commands.guild_only()
        async def say(interaction: discord.Interaction, text: app_commands.Range[str, 1, MAX_SAY_LENGTH]) -> None:
            resolved = await self.ensure_voice_state(interaction)
            if resolved is None:
                await interaction.response.send_message(
                    "Для `/say` нужно находиться в голосовом канале.",
                    ephemeral=True,
                )
                return

            state, channel = resolved
            clean_text = normalize_user_text(text)
            await interaction.response.defer(ephemeral=True, thinking=True)

            try:
                await state.ensure_connected(channel)
                position = await state.enqueue(
                    SpeechRequest(text=clean_text, author_name=interaction.user.display_name, is_radio=False)
                )
            except asyncio.QueueFull:
                await interaction.followup.send(
                    "Очередь переполнена. Подожди, пока диктор дочитает накопившиеся распоряжения.",
                    ephemeral=True,
                )
                return
            except Exception as exc:
                await interaction.followup.send(f"Не удалось поставить текст в очередь: {exc}", ephemeral=True)
                return

            await interaction.followup.send(
                f"Текст поставлен в очередь. Позиция: {position}. Государственный диктор собрался с дыханием.",
                ephemeral=True,
            )

        @self.tree.command(name="radio", description="Включить или выключить автокомментарии про участников канала")
        @app_commands.checks.has_permissions(manage_guild=True)
        @app_commands.describe(
            enabled="true - включить радио, false - выключить",
            min_interval_seconds="Минимальная пауза между репликами",
            max_interval_seconds="Максимальная пауза между репликами",
        )
        @app_commands.guild_only()
        async def radio(
            interaction: discord.Interaction,
            enabled: bool = True,
            min_interval_seconds: app_commands.Range[int, MIN_RADIO_INTERVAL, MAX_RADIO_INTERVAL] | None = None,
            max_interval_seconds: app_commands.Range[int, MIN_RADIO_INTERVAL, MAX_RADIO_INTERVAL] | None = None,
        ) -> None:
            if interaction.guild is None:
                await interaction.response.send_message("Эта команда работает только на сервере.", ephemeral=True)
                return

            state = self.get_state(interaction.guild)

            if not enabled:
                stopped = await state.stop_radio()
                if stopped:
                    await interaction.response.send_message(
                        "Авторадио отключено. Эфир торжественно снят с паузы.",
                        ephemeral=True,
                    )
                else:
                    await interaction.response.send_message("Авторадио и так молчит.", ephemeral=True)
                return

            cfg = self.features.settings(interaction.guild.id)
            min_interval_seconds = min_interval_seconds if min_interval_seconds is not None else cfg["interval_min"]
            max_interval_seconds = max_interval_seconds if max_interval_seconds is not None else cfg["interval_max"]
            if min_interval_seconds > max_interval_seconds:
                await interaction.response.send_message(
                    "Минимальный интервал не может быть больше максимального.",
                    ephemeral=True,
                )
                return

            resolved = await self.ensure_voice_state(interaction)
            if resolved is None:
                await interaction.response.send_message(
                    "Чтобы включить `/radio`, зайди в голосовой канал.",
                    ephemeral=True,
                )
                return

            state, channel = resolved
            await interaction.response.defer(ephemeral=True, thinking=True)
            try:
                await state.ensure_connected(channel)
                started = await state.start_radio(min_interval_seconds, max_interval_seconds)
                self.features.preferences.update(interaction.guild.id, interval_min=min_interval_seconds, interval_max=max_interval_seconds)
                if started:
                    await state.enqueue(
                        SpeechRequest(
                            text=self.phrase_library.pick("RADIO_START_LINES"),
                            author_name="radio",
                            is_radio=True,
                        )
                    )
            except asyncio.QueueFull:
                await interaction.followup.send(
                    "Очередь уже забита, даже радио не может пробиться в эфир.",
                    ephemeral=True,
                )
                return
            except Exception as exc:
                await interaction.followup.send(f"Не удалось включить радиорежим: {exc}", ephemeral=True)
                return

            if started:
                await interaction.followup.send(
                    f"Авторадио включено. Интервал теперь случайный: от {min_interval_seconds} до {max_interval_seconds} секунд. Диктор наблюдает за каналом `{channel.name}`.",
                    ephemeral=True,
                )
            else:
                state.radio_interval_min = min_interval_seconds
                state.radio_interval_max = max_interval_seconds
                await interaction.followup.send(
                    f"Авторадио уже работало, я обновил диапазон до {min_interval_seconds}-{max_interval_seconds} секунд.",
                    ephemeral=True,
                )

        @self.tree.command(name="phrase_help", description="Показать категории фраз и доступные шаблонные переменные")
        @app_commands.guild_only()
        async def phrase_help(interaction: discord.Interaction) -> None:
            await interaction.response.send_message(build_phrase_help_text(), ephemeral=True)

        @self.tree.command(name="add_phrase", description="Добавить новую фразу в библиотеку бота")
        @app_commands.checks.has_permissions(manage_guild=True)
        @app_commands.describe(
            category="Категория фразы",
            text="Новая фраза для выбранной категории",
        )
        @app_commands.choices(category=PHRASE_CATEGORY_CHOICES)
        @app_commands.guild_only()
        async def add_phrase(
            interaction: discord.Interaction,
            category: app_commands.Choice[str],
            text: app_commands.Range[str, 1, 1000],
        ) -> None:
            clean_text = normalize_user_text(text)
            if not clean_text:
                await interaction.response.send_message("Фраза не должна быть пустой.", ephemeral=True)
                return

            try:
                insert_phrase(PHRASE_LIBRARY_PATH, category.value, clean_text)
                self.phrase_library.reload_if_changed(force=True, required=True)
            except Exception as exc:
                await interaction.response.send_message(
                    f"Не удалось добавить фразу: {exc}",
                    ephemeral=True,
                )
                return

            variables_text = format_template_variables(category.value)
            await interaction.response.send_message(
                f"Фраза добавлена в `{category.value}`.\n"
                f"Доступные переменные для этой категории: {variables_text}.",
                ephemeral=True,
            )

        @self.tree.error
        async def command_error(interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
            if isinstance(error, app_commands.MissingPermissions):
                message = "Для этой команды нужны права управления сервером."
            elif isinstance(error, app_commands.CheckFailure) and not isinstance(error, app_commands.CommandOnCooldown):
                message = "Зайди в канал бота. Перемещать его может только управляющий сервером."
            elif isinstance(error, app_commands.CommandOnCooldown):
                message = f"Подожди {error.retry_after:.0f} секунд."
            else:
                logging.error("Command failed: %s", error, exc_info=error)
                message = "Команда не выполнена. Подробности в логах бота."
            if interaction.response.is_done():
                await interaction.followup.send(message, ephemeral=True)
            else:
                await interaction.response.send_message(message, ephemeral=True)

    async def ensure_voice_state(
        self,
        interaction: discord.Interaction,
    ) -> tuple[GuildAudioState, discord.VoiceChannel | discord.StageChannel] | None:
        if interaction.guild is None:
            return None

        member = interaction.user
        if not isinstance(member, discord.Member):
            return None
        if not member.voice or not member.voice.channel:
            return None
        if not isinstance(member.voice.channel, (discord.VoiceChannel, discord.StageChannel)):
            return None

        state = self.get_state(interaction.guild)
        client = state.voice_client
        if (client and client.is_connected() and client.channel
                and client.channel.id != member.voice.channel.id
                and not member.guild_permissions.manage_guild):
            raise app_commands.CheckFailure("Only server managers may move the bot")
        return state, member.voice.channel


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    token = DISCORD_BOT_TOKEN.strip()
    if not token:
        raise RuntimeError(
            f"Токен бота не задан. Передай переменную окружения {DISCORD_BOT_TOKEN_ENV}."
        )

    bot = RadioAnnouncerBot()
    bot.run(token, log_handler=None)


if __name__ == "__main__":
    main()
