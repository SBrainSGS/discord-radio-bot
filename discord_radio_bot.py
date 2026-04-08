import asyncio
import logging
import os
import random
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path

import discord
import imageio_ffmpeg
from discord import app_commands
from gtts import gTTS

BASE_DIR = Path(__file__).resolve().parent
PHRASE_LIBRARY_PATH = BASE_DIR / "radio_phrases.txt"

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

PHRASE_SECTION_NAMES = (
    "SOLO_TEMPLATES",
    "DUO_TEMPLATES",
    "GROUP_TEMPLATES",
    "RADIO_START_LINES",
    "JOIN_ANNOUNCEMENTS",
)

CATEGORY_VARIABLES = {
    "SOLO_TEMPLATES": ("a", "channel"),
    "DUO_TEMPLATES": ("a", "b", "channel"),
    "GROUP_TEMPLATES": ("a", "b", "c", "group", "channel"),
    "RADIO_START_LINES": tuple(),
    "JOIN_ANNOUNCEMENTS": ("a", "channel"),
}

NAME_SANITIZER = re.compile(r"[^0-9A-Za-zА-Яа-яЁё _.-]+")


@dataclass(slots=True)
class SpeechRequest:
    text: str
    author_name: str
    is_radio: bool = False


class PhraseLibrary:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._sections = {name: tuple() for name in PHRASE_SECTION_NAMES}
        self._mtime_ns: int | None = None
        self.reload_if_changed(force=True, required=True)

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

            parsed[current_section].append(line)

        missing_sections = [name for name, phrases in parsed.items() if not phrases]
        if missing_sections:
            joined = ", ".join(missing_sections)
            raise ValueError(f"В файле фраз пустые секции: {joined}")

        return {name: tuple(phrases) for name, phrases in parsed.items()}


class GTTSSpeechSynthesizer:
    def __init__(self) -> None:
        self.temp_dir = Path(tempfile.gettempdir()) / "discord_radio_bot_tts"
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    async def synthesize(self, text: str) -> Path:
        handle, raw_path = tempfile.mkstemp(prefix="tts_gtts_", suffix=".mp3", dir=self.temp_dir)
        os.close(handle)
        output_path = Path(raw_path)
        await asyncio.to_thread(
            gTTS(text=text, lang=GTTS_LANGUAGE, tld=GTTS_TLD, slow=False).save,
            str(output_path),
        )
        return output_path


def normalize_user_text(text: str) -> str:
    return " ".join(text.split()).strip()


def insert_phrase(path: Path, category: str, phrase: str) -> None:
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
        raise ValueError(f"Категория {category!r} не найдена в {path.name}.")

    insert_at = end_index
    while insert_at > start_index + 1 and not lines[insert_at - 1].strip():
        insert_at -= 1

    lines.insert(insert_at, phrase)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")


def format_template_variables(category: str) -> str:
    variables = CATEGORY_VARIABLES.get(category, ())
    if not variables:
        return "(без переменных)"
    return ", ".join(f"{{{variable}}}" for variable in variables)


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
        template = random.choice(phrase_library.get_section("SOLO_TEMPLATES"))
        return template.format(a=random.choice(names), channel=channel_name)

    if template_type == "duo":
        first, second = random.sample(names, 2)
        template = random.choice(phrase_library.get_section("DUO_TEMPLATES"))
        return template.format(a=first, b=second, channel=channel_name)

    chosen = random.sample(names, k=min(3, len(names)))
    template = random.choice(phrase_library.get_section("GROUP_TEMPLATES"))
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
    template = random.choice(phrase_library.get_section("JOIN_ANNOUNCEMENTS"))
    member_name = safe_display_name(member)
    channel_name = NAME_SANITIZER.sub(" ", channel.name).strip() or "секретный канал"
    return template.format(a=member_name, channel=channel_name)


class GuildAudioState:
    def __init__(self, bot: "RadioAnnouncerBot", guild: discord.Guild) -> None:
        self.bot = bot
        self.guild = guild
        self.queue: asyncio.Queue[SpeechRequest] = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        self.worker_task = asyncio.create_task(self.player_loop(), name=f"speech-worker-{guild.id}")
        self.radio_task: asyncio.Task[None] | None = None
        self.radio_interval_min = DEFAULT_RADIO_INTERVAL_MIN
        self.radio_interval_max = DEFAULT_RADIO_INTERVAL_MAX

    @property
    def voice_client(self) -> discord.VoiceClient | None:
        return self.guild.voice_client

    async def ensure_connected(self, channel: discord.VoiceChannel | discord.StageChannel) -> discord.VoiceClient:
        current = self.voice_client
        if current and current.is_connected():
            if current.channel and current.channel.id != channel.id:
                await current.move_to(channel)
            return current
        return await channel.connect(self_deaf=True)

    async def enqueue(self, request: SpeechRequest) -> int:
        if self.queue.full():
            raise asyncio.QueueFull
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
            try:
                client = self.voice_client
                if not client or not client.is_connected():
                    continue

                audio_path = await self.bot.synthesizer.synthesize(request.text)
                source = discord.FFmpegOpusAudio(
                    source=str(audio_path),
                    executable=self.bot.ffmpeg_path,
                    bitrate=96,
                )

                finished = self.bot.main_loop.create_future()

                def after_playback(error: Exception | None) -> None:
                    if finished.done():
                        return
                    self.bot.main_loop.call_soon_threadsafe(finished.set_result, error)

                client.play(source, after=after_playback)
                maybe_error = await finished
                if maybe_error:
                    raise maybe_error
            except Exception:
                logging.exception("Ошибка воспроизведения в guild=%s", self.guild.id)
            finally:
                if audio_path:
                    audio_path.unlink(missing_ok=True)
                self.queue.task_done()

    async def radio_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(random.randint(self.radio_interval_min, self.radio_interval_max))
                client = self.voice_client
                if not client or not client.is_connected() or not client.channel:
                    continue
                if client.is_playing() or self.queue.qsize() > 2:
                    continue

                humans = [member for member in client.channel.members if not member.bot]
                if not humans:
                    continue

                phrase = build_radio_phrase(client.channel, humans, self.bot.phrase_library)
                await self.enqueue(SpeechRequest(text=phrase, author_name="radio", is_radio=True))
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
        self.guild_states: dict[int, GuildAudioState] = {}
        self.main_loop: asyncio.AbstractEventLoop | None = None
        self.register_commands()

    async def setup_hook(self) -> None:
        self.main_loop = asyncio.get_running_loop()
        logging.info("TTS provider locked to gTTS (%s.%s)", GTTS_LANGUAGE, GTTS_TLD)
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
        logging.info("READY: %s (%s), guilds=%s", self.user, self.user.id if self.user else "?", len(self.guilds))

    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ) -> None:
        if member.bot or before.channel == after.channel or member.guild is None:
            return

        state = self.guild_states.get(member.guild.id)
        if state is None:
            return

        client = state.voice_client
        if not client or not client.is_connected() or not client.channel:
            return

        if after.channel is None or after.channel.id != client.channel.id:
            return

        try:
            phrase = build_join_announcement(member, after.channel, self.phrase_library)
            await state.enqueue(SpeechRequest(text=phrase, author_name="join", is_radio=False))
        except asyncio.QueueFull:
            logging.warning("Очередь переполнена, пропускаю приветствие для %s в guild=%s", member.id, member.guild.id)
        except Exception:
            logging.exception("Не удалось озвучить вход участника %s в guild=%s", member.id, member.guild.id)

    def get_state(self, guild: discord.Guild) -> GuildAudioState:
        state = self.guild_states.get(guild.id)
        if state is None:
            state = GuildAudioState(self, guild)
            self.guild_states[guild.id] = state
        return state

    async def close(self) -> None:
        for state in list(self.guild_states.values()):
            await state.shutdown()
        await super().close()

    def register_commands(self) -> None:
        @self.tree.command(name="join", description="Бот заходит в твой голосовой канал")
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
        @app_commands.describe(
            enabled="true - включить радио, false - выключить",
            min_interval_seconds="Минимальная пауза между репликами",
            max_interval_seconds="Максимальная пауза между репликами",
        )
        @app_commands.guild_only()
        async def radio(
            interaction: discord.Interaction,
            enabled: bool = True,
            min_interval_seconds: app_commands.Range[int, MIN_RADIO_INTERVAL, MAX_RADIO_INTERVAL] = DEFAULT_RADIO_INTERVAL_MIN,
            max_interval_seconds: app_commands.Range[int, MIN_RADIO_INTERVAL, MAX_RADIO_INTERVAL] = DEFAULT_RADIO_INTERVAL_MAX,
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
                if started:
                    await state.enqueue(
                        SpeechRequest(
                            text=random.choice(self.phrase_library.get_section("RADIO_START_LINES")),
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

        return self.get_state(interaction.guild), member.voice.channel


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
