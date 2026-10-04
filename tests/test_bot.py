import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import discord_radio_bot as bot


class PhraseTests(unittest.TestCase):
    def test_initialization_and_backup(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'phrases.txt'
            library = bot.PhraseLibrary(path)
            original = path.read_bytes()
            bot.insert_phrase(path, 'JOIN_ANNOUNCEMENTS', 'Hello {a}, {b}!')
            library.reload_if_changed(force=True, required=True)
            self.assertIn('Hello {a}, {b}!', library.get_section('JOIN_ANNOUNCEMENTS'))
            self.assertEqual(original, path.with_suffix('.txt.bak').read_bytes())
            self.assertFalse(list(path.parent.glob('phrases-*.tmp')))

    def test_invalid_templates_are_rejected_before_writing(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'phrases.txt'
            bot.PhraseLibrary(path)
            original = path.read_bytes()
            for phrase in ('{unknown}', '{a.name}', '{a!r}', '{a:>5}', '{', '[JOIN_ANNOUNCEMENTS]', '#hidden'):
                with self.subTest(phrase=phrase), self.assertRaises(ValueError):
                    bot.insert_phrase(path, 'JOIN_ANNOUNCEMENTS', phrase)
                self.assertEqual(original, path.read_bytes())

    def test_invalid_reload_keeps_last_valid_library(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'phrases.txt'
            library = bot.PhraseLibrary(path)
            previous = library.get_section('SOLO_TEMPLATES')
            path.write_text('[UNKNOWN]\nHello\n', encoding='utf-8')
            with self.assertLogs(level='ERROR'):
                library.reload_if_changed(force=True)
            self.assertEqual(previous, library._sections['SOLO_TEMPLATES'])


class AsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_playback_timeout_releases_queue(self):
        channel = SimpleNamespace(id=10)
        voice = Mock(channel=channel)
        voice.is_connected.return_value = True
        voice.disconnect = AsyncMock()
        guild = SimpleNamespace(id=1, voice_client=voice)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'audio.mp3'
            path.touch()
            owner = SimpleNamespace(
                synthesizer=SimpleNamespace(synthesize=AsyncMock(return_value=path)),
                cancel_guild_announcements=Mock(), main_loop=asyncio.get_running_loop(), ffmpeg_path='unused',
            )
            with patch.object(bot.discord, 'FFmpegOpusAudio') as source, patch.object(bot, 'PLAYBACK_TIMEOUT_SECONDS', 0.01):
                state = bot.GuildAudioState(owner, guild)
                try:
                    with self.assertLogs(level='WARNING'):
                        await state.enqueue(bot.SpeechRequest('Hello', 'test'))
                        await asyncio.wait_for(state.queue.join(), 1)
                    voice.stop.assert_called()
                    source.return_value.cleanup.assert_called()
                    self.assertFalse(path.exists())
                finally:
                    await state.shutdown()

    async def test_radio_recovers_after_bad_iteration(self):
        human = SimpleNamespace(id=1, bot=False)
        voice = Mock(channel=SimpleNamespace(id=10, members=[human]))
        voice.is_connected.return_value = True
        voice.is_playing.return_value = False
        voice.disconnect = AsyncMock()
        owner = SimpleNamespace(cancel_guild_announcements=Mock(), phrase_library=Mock())
        state = bot.GuildAudioState(owner, SimpleNamespace(id=1, voice_client=voice))
        state.enqueue = AsyncMock()
        original_sleep = asyncio.sleep
        async def quick_sleep(delay):
            await original_sleep(0.001)
        try:
            with patch.object(bot.asyncio, 'sleep', quick_sleep), patch.object(bot, 'build_radio_phrase', side_effect=[ValueError('bad'), 'Hello', 'Hello']):
                with self.assertLogs(level='ERROR'):
                    await state.start_radio(5, 5)
                    for _ in range(100):
                        if state.enqueue.await_count:
                            break
                        await original_sleep(0.001)
                    self.assertGreater(state.enqueue.await_count, 0)
                    self.assertFalse(state.radio_task.done())
        finally:
            await state.shutdown()

    async def test_stale_audio_is_not_played_after_disconnect(self):
        channel = SimpleNamespace(id=10)
        voice = Mock(channel=channel)
        voice.is_connected.return_value = True
        voice.disconnect = AsyncMock()
        guild = SimpleNamespace(id=1, voice_client=voice)
        started, release = asyncio.Event(), asyncio.Event()
        with tempfile.TemporaryDirectory() as folder:
            audio = Path(folder) / 'test.mp3'
            audio.touch()
            async def synthesize(text):
                started.set()
                await release.wait()
                return audio
            owner = SimpleNamespace(
                synthesizer=SimpleNamespace(synthesize=synthesize),
                cancel_guild_announcements=Mock(),
                main_loop=asyncio.get_running_loop(),
            )
            state = bot.GuildAudioState(owner, guild)
            try:
                await state.enqueue(bot.SpeechRequest('Hello', 'test'))
                await started.wait()
                await state.leave()
                release.set()
                await asyncio.wait_for(state.queue.join(), 1)
                voice.play.assert_not_called()
                self.assertFalse(audio.exists())
            finally:
                release.set()
                await state.shutdown()

    async def test_failed_radio_does_not_block_leave(self):
        voice = Mock()
        voice.is_connected.return_value = False
        guild = SimpleNamespace(id=1, voice_client=voice)
        owner = SimpleNamespace(cancel_guild_announcements=Mock())
        state = bot.GuildAudioState(owner, guild)
        async def fail():
            raise ValueError('bad phrase')
        state.radio_task = asyncio.create_task(fail())
        await asyncio.sleep(0)
        with self.assertLogs(level='ERROR'):
            await state.leave()
        self.assertIsNone(state.radio_task)
        await state.shutdown()

    async def test_new_event_cancels_previous_role_timer(self):
        owner = bot.RadioAnnouncerBot.__new__(bot.RadioAnnouncerBot)
        owner.delayed_announcement_tasks = set()
        owner.pending_announcements = {}
        # Use a real suspended coroutine so cancellation can be observed.
        async def delayed(**kwargs):
            await asyncio.sleep(100)
        owner.delayed_role_announcement = delayed
        args = dict(guild_id=1, channel_id=2, member_id=3, text='Test')
        owner.schedule_delayed_role_announcement(event_type='join', **args)
        previous = owner.pending_announcements[(1, 3)]
        owner.schedule_delayed_role_announcement(event_type='leave', **args)
        await asyncio.sleep(0)
        self.assertTrue(previous.cancelled())
        owner.cancel_guild_announcements(1)
        await asyncio.gather(*owner.delayed_announcement_tasks, return_exceptions=True)
        self.assertFalse(owner.pending_announcements)

    async def test_immediate_and_delayed_announcements_both_run(self):
        channel = SimpleNamespace(id=10, name='Room', members=[])
        role = SimpleNamespace(id=1426625952544194690, position=1)
        member = SimpleNamespace(id=5, bot=False, guild=SimpleNamespace(id=1), roles=[role], display_name='User')
        voice = Mock(channel=channel)
        voice.is_connected.return_value = True
        state = SimpleNamespace(voice_client=voice, enqueue=AsyncMock())
        owner = bot.RadioAnnouncerBot.__new__(bot.RadioAnnouncerBot)
        owner.guild_states = {1: state}
        owner.pending_announcements = {}
        owner.phrase_library = Mock()
        owner.phrase_library.get_section.return_value = ('Hello {a}',)
        owner.schedule_delayed_role_announcement = Mock()
        for before, after, event in ((None, channel, 'join'), (channel, None, 'leave')):
            await owner.on_voice_state_update(member, SimpleNamespace(channel=before), SimpleNamespace(channel=after))
            self.assertEqual(event, state.enqueue.call_args.args[0].author_name)
            self.assertEqual(event, owner.schedule_delayed_role_announcement.call_args.kwargs['event_type'])

    async def test_command_registration(self):
        with tempfile.TemporaryDirectory() as folder:
            with patch.object(bot, 'PHRASE_LIBRARY_PATH', Path(folder) / 'phrases.txt'), patch.object(bot, 'GTTSSpeechSynthesizer'):
                client = bot.RadioAnnouncerBot()
                self.assertEqual({'join', 'leave', 'say', 'radio', 'phrase_help', 'add_phrase',
                                  'settings', 'channel_access', 'role_phrase', 'phrases',
                                  'remove_phrase', 'test_phrase', 'status', 'backup'},
                                 {c.name for c in client.tree.get_commands()})
                await client.close()


if __name__ == '__main__':
    unittest.main()
