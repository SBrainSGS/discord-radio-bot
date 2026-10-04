import asyncio
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import discord_radio_bot as core
from radio_features import CachedSpeech, HostFeatures, Preferences


class PersistenceTests(unittest.TestCase):
    def test_preferences_are_persistent_and_isolated(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'settings.json'
            prefs = Preferences(path)
            prefs.update(1, volume=20, voice='female')
            restored = Preferences(path)
            self.assertEqual(20, restored.get(1)['volume'])
            self.assertEqual('male', restored.get(2)['voice'])
            self.assertEqual(80, restored.get(2)['volume'])

    def test_template_does_not_repeat_immediately(self):
        with tempfile.TemporaryDirectory() as folder:
            library = core.PhraseLibrary(Path(folder) / 'phrases.txt')
            core.insert_phrase(library.path, 'JOIN_ANNOUNCEMENTS', 'Second {a}')
            for _ in range(20):
                first = library.pick('JOIN_ANNOUNCEMENTS')
                self.assertNotEqual(first, library.pick('JOIN_ANNOUNCEMENTS'))

    def test_remove_checks_expected_text_and_preserves_last_phrase(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'phrases.txt'
            library = core.PhraseLibrary(path)
            core.insert_phrase(path, 'RADIO_START_LINES', 'New phrase')
            with self.assertRaises(ValueError):
                core.remove_phrase(path, 'RADIO_START_LINES', 2, 'Wrong')
            core.remove_phrase(path, 'RADIO_START_LINES', 2, 'New phrase')
            library.reload_if_changed(force=True)
            self.assertNotIn('New phrase', library.get_section('RADIO_START_LINES'))
            with self.assertRaises(ValueError):
                core.remove_phrase(path, 'RADIO_START_LINES', 1, library.get_section('RADIO_START_LINES')[0])


class FeatureTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        with patch.object(core, 'PHRASE_LIBRARY_PATH', Path(self.temp.name) / 'phrases.txt'):
            self.bot = core.RadioAnnouncerBot()

    async def asyncTearDown(self):
        await self.bot.close()
        self.temp.cleanup()

    async def test_settings_callback_persists_only_parameters(self):
        interaction = SimpleNamespace(guild_id=1, response=SimpleNamespace(send_message=AsyncMock()))
        await self.bot.tree.get_command('settings').callback(interaction, voice='male', volume=70)
        self.assertEqual(70, self.bot.features.settings(1)['volume'])
        self.assertTrue((Path(self.temp.name) / 'settings.json').exists())

    async def test_role_override_and_disable(self):
        member = SimpleNamespace(guild=SimpleNamespace(id=1), roles=[SimpleNamespace(id=1426625952544194690, position=1)], display_name='Listener')
        self.bot.features.preferences.update(1, roles={'1426625952544194690': {'join': 'Hello {a}'}})
        self.assertEqual('Hello Listener', self.bot.features.role_phrase(member, 'join'))
        self.bot.features.preferences.update(1, roles={'1426625952544194690': {}})
        self.assertIsNone(self.bot.features.role_phrase(member, 'join'))

    async def test_mute_threshold_cooldown_and_reset(self):
        member = SimpleNamespace(id=2, bot=False, display_name='Listener', voice=SimpleNamespace(self_mute=True, mute=False, self_deaf=False, deaf=False))
        channel = SimpleNamespace(id=3, members=[member])
        state = SimpleNamespace(guild=SimpleNamespace(id=1), voice_client=Mock(channel=channel), queue=asyncio.Queue(), enqueue=AsyncMock())
        state.voice_client.is_connected.return_value = True
        self.bot.guild_states[1] = state
        key = (1, 3, 2)
        self.bot.features.muted[key] = (time.monotonic() - 301, -float('inf'))
        await self.bot.features.scan_mutes()
        await self.bot.features.scan_mutes()
        self.assertEqual(1, state.enqueue.await_count)
        member.voice.self_mute = False
        await self.bot.features.scan_mutes()
        self.assertNotIn(key, self.bot.features.muted)
        self.bot.guild_states.clear()

    async def test_batch_merges_and_stale_generation_is_dropped(self):
        state = SimpleNamespace(guild=SimpleNamespace(id=1), generation=1, enqueue=AsyncMock())
        key = (1, 3, 'join')
        channel = SimpleNamespace(id=3)
        self.bot.features.batches[key] = [('A', 'Hello A'), ('B', 'Hello B')]
        with patch('radio_features.asyncio.sleep', AsyncMock()):
            await self.bot.features.flush_batch(state, channel, 'join', key, 1)
        self.assertIn('A и B', state.enqueue.call_args.args[0].text)
        self.bot.features.batches[key] = [('C', 'Hello C')]
        with patch('radio_features.asyncio.sleep', AsyncMock()):
            await self.bot.features.flush_batch(state, channel, 'join', key, 0)
        self.assertEqual(1, state.enqueue.await_count)

    async def test_cache_reuses_audio_and_returned_files_are_disposable(self):
        async def save(path):
            Path(path).write_bytes(b'fake mp3')
        with patch('radio_features.edge_tts.Communicate') as communicate:
            communicate.return_value.save = AsyncMock(side_effect=save)
            synth = self.bot.synthesizer
            first = await synth.synthesize('Hello')
            first.unlink()
            second = await synth.synthesize('Hello')
            self.assertEqual(b'fake mp3', second.read_bytes())
            second.unlink()
            self.assertEqual(1, communicate.return_value.save.await_count)

    async def test_fallback_is_not_deleted_or_cached_as_male(self):
        path = Path(self.temp.name) / 'fallback.mp3'
        path.write_bytes(b'fallback')
        self.bot.synthesizer.fallback = SimpleNamespace(synthesize=AsyncMock(return_value=path))
        with patch('radio_features.edge_tts.Communicate') as communicate:
            communicate.return_value.save = AsyncMock(side_effect=RuntimeError('offline'))
            with self.assertLogs(level='WARNING'):
                returned = await self.bot.synthesizer.synthesize('Hello')
        self.assertTrue(returned.exists())
        self.assertFalse(list(self.bot.synthesizer.folder.glob('*.mp3')))


if __name__ == '__main__':
    unittest.main()
