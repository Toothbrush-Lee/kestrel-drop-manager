"""Worker routing and release gates."""
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from kestrel_drop.worker import cli_command

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('release_checks', ROOT / 'scripts/check_release.py')
checks = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checks)

class ReleaseTests(unittest.TestCase):
    def test_frozen_worker_invokes_binary_without_python_flags(self):
        with patch.object(sys, 'frozen', True, create=True), patch.object(sys, 'executable', '/tmp/app'):
            self.assertEqual(cli_command(['list']), ['/tmp/app', 'list'])

    def test_installed_worker_uses_package(self):
        with patch.object(Path, 'is_file', return_value=False):
            self.assertEqual(cli_command(['list']), [sys.executable, '-u', '-m', 'kestrel_drop', 'list'])

    def test_checkout_worker_uses_launcher(self):
        self.assertEqual(cli_command(['list']), [sys.executable, '-u', str(ROOT / 'kestrel.py'), 'list'])

    def test_tag_must_match_all_version_sources(self):
        value = checks.version()
        self.assertEqual(checks.version('v' + value), value)
        for tag in ('v0.0.0', value, 'v' + value + '-test'):
            with self.assertRaises(ValueError): checks.version(tag)

    def test_prepare_requires_complete_asset_set(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(ValueError, 'assets mismatch'):
                checks.prepare(Path(temporary), 'v' + checks.version(), Path(temporary) / 'notes')
