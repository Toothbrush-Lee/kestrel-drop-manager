"""Exercise the actual CLI worker boundary without BLE or database mutation."""
import json
import subprocess
import tempfile
import unittest
import queue
from types import SimpleNamespace
from unittest.mock import patch

from kestrel_drop.worker import Job,cli_command
from kestrel_drop import manager
from kestrel_drop.tui import Terminal
from test_ui_workflow import Screen

class FollowupCliTests(unittest.TestCase):
    def test_real_cli_preview_accepts_clock_options_and_has_default_false_fields(self):
        for flags in ([],['--sync-clock','--allow-schedule-shift']):
            with tempfile.TemporaryDirectory() as root:
                completed=subprocess.run(cli_command(['--data-dir',root,'--events-json','followup','--parallel','6','--scan-seconds','8.0',*flags]),capture_output=True,text=True,timeout=15)
                self.assertEqual(completed.returncode,0,completed.stderr)
                events=[json.loads(line) for line in completed.stdout.splitlines()]
                self.assertEqual(events[-1],{'version':1,'event':'finished','code':0})
                result=next(e['data'] for e in events if e['event']=='result')
                self.assertEqual(result['status'],'dry_run');self.assertEqual(result['sync_clock'],bool(flags))
    def test_real_worker_retains_argparse_error_in_stderr_tail(self):
        with tempfile.TemporaryDirectory() as root:
            job=Job(root,['followup','--not-a-real-option'])
            try:
                job.thread.join(timeout=15)
                self.assertFalse(job.thread.is_alive())
                self.assertTrue(any('unrecognized arguments' in line for line in job.stderr_tail))
            finally:job.close()

    def test_exact_tui_command_parses_and_missing_ack_has_structured_error(self):
        args=manager.parser().parse_args(['followup','--parallel','6','--scan-seconds','8.0','--execute','--sync-clock','--allow-schedule-shift'])
        self.assertTrue(args.sync_clock and args.allow_schedule_shift and args.execute)
        with tempfile.TemporaryDirectory() as root:
            result=subprocess.run(cli_command(['--data-dir',root,'--events-json','followup','--sync-clock']),capture_output=True,text=True,timeout=15)
        self.assertEqual(result.returncode,2)
        events=[json.loads(line) for line in result.stdout.splitlines()]
        self.assertTrue(any(e['event']=='error' and '采样相位' in e['message'] for e in events))
    def test_result_panel_shows_real_worker_reason(self):
        with tempfile.TemporaryDirectory() as root:
            terminal=Terminal(Screen(24,100),manager.Library(root))
            q=queue.Queue();q.put(('exit',2))
            terminal.job=SimpleNamespace(events=q,payload=None,outcomes=[],failures=[],cancelled=False,arguments=['followup'],stderr_tail=['usage: kestrel.py','kestrel.py: error: unrecognized arguments: --old-option'])
            terminal.action='followup'
            with patch.object(terminal,'start',return_value=True):terminal.poll()
            self.assertIn('unrecognized arguments',terminal.result_view['failures'][0]['message'])
