"""Replay real manager events through the worker and TUI, without BLE or data writes."""
import io
import json
import tempfile
import unittest
from unittest.mock import Mock, patch
from kestrel_drop import events, manager
from kestrel_drop.tui import Terminal
from kestrel_drop.worker import Job, validate_event
from test_iteration import FakeScreen


class WorkerEventTests(unittest.TestCase):
    def completion(self, **changes):
        return {'version':1,'event':'completed','serial':'2509793','status':'downloaded',
                'added_records':8,'total_records':13988,'pending_records':0,
                'first_timestamp':'2025-12-26 04:20:00','last_timestamp':'2026-10-27 00:35:47',
                'code':0,**changes}

    def test_reported_success_completion_accepts_integer_code(self):
        event=self.completion()
        self.assertIs(validate_event(event),event)

    def test_pending_completion_accepts_nonzero_code(self):
        event=self.completion(status='pending',code=3,pending_records=8)
        self.assertIs(validate_event(event),event)

    def test_completion_rejects_string_bool_null_and_negative_code(self):
        for code in ('0',True,False,None,-1):
            with self.subTest(code=code),self.assertRaises(ValueError):validate_event(self.completion(code=code))

    def test_error_requires_symbolic_code(self):
        event={'version':1,'event':'error','code':'completion_pending','message':'未完成','action':'稍后重试','retryable':True}
        self.assertIs(validate_event(event),event)
        for code in (0,True,None):
            with self.subTest(code=code),self.assertRaises(ValueError):validate_event({**event,'code':code})

    def test_finished_requires_integer_code(self):
        for code in (0,2,130):validate_event({'version':1,'event':'finished','code':code})
        for code in ('0',True,None):
            with self.subTest(code=code),self.assertRaises(ValueError):validate_event({'version':1,'event':'finished','code':code})

    def replay(self, lines, code):
        process=Mock(stdout=io.StringIO(lines),stderr=io.StringIO())
        process.wait.return_value=code
        process.poll.return_value=code
        with patch('kestrel_drop.worker.subprocess.Popen',return_value=process):
            job=Job('/tmp/kestrel-worker-event-model-only',['download','--serials','2509793','2525425','3087666','--execute'])
            job.thread.join(timeout=5)
        self.assertFalse(job.thread.is_alive())
        terminal=Terminal(FakeScreen(),manager.Library('/tmp/kestrel-worker-event-model-only'))
        terminal.devices={serial:{'serial':serial} for serial in ('2509793','2525425','3087666')}
        terminal.job=job;terminal.action='download'
        with patch.object(terminal,'start') as start:
            terminal.poll();start.assert_called_once_with('list',['list'])
        return job,terminal

    def test_real_three_device_manager_stream_is_successful_end_to_end(self):
        output=io.StringIO()
        expected=[('2509793',13988,'2025-12-26 04:20:00','2026-10-27 00:35:47'),
                  ('2525425',8490,'2026-02-15 07:20:00','2026-10-27 00:34:30'),
                  ('3087666',6958,'2026-10-02 20:50:00','2026-10-27 00:37:08')]
        with tempfile.TemporaryDirectory() as directory:
            library=manager.Library(directory)
            with events.stream(output):
                for serial,total,first,last in expected:
                    state={'serial':serial,'committed_records':total,'pending_records':0,'first_timestamp':first,'last_timestamp':last}
                    with patch.object(library,'status',return_value=[state]):library.complete_download(serial,previous=total-8)
                events.result({'downloads':list(library.download_results.values()),'incomplete':[]})
                events.emit('finished',code=0)
        job,terminal=self.replay(output.getvalue(),0)
        self.assertFalse(job.protocol_error);self.assertTrue(job.finished)
        self.assertEqual(len(job.outcomes),3);self.assertEqual(terminal.result_view['code'],0)
        self.assertFalse(terminal.result_view['failures'])
        for serial,total,_,_ in expected:
            self.assertEqual(terminal.devices[serial]['database_records'],total)
        self.assertFalse(any('后台事件格式错误' in line for line in terminal.logs))

    def test_pending_worker_stream_remains_incomplete(self):
        completed=self.completion(status='pending',code=3,pending_records=8)
        error={'version':1,'event':'error','serial':'2509793','code':'completion_pending',
               'message':'仍有暂存','action':'等待新样本','retryable':True}
        result={'version':1,'event':'result','data':{'downloads':[completed],'incomplete':[error]}}
        lines=''.join(json.dumps(event)+'\n' for event in (completed,error,result,{'version':1,'event':'finished','code':2}))
        job,terminal=self.replay(lines,2)
        self.assertFalse(job.protocol_error);self.assertEqual(terminal.result_view['code'],2)
        self.assertEqual(terminal.result_view['failures'][0]['code'],'completion_pending')

    def test_bad_completion_still_forces_protocol_failure(self):
        lines=''.join(json.dumps(event)+'\n' for event in (self.completion(code='0'),{'version':1,'event':'finished','code':0}))
        job,terminal=self.replay(lines,0)
        self.assertTrue(job.protocol_error);self.assertEqual(terminal.result_view['code'],2)
