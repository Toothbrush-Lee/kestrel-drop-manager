"""Offline interaction contracts; these tests never open a BLE connection."""
import curses
import unittest
from unittest.mock import patch
from kestrel_drop import manager
from kestrel_drop.tui import Terminal
from kestrel_drop.ui_actions import retry_download_arguments, setting_change
from kestrel_drop.ui_text import input_window, text_width, wrap_lines
from kestrel_drop.worker import validate_event, validate_result


class Screen:
    def __init__(self, height=24, width=100): self.size=(height,width);self.lines=[]
    def getmaxyx(self): return self.size
    def erase(self): self.lines=[]
    def refresh(self): pass
    def addstr(self,row,col,text,attr=0):
        assert 0<=row<self.size[0],(row,text)
        assert col+text_width(text)<self.size[1],(col,text)
        self.lines.append(text)
    def move(self,row,col):
        assert 0<=row<self.size[0] and 0<=col<self.size[1]


class WorkflowTests(unittest.TestCase):
    def terminal(self,height=24,width=100):
        t=Terminal(Screen(height,width),manager.Library('/tmp/kestrel-ui-model-only'))
        t.devices={str(i):{'serial':str(i),'device_type':'D2','nearby':True,'database_records':12} for i in range(100)}
        return t
    def settings(self,t):
        t.device_state={'serial':'99','device_type':'D2','settings':{'interval_seconds':300,'wrap':True,'update_seconds':30}}
    def result(self,t):
        t.result_view={'action':'download','arguments':['download','--serials','0','99','--execute'],
                       'outcomes':[{'serial':str(i),'status':'downloaded','added_records':1,'total_records':12,'pending_records':0} for i in range(100)],'failures':[],'code':0}
    def test_help_and_logs_return_to_device_page(self):
        t=self.terminal();self.settings(t)
        for key in ('?','l'):
            t.handle(key);self.assertIsNotNone(t.text_view);t.draw()
            t.handle('\x1b');self.assertIsNone(t.text_view);self.assertEqual(t.device_state['serial'],'99')
    def test_hundred_results_can_reach_last_and_export_selected(self):
        t=self.terminal();self.result(t);t.handle(curses.KEY_END)
        self.assertEqual(t.result_view['selected'],99);t.draw();self.assertIn('99',' '.join(t.screen.lines))
        t.handle('e');self.assertEqual(t.form['target'],'99')
    def test_result_help_preserves_selection(self):
        t=self.terminal();self.result(t);t.handle(curses.KEY_NPAGE);selected=t.result_view['selected']
        t.handle('?');t.handle('\x1b');self.assertEqual(t.result_view['selected'],selected)
    def test_review_requires_second_enter_and_uses_device_page_target(self):
        t=self.terminal();self.settings(t);t.handle('1')
        t.form['fields'][0][1]='600'
        with patch.object(t,'start',return_value=True) as start:
            t.handle('\n');start.assert_not_called();self.assertEqual(t.review['serial'],'99')
            t.handle('\x1b');self.assertIsNotNone(t.form)
            t.handle('\n');t.handle('\n');start.assert_called_once_with('set',['set','99','--interval','600','--execute'])
        self.assertTrue(t.device_state['read_stale'])
        t.handle('1');self.assertIsNone(t.form);self.assertIn('重新读取',t.message)
    def test_failed_start_retains_review_and_editor(self):
        t=self.terminal();self.settings(t);t.handle('1');t.submit()
        with patch.object(t,'start',return_value=False): t.handle('\n')
        self.assertIsNotNone(t.review);self.assertIsNotNone(t.form);self.assertIn('error',t.review)
    def test_unknown_setting_cannot_be_changed(self):
        t=self.terminal();self.settings(t);t.device_state['settings']={}
        t.handle('1');self.assertIsNone(t.form);self.assertIn('未读到',t.message)
    def test_editor_inserts_and_deletes_at_unicode_cursor(self):
        t=self.terminal();t.open_form('测试',[('值','甲乙丙')],'add')
        for key in (curses.KEY_HOME,curses.KEY_RIGHT,'中',curses.KEY_DC):t.handle(key)
        self.assertEqual(t.form['fields'][0][1],'甲中丙')
        t.handle('\x15');self.assertEqual(t.form['fields'][0][1],'')
    def test_export_keeps_original_target_after_selection_changes(self):
        t=self.terminal();t.handle('e');t.selected=99
        for field,value in zip(t.form['fields'],('/tmp/export.csv','','')):field[1]=value
        with patch.object(t,'start',return_value=True) as start:t.submit()
        self.assertEqual(start.call_args.args[1][1],'0')
    def test_select_all_excludes_unknown_offline_unsupported_and_conflicting(self):
        t=self.terminal();t.devices['0']['nearby']=False;t.devices['1']['identity_conflict']=True;t.devices['2']['history_supported']=False
        t.handle('\x01');self.assertEqual(len(t.marked),97)
        t.handle('u');self.assertFalse(t.marked)
    def test_escape_at_root_does_not_exit(self):
        t=self.terminal();self.assertTrue(t.handle('\x1b'));self.assertFalse(t.handle('q'))
    def test_all_overlays_fit_minimum_and_normal_terminal(self):
        for height,width in ((18,68),(24,100),(40,140)):
            t=self.terminal(height,width);t.draw();self.settings(t);t.draw()
            t.open_text('?');t.draw();t.handle(curses.KEY_END);t.draw();t.handle('\x1b')
            t.handle('1');t.draw();t.submit();t.draw();t.handle('\x1b');t.handle('\x1b')
            self.result(t);t.handle(curses.KEY_END);t.draw()
    def test_process_launch_failure_is_shown(self):
        t=self.terminal()
        with patch('kestrel_drop.tui.Job',side_effect=OSError('无法启动')):self.assertFalse(t.start('list',['list']))
        self.assertIsNone(t.job);self.assertIn('无法启动',t.message)


class PlanningTests(unittest.TestCase):
    def test_retry_keeps_only_unfinished_and_preserves_address_alignment(self):
        args=['download','--serials','1','2','3','4','--ble-ids','a','-','c','d','--parallel','2','--timeout','30','--execute']
        result=retry_download_arguments(args,[{'serial':'1','status':'downloaded'}],[{'serial':'2','retryable':False}])
        self.assertEqual(result,['download','--serials','3','4','--ble-ids','c','d','--parallel','2','--timeout','30','--execute'])
    def test_no_retry_for_all_successful(self):
        self.assertIsNone(retry_download_arguments(['download','--serials','1'],[{'serial':'1','status':'up_to_date'}],[]))
    def test_chinese_wrap_and_clock_review(self):
        state={'serial':'123','settings':{'wrap':True}}
        self.assertEqual(setting_change('set_wrap','关闭',state)['arguments'][3],'off')
        self.assertIn('发送校时命令前',setting_change('set_time','now',state)['new'])
    def test_unicode_wrap_preserves_all_text_and_cell_limit(self):
        text='中文/很长的路径/abc'*20
        lines=wrap_lines(text,17);self.assertEqual(''.join(lines),text)
        self.assertTrue(all(text_width(line)<=17 for line in lines))
        visible,column=input_window(text,len(text),20)
        self.assertLessEqual(text_width(visible),20);self.assertLess(column,20)
    def test_malformed_events_are_rejected(self):
        for event in ({'version':True,'event':'finished','code':0},{'version':1,'event':'progress','received':-1,'expected':2,'rate':1,'elapsed':0,'eta':None},{'version':1,'event':'progress','received':1,'expected':2,'rate':float('nan'),'elapsed':0,'eta':None},{'version':1,'event':'error','code':'error','message':'x','action':'x','retryable':'yes'}):
            with self.assertRaises(ValueError): validate_event(event)
    def test_malformed_results_are_rejected(self):
        for action,data in (('list',{}),('download',{'downloads':[{'serial':'1','status':'downloaded','total_records':-1}]}),('set',{'before':{}})):
            with self.assertRaises(ValueError): validate_result(action,data)

class WorkerLifecycleTests(unittest.TestCase):
    def test_cancel_sends_only_one_interrupt(self):
        from unittest.mock import Mock
        from kestrel_drop.worker import Job
        job=Job.__new__(Job);job.cancelled=False;job.process=Mock();job.process.poll.return_value=None
        job.cancel();job.cancel()
        job.process.send_signal.assert_called_once()

    def test_pending_result_never_claims_formal_completion(self):
        t=WorkflowTests().terminal();WorkflowTests().result(t)
        t.result_view['outcomes']=[{'serial':'0','status':'pending','added_records':0,'total_records':12,'pending_records':3}]
        t.draw_result(24,100)
        text=' '.join(t.screen.lines)
        self.assertIn('未完成确认',text);self.assertNotIn('记录已正式保存',text)
