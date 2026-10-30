"""Check rendered header/data cell positions, including wide participant identifiers."""
import unittest

from kestrel_drop import manager
from kestrel_drop.tui import Terminal
from kestrel_drop.ui_text import text_width
from test_ui_workflow import Screen


class Grid(Screen):
    def __init__(self,height,width):super().__init__(height,width);self.rows={}
    def erase(self):super().erase();self.rows={}
    def addstr(self,row,col,text,attr=0):
        super().addstr(row,col,text,attr);self.rows[row]=text


def start(text,token):return text_width(text[:text.index(token)])
def end(text,token):return start(text,token)+text_width(token)

def window(text,left,right):
    result='';position=0
    for char in text:
        width=text_width(char)
        if left<=position and position+width<=right:result+=char
        position+=width
    return result


class TableAlignmentTests(unittest.TestCase):
    def terminal(self,width):
        t=Terminal(Grid(24,width),manager.Library('/tmp/kestrel-table-layout-only'))
        t.devices={
            '2417129':{'serial':'2417129','device_type':'D1','database':True,'management_code':'D1-001','participant_code':'00007','nearby':True,'seen_this_scan':True,'database_records':12345,'pending_records':0,'last_timestamp':'2026-10-30 12:34:56'},
            '2467018':{'serial':'2467018','device_type':'未知','database':True,'management_code':'D2-009','participant_code':'受试甲','nearby':True,'database_records':6,'pending_records':123,'last_timestamp':'2026-10-29 09:00:00'}}
        t.marked={'2417129'}
        return t
    def test_headers_and_both_rows_share_cell_positions_at_every_width(self):
        for width in (68,75,80,100,140):
            with self.subTest(width=width):
                t=self.terminal(width);t.draw();header=t.screen.rows[4];one=t.screen.rows[5];two=t.screen.rows[6]
                for title,a,b in [('型号','D1','未知'),('管理编号','D1-001','D2-009'),('序列号','2417129','2467018'),('受试者','00007','受试甲'),('发现','本次','近期'),('状态/末条','2026','2026')]:
                    self.assertEqual(start(header,title),start(one,a),(width,title))
                    self.assertEqual(start(header,title),start(two,b),(width,title))
                for title,a,b in [('已存','12345','6'),('未确认','0','123')]:
                    column_end=end(header,title)
                    for row,value in ((one,a),(two,b)):
                        self.assertEqual(window(row,column_end-6,column_end).strip(),value)
                        self.assertEqual(window(row,column_end,column_end+1),' ')
                self.assertEqual(start(header,'选'),start(one,'*'))
                self.assertEqual(text_width(header),width-2);self.assertEqual(text_width(one),width-2)
    def test_long_and_wide_participant_does_not_move_adjacent_columns_or_edit_target(self):
        for code in ('Ｐ００１受试者甲乙','P-e\u0301-'+'试'*30):
            t=self.terminal(68);t.devices['2417129']['participant_code']=code;t.draw()
            row=t.screen.rows[5];header=t.screen.rows[4]
            self.assertIn('…',row)
            self.assertEqual(start(row,'本次'),start(header,'发现'))
            self.assertEqual(t.devices['2417129']['participant_code'],code)
            t.handle('z');self.assertEqual(t.form['target'],'2417129');self.assertEqual(t.form['fields'][0][1],code)
    def test_unassigned_and_unverified_devices_keep_distinct_columns(self):
        t=self.terminal(68);t.devices={'ble:a':{'serial':None,'device_type':'未知','nearby':True}}
        t.draw();header=t.screen.rows[4];row=t.screen.rows[5]
        self.assertEqual(start(header,'序列号'),start(row,'待核验'))
        self.assertEqual(start(header,'受试者'),start(row,'未分配'))
        self.assertEqual(start(header,'发现'),start(row,'近期'))
    def test_download_and_configuration_results_show_current_participant_aligned(self):
        for width in (68,100,140):
            for action in ('download','set-batch'):
                t=self.terminal(width)
                t.result_view={'action':action,'outcomes':[{'serial':'2417129','status':'verified' if action=='set-batch' else 'downloaded','added_records':3,'total_records':12345,'pending_records':0}],'failures':[],'code':0,'arguments':[]}
                t.draw();header=t.screen.rows[3];row=t.screen.rows[4]
                for title,value in [('管理编号','D1-001'),('序列号','2417129'),('受试者','00007'),('状态','配置已回读验证' if action=='set-batch' else '已保存')]:
                    self.assertEqual(start(header,title),start(row,value))
                self.assertEqual(text_width(row),width-2)
                if action=='set-batch':self.assertNotIn('新增',header)
    def test_preview_measurement_headers_and_values_have_matching_right_edges(self):
        for width in (68,100,140):
            t=self.terminal(width);t.preview=[{'timestamp_device_local':'2026-10-30 12:34:56','temperature_c':-12.34,'relative_humidity_pct':None,'station_pressure_hpa':1013.2}]
            t.draw();header=t.screen.rows[2];row=t.screen.rows[3]
            self.assertEqual(start(header,'记录时间'),start(row,'2026'))
            for title,value in [('温度°C','-12.34'),('湿度%','--'),('气压hPa','1013.2')]:self.assertEqual(end(header,title),end(row,value))
