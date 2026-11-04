import json
import tempfile
import unittest
from kestrel_drop.manager import Library
from kestrel_drop import protocol
from test_manager import RAW

class DownloadDiagnosticsTests(unittest.TestCase):
    def test_failure_is_durable_without_publishing_or_removing_pending(self):
        with tempfile.TemporaryDirectory() as tmp:
            library=Library(tmp);store=library._writable_store()
            store.register_device('123','D1')
            transfer=store.begin_transfer('123',0,10)
            row=protocol.decode_record(RAW[:9],block=0,position=0,model='D1')
            store.append_block(transfer,[row],[{'block':0,'records':1}])
            store.interrupt_transfer(transfer,RuntimeError('Missing block 4'))
            with store.connect() as c:
                row=c.execute('SELECT status,validation_json FROM transfers WHERE id=?',(transfer,)).fetchone()
                self.assertEqual(row['status'],'interrupted')
                self.assertEqual(json.loads(row['validation_json'])['error'],'Missing block 4')
                self.assertEqual(c.execute('SELECT count(*) FROM observations').fetchone()[0],0)
                self.assertEqual(c.execute('SELECT count(*) FROM pending_blocks').fetchone()[0],1)
            store.interrupt_transfer(transfer,RuntimeError('do not overwrite'))
            with store.connect() as c:self.assertIn('Missing block 4',c.execute('SELECT validation_json FROM transfers WHERE id=?',(transfer,)).fetchone()[0])
