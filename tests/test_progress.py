import io
import unittest
from kestrel_drop.protocol import TransferProgress

class FakeClock:
    def __init__(self): self.current = 0.0
    def __call__(self): return self.current
    def advance(self, dt): self.current += dt

class FakeStream(io.StringIO):
    def __init__(self, tty): super().__init__(); self.tty=tty
    def isatty(self): return self.tty

class TestProgress(unittest.TestCase):
    def test_terminal_renders_in_place_and_finishes(self):
        clock=FakeClock(); out=FakeStream(tty=True)
        p=TransferProgress(100,stream=out,clock=clock)
        clock.advance(10);p.update(25)
        clock.advance(10);p.update(50)
        p.finish(success=True)
        text=out.getvalue()
        self.assertIn('25.0%',text)
        self.assertIn('50.0%',text)
        self.assertIn(' 2.5 rec/s',text)
        self.assertIn('ETA 00:20',text)
        self.assertIn('saved and verified',text)
        self.assertIn('\r',text)
        self.assertTrue(text.endswith('\n'))

    def test_new_samples_do_not_finish_at_original_count(self):
        clock=FakeClock();out=FakeStream(tty=True)
        p=TransferProgress(100,stream=out,clock=clock)
        clock.advance(10);p.update(100)
        self.assertFalse(p.closed)
        clock.advance(2);p.update(102)
        self.assertIn('(+2 new)',out.getvalue())
        self.assertIn('awaiting end',out.getvalue())
        p.finish(success=True)
        self.assertTrue(p.closed)

    def test_non_tty_logs_periodically_without_carriage_return(self):
        clock=FakeClock();out=FakeStream(tty=False)
        p=TransferProgress(20,stream=out,clock=clock)
        clock.advance(3);p.update(5)
        clock.advance(15);p.update(10)
        p.finish(success=False)
        self.assertEqual(len(out.getvalue().strip().splitlines()),3)
        self.assertNotIn('\r',out.getvalue())
        self.assertIn('stopped / not saved',out.getvalue())

    def test_empty_and_zero_time_does_not_divide_by_zero(self):
        clock=FakeClock();out=FakeStream(tty=True)
        p=TransferProgress(1,stream=out,clock=clock)
        p.update(1,force=True)
        self.assertIn('100.0%',out.getvalue())
        self.assertIn('ETA --:--',out.getvalue())
