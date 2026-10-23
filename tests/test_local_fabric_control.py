import pathlib,sys,tempfile,unittest
from unittest.mock import patch
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]/"scripts"))
import local_fabric_control as c
class Controls(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        c.STATE=pathlib.Path(self.tmp.name)/"state";c.MARKER=c.STATE/"operator-pause.json";c.DROPINS=pathlib.Path(self.tmp.name)/"units"
        self.calls=[]
    def command(self,*args):
        self.calls.append(args)
        if args[0]=="show":return "active" if args[1]==c.UNITS[0] else "inactive"
        return ""
    def test_repeat_pause_preserves_resume_set_and_inactive_unit(self):
        with patch.object(c,"command",side_effect=self.command):
            c.operate("pause");first=c.MARKER.read_bytes();c.operate("pause")
            self.assertEqual(c.MARKER.read_bytes(),first)
            self.assertTrue(all((c.DROPINS/(u+".d")/"90-ziji-operator-pause.conf").read_text()==c.CONTENT for u in c.UNITS))
            result=c.operate("resume")
        self.assertEqual(result["started_units"],[c.UNITS[0]])
        self.assertFalse(c.MARKER.exists())
        self.assertFalse(any("enable" in args for args in self.calls))
        self.assertFalse(any("ziji-local-recovery.timer" in args for args in self.calls))
    def test_conflicting_dropin_preserved(self):
        p=c.DROPINS/(c.UNITS[0]+".d")/"90-ziji-operator-pause.conf";p.parent.mkdir(parents=True);p.write_text("user configuration")
        with patch.object(c,"command",side_effect=self.command),self.assertRaisesRegex(RuntimeError,"dropin_conflict"):c.operate("pause")
        self.assertEqual(p.read_text(),"user configuration");self.assertFalse(c.MARKER.exists());self.assertEqual(self.calls,[])
    def test_failed_resume_restores_pause(self):
        with patch.object(c,"command",side_effect=self.command):c.operate("pause")
        first=c.MARKER.read_bytes()
        original=c.command
        def failure(*args):
            if args[0]=="start":return original(*args)
            return self.command(*args)
        with patch.object(c,"command",side_effect=failure),patch.object(c.subprocess,"run",side_effect=c.subprocess.TimeoutExpired("systemctl",30)),self.assertRaisesRegex(RuntimeError,"start_timeout"):c.operate("resume")
        self.assertEqual(c.MARKER.read_bytes(),first)
        self.assertEqual(self.calls[-1],("stop",c.UNITS[0]))
if __name__=="__main__":unittest.main()
