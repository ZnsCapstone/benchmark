import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch
from types import SimpleNamespace

spec=importlib.util.spec_from_file_location('native_controller',Path(__file__).resolve().parents[1]/'scripts/run-fio-native-controller.py')
c=importlib.util.module_from_spec(spec); spec.loader.exec_module(c)


class NativeTests(unittest.TestCase):
    def test_read_only_checks_retry_disconnect(self):
        replies=[SimpleNamespace(returncode=255,stdout='',stderr='disconnect'),
                 SimpleNamespace(returncode=0,stdout='ok',stderr='')]
        with patch.object(c.common,'call',side_effect=replies) as call, patch.object(c.time,'sleep'):
            self.assertEqual('ok',c.read_call('guest','sha256sum file').stdout)
            self.assertEqual(2,call.call_count)

    def test_remote_command_failure_is_not_retried(self):
        reply=SimpleNamespace(returncode=1,stdout='',stderr='missing')
        with patch.object(c.common,'call',return_value=reply) as call:
            with self.assertRaises(c.subprocess.CalledProcessError): c.read_call('guest','test -f missing')
            self.assertEqual(1,call.call_count)

    def test_plan_same_conditions_filtered_to_native_fs(self):
        p=c.plan()
        self.assertEqual(18,len(p))
        self.assertEqual(15,sum(x['group']=='short' for x in p))
        self.assertEqual(3,sum(x['group']=='gc18' for x in p))
        self.assertEqual({'f2fs'},{x['filesystem'] for x in p})
        self.assertEqual({('seq-write','4k'),('rand-write','4k'),('seq-write','1M')},{(x['workload'],x['bs']) for x in p})

    def test_complete_raw_and_crc_are_required(self):
        cell=c.plan()[0]
        row=dict(kernel_log_available=True,kernel_storage_errors=[],dm_available=False)
        raw={'jobs':[{'error':0,'write':{'runtime':120000}}]}
        prep={'jobs':[{'error':0,'read':{'io_bytes':1024**3},'write':{'io_bytes':1024**3}}]}
        manifest=dict(cell=cell,backend='native-f2fs-zns')
        c.validate(cell,manifest,row,raw,prep,dict(exit_code=0))
        for changed in [dict(exit_code=1),dict()]:
            with self.assertRaises(RuntimeError): c.validate(cell,manifest,row,raw,prep,changed)
        with self.assertRaises(RuntimeError): c.validate(cell,manifest,row,raw,{'jobs':[]},dict(exit_code=0))
        with self.assertRaises(RuntimeError): c.validate(cell,manifest,dict(row,kernel_storage_errors=['err']),raw,prep,dict(exit_code=0))

    def test_gc_scope_and_monotonicity(self):
        def data(n):
            return f'GC calls: {n} (BG: 0)\nCP calls: {n}\nTry to move {n} blocks\nReclaimed segs : Normal ({n})'
        r=c.fs_delta(data(3),data(10))
        self.assertEqual(7,r['gc_calls']); self.assertEqual(0,r['gc_bg_calls'])
        self.assertEqual(7,r['move_attempt_blocks'])
        with self.assertRaises(RuntimeError): c.fs_delta(data(10),data(3))
        with self.assertRaises(RuntimeError): c.fs_delta('',data(10))


if __name__=='__main__': unittest.main()
