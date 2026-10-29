import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

from fio_evaluation import fio_command, validate_fixed_write, fixed_work_costs

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('final_plan',ROOT/'scripts/plan-fio-final.py')
planner=importlib.util.module_from_spec(spec);spec.loader.exec_module(planner)

class ExperimentSplitTests(unittest.TestCase):
    def test_performance_remains_time_based(self):
        cmd=fio_command('/tmp/data',2**30,'write','4k',32,20,120,'/tmp/raw.json')
        self.assertIn('--time_based=1',cmd)
        self.assertIn('--runtime=120',cmd)
        self.assertNotIn('--io_size=0',cmd)
        self.assertFalse(any(x.startswith('--io_size=') for x in cmd))

    def test_efficiency_uses_fixed_work_not_time(self):
        cmd=fio_command('/tmp/data',18*2**30,'randwrite','4k',32,0,120,'/tmp/raw.json',
                        total_write_bytes=54*2**30,randseed=20261019)
        for value in ['--size=19327352832','--io_size=57982058496',
                      '--randseed=20261019','--randrepeat=1']:
            self.assertIn(value,cmd)
        self.assertFalse(any(x.startswith(('--runtime=','--ramp_time=','--time_based=','--verify=')) for x in cmd))
        with self.assertRaises(ValueError):
            fio_command('/tmp/data',1024,'write','4k',32,20,120,'/tmp/raw',total_write_bytes=4096)

    def test_partial_work_is_failure(self):
        valid={'jobs':[{'error':0,'write':{'io_bytes':8192}}]}
        validate_fixed_write(valid,8192)
        for value in [4096,16384]:
            with self.assertRaises(RuntimeError):validate_fixed_write(valid,value)
        with self.assertRaises(ValueError):
            validate_fixed_write({'jobs':[{'error':28,'write':{'io_bytes':8192}}]},8192)

    def test_cost_scopes_exclude_double_counting_dm_device(self):
        record=dict(write_bytes=100,dm_delta=dict(logical_write_bytes=110,
                    user_write_bytes=110,wal_write_bytes=22,gc_write_bytes=33,
                    sstable_write_bytes=0,gc_moved_bytes=30,gc_reclaims=2),
                    os=dict(available=True,devices={'dm-0':{'write_bytes':110},
                                                   'nvme0n1':{'write_bytes':165}}))
        costs=fixed_work_costs(record)
        self.assertEqual(costs['dm_wa_issued'],1.5)
        self.assertEqual(costs['fio_to_device_write_ratio'],1.65)
        self.assertEqual(costs['gc_moved_bytes_per_fio_byte'],.3)
        self.assertIsNone(fixed_work_costs({'write_bytes':100})['fio_to_device_write_ratio'])

    def test_independent_plan_and_backend_commands(self):
        self.assertEqual(len(planner.cells('1-1')),15)
        self.assertEqual(len(planner.cells('1-2')),3)
        cell=planner.cells('1-2')[0]
        self.assertEqual(cell['warmup'],0)
        cmd=planner.command(cell,'dm-zns','f2fs','/tmp/results','/tmp/diag.ko',0)
        self.assertIn('/tmp/results/1-2/dm-zns/f2fs/wal-0ms/cell-01',cmd)
        self.assertEqual(cmd[cmd.index('--total-write-gib')+1],'54')
        self.assertEqual(cmd[cmd.index('--rounds')+1],'1')
        native=planner.command(cell,'native-f2fs-zns','f2fs','/tmp/results')
        self.assertNotIn('--prepare',native)
        self.assertIn('nvme0n1,nvme1n1',native)
        with self.assertRaises(ValueError):planner.command(cell,'native-f2fs-zns','ext4','/tmp/results')

    def test_cli_rejects_uncontrolled_efficiency(self):
        base=[sys.executable,str(ROOT/'src/fio_evaluation.py'),'--evaluation','gc-efficiency',
              '--file-gib','18','--total-write-gib','54','--workloads','seq-write',
              '--qds','32','--warmup','0','--randseed','123']
        success=subprocess.run(base,capture_output=True,text=True)
        self.assertEqual(success.returncode,0,success.stderr)
        self.assertEqual(json.loads(success.stdout)['termination'],'fixed-write-bytes')
        for extra in [['--warmup','20'],['--qds','1,32'],['--rounds','2'],['--resume']]:
            result=subprocess.run(base+extra,capture_output=True,text=True)
            self.assertEqual(result.returncode,2)

    @unittest.skipUnless(shutil.which('fio'), 'fio unavailable')
    def test_real_fio_wraps_file_without_growing_it(self):
        version=subprocess.run(['fio','--version'],capture_output=True,text=True)
        if not version.stdout.startswith('fio-'):self.skipTest('not Flexible I/O Tester')
        # CLI 의미 검증용 임시 1MiB 파일만 사용. 장치/마운트/모듈 접근 없음.
        with tempfile.TemporaryDirectory(prefix='fio-fixed-work-') as folder:
            p=Path(folder); data=p/'data';data.write_bytes(b'\0'*2**20)
            for rw in ['write','randwrite']:
                raw=p/(rw+'.json')
                cmd=fio_command(data,2**20,rw,'4k',1,0,0,raw,
                                total_write_bytes=3*2**20,randseed=123)
                cmd[cmd.index('--direct=1')]='--direct=0'
                cmd[cmd.index('--ioengine=libaio')]='--ioengine=sync'
                subprocess.run(cmd,capture_output=True,text=True,check=True,timeout=20)
                validate_fixed_write(json.loads(raw.read_text()),3*2**20)
                self.assertEqual(data.stat().st_size,2**20)

if __name__=='__main__':unittest.main()
