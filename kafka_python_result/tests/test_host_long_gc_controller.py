import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('host_long_gc',Path(__file__).resolve().parents[1]/'scripts/run-long-gc-on-femu-host.py')
host=importlib.util.module_from_spec(spec);spec.loader.exec_module(host)

class HostControllerTests(unittest.TestCase):
    def test_plan_preserves_all_twenty_four_conditions(self):
        cells=host.cells()
        self.assertEqual(len(cells),24)
        self.assertEqual([c['index'] for c in cells],list(range(1,25)))
        self.assertEqual([c['rate'] for c in cells[:4]],[3000,5000,10000,20000])
        self.assertEqual(cells[16]['backend'],'native')
        self.assertEqual(len({tuple(c[k] for k in ['backend','filesystem','scenario','rate']) for c in cells}),24)

    def test_existing_guest_output_paths_are_preserved(self):
        out,_=host.paths(host.cells()[0])
        self.assertTrue(out.endswith('test2-long-gc-dm-20261020/write/ext4/3000ops'))
        out,_=host.paths(host.cells()[16])
        self.assertTrue(out.endswith('test2-long-gc-native-20261020/write/3000ops'))

    def test_unknown_team4_vm_is_not_shutdown(self):
        with patch.object(host,'run',return_value=SimpleNamespace(returncode=0,stdout='123 qemu-system-x86_64 -name unrelated')):
            with self.assertRaises(RuntimeError):host.qemu()

    def test_recognized_team4_vm_is_only_observed(self):
        with patch.object(host,'run',return_value=SimpleNamespace(returncode=0,stdout='123 qemu-system-x86_64 -name dm-zns-512m-team4')):
            self.assertEqual(len(host.qemu()),1)

    def test_guest_connection_is_strict_and_loopback_only(self):
        self.assertIn('StrictHostKeyChecking=yes',host.SSH)
        with patch.object(host,'run',return_value=SimpleNamespace(returncode=0,stdout='ok')) as run:
            host.guest('hostname')
            self.assertIn('zns@127.0.0.1',run.call_args.args[0])
            self.assertNotIn('ProxyJump',str(run.call_args.args[0]))
