import argparse
from pathlib import Path
import unittest
from kafka_only_fixed_load import command


class FixedLoadTests(unittest.TestCase):
    def test_fill_and_cycle_use_identical_target_for_both_conditions(self):
        args = argparse.Namespace(filesystem='ext4', scenario='write',
                                  jar='/example.jar', module_provenance='/module.json')
        for rate in (10000, 20000):
            cmd = command(args, rate, Path('/example-results'))
            self.assertEqual(cmd[cmd.index('--rates') + 1], str(rate))
            self.assertEqual(cmd[cmd.index('--prepare-rate') + 1], str(rate))
            self.assertEqual(cmd[cmd.index('--measure') + 1], '600')
