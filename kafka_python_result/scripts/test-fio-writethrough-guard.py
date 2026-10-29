"""호스트 전용 모듈을 import하지 않고 실제 식별 함수를 회귀 검사한다."""
import ast
from pathlib import Path
import unittest

source=Path(__file__).with_name('run-fio-writethrough-suite.py').read_text()
node=next(n for n in ast.parse(source).body if isinstance(n,ast.FunctionDef) and n.name=='classify_vm')
scope={'Path':Path}
exec(compile(ast.Module(body=[node],type_ignores=[]),'<production guard>','exec'),scope)
classify=scope['classify_vm']
BIN='/team4/qemu-system-x86_64'
IMAGE='/team4/guest.qcow2'
ARGS=[BIN,'-name','fio-wt-team4','-drive','file='+IMAGE+',format=qcow2']

class GuardTest(unittest.TestCase):
    def test_owned_vm(self):self.assertIs(classify(ARGS,BIN,BIN,IMAGE),True)
    def test_launcher_is_not_vm(self):
        self.assertIsNone(classify(['bash','-lc',' '.join(ARGS)],'/usr/bin/bash',BIN,IMAGE))
    def test_other_binary_rejected(self):
        self.assertIs(classify(ARGS,'/other/qemu-system-x86_64',BIN,IMAGE),False)
    def test_other_name_rejected(self):
        args=ARGS.copy();args[2]='another-team4-test'
        self.assertIs(classify(args,BIN,BIN,IMAGE),False)
    def test_other_image_rejected(self):
        self.assertIs(classify(ARGS,BIN,BIN,'/team4/other.qcow2'),False)
    def test_duplicate_name_rejected(self):
        self.assertIs(classify(ARGS+['-name','fio-wt-team4'],BIN,BIN,IMAGE),False)
    def test_exited_process(self):self.assertIsNone(classify([],BIN,BIN,IMAGE))

if __name__=='__main__':unittest.main()
