"""인증 부재와 응답 유실에서 결과를 잃거나 중복 게시하지 않는지 확인한다."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

s=importlib.util.spec_from_file_location('publisher',Path(__file__).resolve().parents[1]/'scripts/publish-benchmark-notion.py')
p=importlib.util.module_from_spec(s);s.loader.exec_module(p)


class OutboxTest(unittest.TestCase):
    def setup_outbox(self,root):
        row=dict(cell=dict(index=1,kind='fio-read',backend='dm-zns',filesystem='ext4',workload='seq-read',bs='4k',module='final'),status='failed')
        (root/'notion-outbox').mkdir();f=root/'notion-outbox/cell-01.json';f.write_text(json.dumps(row));return row,f

    def test_no_credentials_keeps_pending_without_network(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);row,f=self.setup_outbox(root)
            with patch.object(p,'credentials',return_value=None),patch.object(p,'api') as api:p.once(root)
            api.assert_not_called();self.assertTrue(f.exists())
            self.assertEqual(json.loads((root/'notion-publisher-status.json').read_text())['state'],'waiting-for-credentials')

    def test_response_loss_does_not_duplicate_remote_result(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);row,f=self.setup_outbox(root);remote=[];appends=[]
            def transport(token,method,path,payload=None):
                if method=='GET':return dict(results=remote,has_more=False)
                appends.append(payload)
                for b in payload['children']:
                    remote.append(dict(type='paragraph',paragraph=dict(rich_text=b['paragraph']['rich_text'])))
                raise TimeoutError('응답만 유실')
            with patch.object(p,'credentials',return_value='test-only'),patch.object(p,'api',side_effect=transport):
                with self.assertRaises(TimeoutError):p.once(root)
                p.once(root)
            self.assertEqual(len(appends),1)
            self.assertTrue((root/'notion-posted/cell-01.json').exists())


if __name__=='__main__':unittest.main()
