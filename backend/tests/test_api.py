"""HTTP-level checks for the adjudication query endpoints."""
import os
import sys
import unittest

os.environ.setdefault('DB_DRIVER', 'mysql+pymysql')
os.environ.setdefault('DB_USER', 'app')
os.environ.setdefault('DB_PASSWORD', 'apppw')
os.environ.setdefault('DB_HOST', '127.0.0.1')
os.environ.setdefault('DB_PORT', '13306')
os.environ.setdefault('DB_NAME', 'workflow')
os.environ.setdefault('AUTH_KEY', 'test-key-for-adjudication-suite')
os.environ.setdefault('REDIS_URL', 'redis://127.0.0.1:6390/0')

sys.path.insert(0, '/workspace/backend')

from fastapi.testclient import TestClient  # noqa: E402

from src.config.settings import engine  # noqa: E402
from src.main import app  # noqa: E402
from src.models.tables import audit, cases, evidence, runs, schema, users  # noqa: E402
from src.services import domain  # noqa: E402
from src.services.security import hash_password  # noqa: E402
from src.utils.encoding import canonical  # noqa: E402
from src.utils.time import utc_time  # noqa: E402

client = TestClient(app)


class AdjudicationApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        schema.create_all(engine)
        with engine.begin() as conn:
            conn.execute(users.insert().values(
                username='api_tester', password_hash=hash_password('pw'), role='viewer'))
            cls.case_id = conn.execute(cases.insert().values(
                slug='api-ruling-case', title='接口裁定用例', payload=domain.SPEC['sample'],
                author='api_tester', created_at=utc_time())).inserted_primary_key[0]
            cls.run_id = conn.execute(runs.insert().values(
                case_id=cls.case_id, status='complete', owner='api_tester',
                started_at=utc_time(), finished_at=utc_time(), created_at=utc_time())
            ).inserted_primary_key[0]
            cls.report = domain.solve(domain.SPEC['sample'], engine)
            cls.evidence_id = conn.execute(evidence.insert().values(
                run_id=cls.run_id, report=cls.report,
                digest='0' * 64)).inserted_primary_key[0]
        login = client.post('/login', json={'username': 'api_tester', 'password': 'pw'})
        cls.token = login.json()['token']
        cls.headers = {'Authorization': f'Bearer {cls.token}'}

    @classmethod
    def tearDownClass(cls):
        with engine.begin() as conn:
            conn.execute(audit.delete())
            conn.execute(evidence.delete())
            conn.execute(runs.delete())
            conn.execute(cases.delete())
            conn.execute(users.delete())

    def test_requires_authentication(self):
        self.assertEqual(client.get(f'/evidence/{self.evidence_id}/adjudications').status_code, 401)
        self.assertEqual(client.get(f'/evidence/{self.evidence_id}/released').status_code, 401)

    def test_adjudication_status_endpoint(self):
        response = client.get(f'/evidence/{self.evidence_id}/adjudications', headers=self.headers)
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body['evidence_id'], self.evidence_id)
        self.assertEqual(body['count'], 5)
        states = {row['裁定编号']: row['状态'] for row in body['rulings']}
        self.assertEqual(states['A-15-1'], '整组已放行')
        self.assertEqual(states['A-06-1'], '原裁定失效')
        self.assertEqual(states['A-02-1'], '裁定驳回')

    def test_released_endpoint(self):
        response = client.get(f'/evidence/{self.evidence_id}/released', headers=self.headers)
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body['count'], 3)
        self.assertEqual({row['裁定编号'] for row in body['released']},
                         {'A-15-1', 'A-06-2', 'A-03-2'})

    def test_unknown_evidence_returns_404(self):
        response = client.get('/evidence/999999/adjudications', headers=self.headers)
        self.assertEqual(response.status_code, 404)

    def test_spec_exposes_adjudication_field(self):
        response = client.get('/spec', headers=self.headers)
        labels = response.json()['field_labels']
        self.assertIn('adjudications', labels)
        self.assertIn('ruling_id', labels)


if __name__ == '__main__':
    unittest.main(verbosity=2)
