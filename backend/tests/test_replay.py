"""End-to-end checks for the stall-sign replay engine with upstream rulings."""
import json
import os
import sys
import unittest

from sqlalchemy import create_engine, event

sys.path.insert(0, '/workspace/backend')

from src.services import domain, marquee_store as store  # noqa: E402

ENGINE = create_engine(os.environ['GB5310_TEST_MYSQL_URL'], pool_pre_ping=True)


class ReplayTests(unittest.TestCase):
    def run_sample(self):
        return domain.solve(domain.SPEC['sample'], ENGINE)

    def test_sample_metrics(self):
        report = self.run_sample()
        self.assertEqual(report['metrics'], domain.SPEC['expect'])

    def test_ruling_states(self):
        report = self.run_sample()
        rulings = {row['裁定编号']: row for row in report['sections'][2]['rows']}
        self.assertEqual(rulings['A-15-1']['状态'], '整组已放行')
        self.assertEqual(rulings['A-06-1']['状态'], '原裁定失效')
        self.assertEqual(rulings['A-06-2']['状态'], '整组已放行')
        self.assertEqual(rulings['A-02-1']['状态'], '裁定驳回')
        self.assertEqual(rulings['A-03-2']['状态'], '整组已放行')
        self.assertEqual(len(report['sections'][3]['rows']), 3)

    def test_final_projection_and_offsets(self):
        report = self.run_sample()
        projection = {(row['分区流'], row['牌面编号']): row['当前内容']
                      for row in report['sections'][0]['rows']}
        self.assertEqual(projection[('NORTH', 'N-07')], '桂花米酿｜18元')
        self.assertEqual(projection[('NORTH', 'N-09')], '红糖糍粑｜15元')
        self.assertEqual(projection[('SOUTH', 'S-02')], '烤梨｜13元')
        self.assertEqual(projection[('WEST', 'W-04')], '糖炒栗子｜12元')
        offsets = {row['分区流']: row for row in report['sections'][1]['rows']}
        self.assertEqual(offsets['NORTH']['消费位点'], 16)
        self.assertEqual(offsets['SOUTH']['消费位点'], 7)
        self.assertEqual(offsets['WEST']['消费位点'], 3)
        for row in offsets.values():
            self.assertEqual(row['续放状态'], '已追平')

    def test_identical_redelivery_ignored(self):
        report = self.run_sample()
        rows = [row for row in report['sections'][5]['rows'] if row['状态'] == '重投忽略']
        self.assertEqual(len(rows), 3)

    def test_held_ruling_matching_takes_effect(self):
        payload = json.loads(json.dumps(domain.SPEC['sample']))
        # The real seq-6 arrives in the same batch as the held ruling and matches
        # its envelope (14 元): held -> effective -> released in one batch.
        # Seq 7 is repointed at another sign so it cannot overwrite S-02.
        payload['events'] = [row for row in payload['events'] if not (
            row['batch'] == 5 and row['sequence'] == 6)]
        for row in payload['events']:
            if row['batch'] == 2 and row['stream'] == 'SOUTH' and row['sequence'] == 7:
                row['sign_id'] = 'S-09'
                row['content'] = '临时摊位｜撤牌'
        payload['events'].append(dict(batch=4, stream='SOUTH', sequence=6, event_id='E-6',
                                      sign_id='S-02', operation='put', content='烤梨｜14元'))
        payload['adjudications'] = [row for row in payload['adjudications']
                                    if row['ruling_id'] != 'A-06-2']
        report = domain.solve(payload, ENGINE)
        rulings = {row['裁定编号']: row for row in report['sections'][2]['rows']}
        self.assertEqual(rulings['A-06-1']['状态'], '整组已放行')
        projection = {(row['分区流'], row['牌面编号']): row['当前内容']
                      for row in report['sections'][0]['rows']}
        self.assertEqual(projection[('SOUTH', 'S-02')], '烤梨｜14元')
        # Seq 7 arrives after the ruled seq 6, so SOUTH catches up to 7.
        offsets = {row['分区流']: row for row in report['sections'][1]['rows']}
        self.assertEqual(offsets['SOUTH']['消费位点'], 7)

    def test_selected_receipt_cannot_clash_again(self):
        # A third N-09 version after A-15-1: the pinned receipts stay untouched.
        payload = json.loads(json.dumps(domain.SPEC['sample']))
        payload['events'].append(dict(batch=7, stream='NORTH', sequence=15, event_id='E-15',
                                      sign_id='N-09', operation='put', content='红糖糍粑｜99元'))
        report = domain.solve(payload, ENGINE)
        quarantined = [row for row in report['sections'][5]['rows']
                       if row['状态'] == '矛盾隔离' and row['分区流'] == 'NORTH']
        self.assertTrue(any('已被裁定' in row['依据'] for row in quarantined))
        projection = {(row['分区流'], row['牌面编号']): row['当前内容']
                      for row in report['sections'][0]['rows']}
        self.assertEqual(projection[('NORTH', 'N-09')], '红糖糍粑｜15元')

    def test_later_ruling_never_rewrites_submitted(self):
        report = self.run_sample()
        rulings = {row['裁定编号']: row for row in report['sections'][2]['rows']}
        self.assertIn('已提交', rulings['A-02-1']['依据'])

    def test_retry_is_idempotent(self):
        first = self.run_sample()
        second = self.run_sample()
        self.assertEqual(first['metrics'], second['metrics'])
        self.assertEqual(first['sections'], second['sections'])

    def test_validation_rejects_bad_ruling(self):
        payload = json.loads(json.dumps(domain.SPEC['sample']))
        payload['adjudications'][0]['ruling_id'] = 'bad id!'
        with self.assertRaises(ValueError):
            domain.validate(payload)
        payload = json.loads(json.dumps(domain.SPEC['sample']))
        payload['adjudications'][1]['selected_receipts'] = []
        payload['adjudications'][1]['content'] = None
        with self.assertRaises(ValueError):
            domain.validate(payload)

    def test_group_closure_by_event_id(self):        # Two conflicting versions sharing only an event id must be ruled as one group.
        payload = json.loads(json.dumps(domain.SPEC['sample']))
        payload['events'] = [row for row in payload['events'] if row['batch'] != 6
                             or row['stream'] != 'WEST']
        payload['events'].extend([
            dict(batch=6, stream='WEST', sequence=3, event_id='E-3', sign_id='W-05',
                 operation='put', content='山楂糕｜8元'),
            dict(batch=6, stream='WEST', sequence=4, event_id='E-X', sign_id='W-09',
                 operation='put', content='甲版本'),
            dict(batch=6, stream='WEST', sequence=5, event_id='E-X', sign_id='W-09',
                 operation='put', content='乙版本'),
        ])
        payload['adjudications'] = [
            row for row in payload['adjudications'] if row['ruling_id'] not in ('A-02-1', 'A-03-2')]
        payload['adjudications'].append(dict(batch=6, ruling_id='A-X-1', stream='WEST',
                                             sequence=4, event_id='E-X',
                                             selected_receipts=[16]))
        report = domain.solve(payload, ENGINE)
        rulings = {row['裁定编号']: row for row in report['sections'][2]['rows']}
        self.assertEqual(rulings['A-X-1']['状态'], '整组已放行')
        # Closure pins both receipts sharing E-X, even though only one was selected.
        pinned = {row['接收行']: row for row in report['sections'][5]['rows']
                  if row.get('裁定编号') == 'A-X-1'}
        self.assertEqual(set(pinned), {16, 17})
        self.assertTrue(all(row['状态'] == '已提交' for row in pinned.values()))
        offsets = {row['分区流']: row for row in report['sections'][1]['rows']}
        self.assertEqual(offsets['WEST']['消费位点'], 5)

    def test_rollback_keeps_previous_batch_and_retry_releases_once(self):
        payload = domain.SPEC['sample']
        original = store.advance_offset
        calls = {'n': 0}

        def flaky_advance(conn, stream, seq):
            calls['n'] += 1
            if calls['n'] == 1:
                raise RuntimeError('injected failure before commit')
            return original(conn, stream, seq)

        store.advance_offset = flaky_advance
        try:
            with self.assertRaises(RuntimeError):
                domain.solve(payload, ENGINE)
        finally:
            store.advance_offset = original
        # The failed run only used temp tables; a clean retry reaches the same
        # result and releases every group exactly once.
        report = domain.solve(payload, ENGINE)
        released = report['sections'][3]['rows']
        self.assertEqual(len(released), 3)
        self.assertEqual([row['状态'] for row in released], ['整组已放行'] * 3)
        self.assertEqual(report['metrics'], domain.SPEC['expect'])

    def test_concurrent_arrivals_in_one_batch(self):
        # Two different envelopes for the same seq arrive in the same batch:
        # registration happens before consumption so both are quarantined.
        payload = json.loads(json.dumps(domain.SPEC['sample']))
        payload['events'] = [row for row in payload['events'] if row['batch'] != 6
                             or row['stream'] != 'WEST']
        payload['events'].extend([
            dict(batch=6, stream='WEST', sequence=3, event_id='E-3', sign_id='W-04',
                 operation='put', content='糖炒栗子｜12元'),
            dict(batch=6, stream='WEST', sequence=3, event_id='E-3', sign_id='W-04',
                 operation='put', content='糖炒栗子｜10元'),
        ])
        payload['adjudications'] = [
            row for row in payload['adjudications'] if row['ruling_id'] not in ('A-02-1', 'A-03-2')]
        report = domain.solve(payload, ENGINE)
        conflicts = [row for row in report['sections'][5]['rows']
                     if row['分区流'] == 'WEST' and row['序号'] == 3]
        self.assertEqual(len(conflicts), 2)
        self.assertTrue(all(row['状态'] == '矛盾隔离' for row in conflicts))
        offsets = {row['分区流']: row for row in report['sections'][1]['rows']}
        self.assertEqual(offsets['WEST']['续放状态'], '矛盾待核')

    def test_ruling_query_sections_are_complete(self):
        report = self.run_sample()
        titles = [section['title'] for section in report['sections']]
        self.assertIn('裁定状态', titles)
        self.assertIn('已整组放行记录', titles)
        status_section = next(s for s in report['sections'] if s['title'] == '裁定状态')
        labels = {row['状态'] for row in status_section['rows']}
        self.assertIn('整组已放行', labels)
        self.assertIn('原裁定失效', labels)
        self.assertIn('裁定驳回', labels)
        # Locked releases always carry their ruling id in the journal; the
        # confirmation-only release (lapsed-gap path) is exempt because the
        # receipt landed before the ruling was registered.
        journal_rulings = {row['裁定编号'] for row in report['sections'][5]['rows']
                           if row['裁定编号']}
        self.assertIn('A-15-1', journal_rulings)
        self.assertIn('A-03-2', journal_rulings)


if __name__ == '__main__':
    unittest.main(verbosity=2)
