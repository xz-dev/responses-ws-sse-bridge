"""Offline fail-closed runner selftests. Runtime calls mocked; no engine contacted."""
import contextlib
import datetime
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('runner', HERE / 'accept.py')
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
SHA = 'a' * 40
IMAGE = 'example.invalid/bridge:' + SHA + '@sha256:' + 'b' * 64


class GateTests(unittest.TestCase):
    def approval(self):
        return {'source_approved': True, 'actual_image_run_authorized': True,
                'source_sha': SHA, 'ci_source_sha': SHA, 'ci_conclusion': 'success',
                'ci_url': 'https://example.invalid/ci/1', 'image': IMAGE,
                'filesystem_comparison_approved': True,
                'deployment_statfs': {'type': 'ef53', 'blocks': 1000000, 'block_size': 4096,
                    'measured_at': datetime.datetime.now(datetime.timezone.utc).isoformat()}}

    def test_ref_rejection_without_runtime(self):
        for image in ('example.invalid/bridge:latest', 'example.invalid/bridge@sha256:' + 'b' * 64,
                      IMAGE.replace(SHA, 'c' * 40)):
            with patch.object(r.subprocess, 'run', side_effect=AssertionError('runtime forbidden')) as call:
                with self.assertRaises(AssertionError):
                    r.execute(image, SHA, '/nonexistent')
                call.assert_not_called()

    def test_approval_and_freshness_rejection_without_runtime(self):
        for field, value in [('source_approved', False), ('actual_image_run_authorized', False),
                             ('ci_source_sha', 'c' * 40), ('ci_conclusion', 'failure'),
                             ('filesystem_comparison_approved', False), ('image', 'wrong')]:
            with tempfile.TemporaryDirectory(dir=HERE) as td:
                approval = self.approval(); approval[field] = value
                path = Path(td) / 'approval.json'; path.write_text(json.dumps(approval))
                with patch.object(r.subprocess, 'run') as call:
                    with self.assertRaises(AssertionError):
                        r.execute(IMAGE, SHA, str(path))
                    call.assert_not_called()
        with tempfile.TemporaryDirectory(dir=HERE) as td:
            approval = self.approval(); approval['deployment_statfs']['measured_at'] = '2000-01-01T00:00:00Z'
            path = Path(td) / 'approval.json'; path.write_text(json.dumps(approval))
            with patch.object(r.subprocess, 'run') as call:
                with self.assertRaises(AssertionError):
                    r.execute(IMAGE, SHA, str(path))
                call.assert_not_called()

    def test_unknown_or_remote_or_rootful_engine_fails_before_image(self):
        for host in ({'security': {'rootless': True}, 'serviceIsRemote': True},
                     {'security': {'rootless': False}, 'serviceIsRemote': False},
                     {'security': {'rootless': True}}):
            with tempfile.TemporaryDirectory(dir=HERE) as td:
                path = Path(td) / 'approval.json'; path.write_text(json.dumps(self.approval()))
                fake = types.SimpleNamespace(returncode=0, stderr='', stdout=json.dumps({'host':host}))
                with patch.object(r, 'ROOT', Path(td)), patch.object(r.subprocess, 'run', return_value=fake) as call, contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(r.execute(IMAGE, SHA, str(path)), 1)
                    self.assertEqual(call.call_count, 1)
                    args = call.call_args
                    self.assertEqual(args.args[0], ['podman', '--remote=false', 'info', '--format=json'])
                    self.assertNotIn('CONTAINER_HOST', args.kwargs['env'])
                receipt = json.loads(next(Path(td).glob('run-*/receipt.json')).read_text())
                self.assertEqual(receipt['status'], 'FAILED_CLOSED')


class WorkloadOracleTests(unittest.TestCase):
    def phases(self):
        return [
            {'phase': 'large_normal', 'clock': 'CLOCK_MONOTONIC', 'monotonic_ns': 2_000_000_000},
            {'phase': 'large_blocked_cancel', 'clock': 'CLOCK_MONOTONIC',
             'plateau_start_ns': 10_000_000_000, 'plateau_end_ns': 12_500_000_000,
             'monotonic_ns': 13_000_000_000, 'source_sent': 5, 'source_total': 64},
            {'phase': 'passed', 'clock': 'CLOCK_MONOTONIC', 'tcp_close_start_ns': 13_100_000_000,
             'tcp_cleanup_limit_ns': 200_000_000, 'tcp_CloseNow_cleanup_ns': 100_000_000,
             'backend_and_selected_reader_joined': True, 'proper_close_tested': False}]

    def sample(self, start, end, size):
        return {'clock': 'CLOCK_MONOTONIC', 'sample_start_ns': start,
                'sample_end_ns': end, 'spool_max_file': size}

    def test_earlier_normal_large_sample_cannot_satisfy_blocked_proof(self):
        earlier_normal = self.sample(1_000_000_000, 1_100_000_000, 1 << 20)
        small_blocked = self.sample(11_000_000_000, 11_100_000_000, 512 << 10)
        for samples in ([earlier_normal], [earlier_normal, small_blocked]):
            with self.assertRaisesRegex(AssertionError, 'inside large blocked plateau'):
                r.verify_blocked_spool(samples, self.phases())

    def test_actual_large_sample_inside_blocked_plateau_passes(self):
        samples = [self.sample(1_000_000_000, 1_100_000_000, 1 << 20),
                   self.sample(11_000_000_000, 11_100_000_000, (1 << 20) + 80)]
        proof = r.verify_blocked_spool(samples, self.phases())
        self.assertEqual(proof['qualifying_samples'], 1)
        self.assertEqual(proof['max_blocked_spool_file'], (1 << 20) + 80)

    def test_boundary_spanning_or_post_cancel_samples_fail(self):
        for sample in (self.sample(9_900_000_000, 10_100_000_000, 1 << 20),
                       self.sample(12_400_000_000, 12_600_000_000, 1 << 20),
                       self.sample(14_000_000_000, 14_100_000_000, 1 << 20)):
            with self.assertRaises(AssertionError):
                r.verify_blocked_spool([sample], self.phases())

    def test_untimestamped_final_event_cannot_supply_phase_proof(self):
        phases = self.phases(); del phases[1]['plateau_start_ns']
        with self.assertRaises(KeyError):
            r.verify_blocked_spool([self.sample(11_000_000_000, 11_100_000_000, 1 << 20)], phases)

    def test_tcp_total_200ms_boundary_and_delayed_total_rejected(self):
        passed = self.phases()[-1]
        for total in (0, 100_000_000, 200_000_000):
            passed['tcp_CloseNow_cleanup_ns'] = total
            r.verify_tcp_cleanup(passed)
        for total in (-1, 200_000_001, 240_000_000, 14_000_000_000):
            passed['tcp_CloseNow_cleanup_ns'] = total
            with self.assertRaisesRegex(AssertionError, 'TCP total cleanup'):
                r.verify_tcp_cleanup(passed)


class KernelCapabilities(unittest.TestCase):
    def test_zero_kernel_sets_required(self):
        names = ('CapInh', 'CapPrm', 'CapEff', 'CapBnd', 'CapAmb')
        zero = '\n'.join(name + ':\t0000000000000000' for name in names)
        self.assertEqual(set(r.verify_kernel_capabilities(zero).values()), {0})
        for name in names:
            with self.assertRaisesRegex(AssertionError, 'retains kernel capabilities'):
                r.verify_kernel_capabilities(zero.replace(name + ':\t0000000000000000', name + ':\t0000000000000001'))
        with self.assertRaisesRegex(AssertionError, 'fields missing'):
            r.verify_kernel_capabilities('CapEff:\t0000000000000000')


if __name__ == '__main__':
    unittest.main()
