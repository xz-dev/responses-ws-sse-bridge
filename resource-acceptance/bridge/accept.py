#!/usr/bin/env python3
"""Future authorized local-only actual-image gate. Never pulls or publishes ports."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import subprocess
import sys
import time
import uuid

os.umask(0o077)
ROOT = Path(__file__).resolve().parent
LIMIT = 256 * 1024**2
# Executed only in the rootless engine user namespace; no persistent global changes.
OBSERVE = r'''
import json,os,stat,sys
pid,root=sys.argv[1:]
s=os.stat(root)
assert stat.S_IMODE(s.st_mode)==0o700 and s.st_uid==65534, f'spool root mode={stat.S_IMODE(s.st_mode):o} uid={s.st_uid}'
files=[]
for name in os.listdir(root):
 p=root+'/'+name
 try: s=os.lstat(p)
 except FileNotFoundError: continue
 assert stat.S_ISREG(s.st_mode) and stat.S_IMODE(s.st_mode)==0o600 and s.st_uid==65534
 files.append(s.st_size)
fds=os.listdir('/proc/'+pid+'/fd');opened=[]
for fd in fds:
 p='/proc/'+pid+'/fd/'+fd
 try:
  link=os.readlink(p)
  if link.startswith('/spool/'):
   s=os.stat(p);assert stat.S_IMODE(s.st_mode)==0o600;opened.append(s.st_size)
 except FileNotFoundError: pass
print(json.dumps({'fd_count':len(fds),'spool_files':len(files),'spool_bytes':sum(files),'spool_max_file':max(files+opened+[0]),'open_spool_fds':len(opened),'open_spool_bytes':sum(opened)}))
'''


def verify_blocked_spool(samples, phases):
    blocked = next(p for p in phases if p['phase'] == 'large_blocked_cancel')
    passed = next(p for p in phases if p['phase'] == 'passed')
    assert blocked['clock'] == passed['clock'] == 'CLOCK_MONOTONIC'
    start, end = blocked['plateau_start_ns'], blocked['plateau_end_ns']
    assert 0 < start < end <= blocked['monotonic_ns'] <= passed['tcp_close_start_ns']
    assert end - start >= 2_000_000_000, 'blocked plateau shorter than two seconds'
    assert 0 < blocked['source_sent'] < blocked['source_total'], 'blocked source must be unfinished'
    qualifying = [s for s in samples if s['clock'] == 'CLOCK_MONOTONIC'
                  and start <= s['sample_start_ns'] <= s['sample_end_ns'] <= end
                  and s['spool_max_file'] >= 1 << 20]
    assert qualifying, 'no actual app >=1MiB spool sample entirely inside large blocked plateau before cancellation'
    return {'plateau_start_ns': start, 'plateau_end_ns': end,
            'qualifying_samples': len(qualifying),
            'max_blocked_spool_file': max(s['spool_max_file'] for s in qualifying)}


def verify_tcp_cleanup(passed):
    assert passed['tcp_cleanup_limit_ns'] == 200_000_000
    assert 0 <= passed['tcp_CloseNow_cleanup_ns'] <= 200_000_000, 'TCP total cleanup exceeded 200ms'
    assert passed['backend_and_selected_reader_joined'] is True
    assert passed['proper_close_tested'] is False


def verify_kernel_capabilities(status):
    required = {'CapInh', 'CapPrm', 'CapEff', 'CapBnd', 'CapAmb'}
    fields = dict(line.split(':', 1) for line in status.splitlines() if ':' in line)
    assert required <= fields.keys(), 'kernel capability fields missing'
    caps = {key: int(fields[key].strip(), 16) for key in required}
    assert not any(caps.values()), 'application retains kernel capabilities'
    return caps


def execute(image, sha, approval_path):
    assert re.fullmatch(r'[0-9a-f]{40}', sha), 'expected full source SHA required'
    m = re.fullmatch(r'([^@\s]+):([0-9a-f]{40})@sha256:([0-9a-f]{64})', image)
    assert m and m[2] == sha, 'fullSHA tag@sha256digest required; no floating/default ref'
    approval = json.loads(Path(approval_path).read_text())
    assert approval['source_approved'] is True and approval['actual_image_run_authorized'] is True
    assert approval['source_sha'] == sha and approval['ci_source_sha'] == sha
    assert approval['ci_conclusion'] == 'success' and approval['ci_url'].startswith('https://')
    assert approval['image'] == image
    target = approval['deployment_statfs']
    measured = datetime.datetime.fromisoformat(target['measured_at'].replace('Z', '+00:00'))
    age = (datetime.datetime.now(datetime.timezone.utc) - measured).total_seconds()
    assert 0 <= age < 86400 and target['blocks'] > 0
    assert approval['filesystem_comparison_approved'] is True
    env = dict(os.environ)
    for key in ('CONTAINER_HOST', 'CONTAINER_CONNECTION', 'DOCKER_HOST', 'DOCKER_CONTEXT', 'PODMAN_CONNECTIONS_CONF'):
        env.pop(key, None)
    run_id = uuid.uuid4().hex
    label = 'bridge.acceptance.run='
    app, driver, volume = ['bridge-accept-' + x + '-' + run_id for x in ('app', 'driver', 'spool')]
    out = ROOT / ('run-' + run_id)
    out.mkdir(mode=0o700)
    receipt = {'status': 'RUNNING', 'image': image, 'source_sha': sha,
               'nonce': run_id, 'samples': [], 'phases': [], 'deployment_statfs': target,
               'approval_sha256': hashlib.sha256(Path(approval_path).read_bytes()).hexdigest(),
               'ci': {'source_sha': approval['ci_source_sha'], 'conclusion': approval['ci_conclusion'], 'url': approval['ci_url']},
               'proper_close_tested': False, 'internal_credits_observable': False}
    created = []
    volume_created = False
    process = None
    log = None

    def cmd(*args, timeout=20):
        p = subprocess.run(['podman', '--remote=false', *args], env=env, capture_output=True, text=True, timeout=timeout)
        if p.returncode:
            # Keep errors protected; do not include inventory/auth in compact stdout.
            (out / 'command-error.txt').write_text(p.stderr)
            raise RuntimeError('local podman command failed: ' + args[0])
        return p.stdout

    def inspect(name):
        return json.loads(cmd('container', 'inspect', name))[0]

    def owned(name):
        return inspect(name)['Config']['Labels'].get(label[:-1]) == run_id

    def capture(cgroup, pid, spool):
        values = {'clock': 'CLOCK_MONOTONIC', 'sample_start_ns': time.monotonic_ns()}
        for key in ('memory.current', 'memory.peak', 'memory.max', 'memory.swap.max', 'pids.current', 'pids.max', 'cpu.max'):
            t = (cgroup / key).read_text().strip()
            values[key] = int(t) if t.isdecimal() else t
        values['memory.events'] = dict((k, int(v)) for k, v in (line.split() for line in (cgroup / 'memory.events').read_text().splitlines()))
        assert values['memory.max'] == LIMIT and values['memory.swap.max'] == 0
        assert values['pids.max'] == 64 and values['pids.current'] <= 64
        quota, period = map(int, values['cpu.max'].split())
        assert quota / period == 0.5
        assert values['memory.current'] <= LIMIT and values['memory.peak'] <= LIMIT
        values.update(json.loads(cmd('unshare', 'python3', '-c', OBSERVE, str(pid), spool)))
        assert values['spool_bytes'] <= 8 * 1024**2 and values['open_spool_bytes'] <= 8 * 1024**2
        assert values['spool_files'] <= 140 and values['open_spool_fds'] <= 140
        values['sample_end_ns'] = time.monotonic_ns()
        values['monotonic'] = values['sample_end_ns'] / 1_000_000_000
        receipt['samples'].append(values)
        return values

    try:
        info = json.loads(cmd('info', '--format=json'))
        assert info['host']['security']['rootless'] is True
        assert info['host'].get('serviceIsRemote') is False, 'local engine not proven'
        receipt['engine'] = {'rootless': True, 'remote': False, 'version': info['version']['Version']}
        identity = json.loads(cmd('image', 'inspect', image))[0]
        assert identity['Labels']['org.opencontainers.image.revision'] == sha
        repo_digest = m[1] + '@sha256:' + m[3]
        assert repo_digest in identity['RepoDigests'], 'RepoDigest mismatch'
        engine_id = identity['Id']
        assert re.fullmatch(r'(sha256:)?[0-9a-f]{64}', engine_id)
        receipt['identity'] = {'engineImageID': engine_id, 'RepoDigest': repo_digest,
                               'labels': {k: identity['Labels'][k] for k in ('org.opencontainers.image.revision', 'org.opencontainers.image.source', 'org.opencontainers.image.version') if k in identity['Labels']}}
        receipt['helper_sha256'] = hashlib.sha256((ROOT / 'probe').read_bytes()).hexdigest()
        for name in (app, driver):
            p = subprocess.run(['podman', '--remote=false', 'container', 'exists', name], env=env, capture_output=True, timeout=10)
            assert p.returncode == 1, 'refuse existing/unknown container'
        p = subprocess.run(['podman', '--remote=false', 'volume', 'exists', volume], env=env, capture_output=True, timeout=10)
        assert p.returncode == 1, 'refuse existing/unknown volume'
        cmd('volume', 'create', '--label', label + run_id, volume)
        volume_created = True
        vi = json.loads(cmd('volume', 'inspect', volume))[0]
        assert vi['Labels'][label[:-1]] == run_id and vi['Driver'] == 'local'
        assert not vi.get('Options'), 'unexpected volume driver options'
        spool = vi['Mountpoint']
        # Only this freshly nonce-labeled volume is changed; never system/global ownership.
        cmd('unshare', 'chown', '65534:65534', spool)
        cmd('unshare', 'chmod', '0700', spool)
        fs_type, blocks, free, block_size = cmd('unshare', 'stat', '-f', '-c', '%t %b %f %S', spool).split()
        fs = {'type': fs_type, 'blocks': int(blocks), 'free_blocks': int(free), 'block_size': int(block_size)}
        receipt['test_volume_statfs'] = fs
        assert fs['type'] == target['type'] and fs['block_size'] == target['block_size'], 'filesystem mismatch'
        assert fs['free_blocks'] * fs['block_size'] >= 512 * 1024**2, 'insufficient test volume headroom'
        assert fs_type != '1021994', 'tmpfs is not target-volume proof'
        common = ['--pull=never', '--label', label + run_id, '--read-only', '--cap-drop=ALL', '--security-opt=no-new-privileges', '--user=65534:65534', '--pids-limit=64']
        cmd('run', '--detach', '--name', app, *common,
            '--network=none', '--memory=256m', '--memory-swap=256m', '--cpus=0.5', '--stop-timeout=10',
            '--volume', volume + ':/spool:rw,nocopy',
            '-e', 'UPSTREAM_URL=http://127.0.0.1:9001', '-e', 'LISTEN_ADDR=127.0.0.1:8080',
            '-e', 'SPOOL_DIR=/spool', '-e', 'GOMEMLIMIT=192MiB',
            '-e', 'MAX_MESSAGE_BYTES=16777216', '-e', 'MAX_BODY_BYTES=16777216', '-e', 'WORK_MAX_BYTES=268435456', image)
        created.append(app)
        for _ in range(30):
            p = subprocess.run(['podman', '--remote=false', 'exec', app, '/responses-ws-sse-bridge', 'healthcheck'], env=env, capture_output=True, timeout=5)
            if p.returncode == 0:
                break
            time.sleep(.2)
        else:
            raise RuntimeError('health never ready')
        initial = inspect(app)
        cfg = initial['HostConfig']
        (out / 'app-initial.json').write_text(json.dumps(initial, indent=2))
        assert initial['Image'] == engine_id
        receipt['identity']['containerImageID'] = initial['Image']
        assert initial['Config']['Labels']['org.opencontainers.image.revision'] == sha
        assert cfg['Memory'] == LIMIT and cfg['MemorySwap'] == LIMIT
        assert cfg['PidsLimit'] == 64 and cfg['ReadonlyRootfs'] and cfg['NetworkMode'] == 'none'
        assert cfg['NanoCpus'] == 500000000 or cfg['CpuQuota'] / cfg['CpuPeriod'] == .5
        assert initial['Config']['User'] == '65534:65534'
        assert any(x.split('=')[0] == 'no-new-privileges' for x in cfg['SecurityOpt'])
        # Podman expands ALL into capability names; prove the resulting kernel state.
        assert cfg.get('CapDrop') and not cfg.get('CapAdd'), 'capability drop configuration missing'
        receipt['kernel_capabilities'] = verify_kernel_capabilities(
            cmd('unshare', 'cat', f"/proc/{initial['State']['Pid']}/status"))
        assert not cfg.get('PortBindings') and not any(initial['NetworkSettings'].get('Ports', {}).values())
        assert initial['Config']['Env'].count('GOMEMLIMIT=192MiB') == 1
        assert not cfg.get('Tmpfs'), 'service tmpfs not allowed for this proof'
        mounts = [v for v in initial['Mounts'] if v['Destination'] == '/spool']
        assert len(mounts) == 1 and mounts[0]['Type'] == 'volume' and mounts[0]['Name'] == volume
        pid = initial['State']['Pid']
        relative = next(x.split(':', 2)[2] for x in Path(f'/proc/{pid}/cgroup').read_text().splitlines() if x.startswith('0::'))
        cgroup = Path('/sys/fs/cgroup') / relative.lstrip('/')
        receipt['resource_config'] = {k: cfg.get(k) for k in ('Memory','MemorySwap','PidsLimit','NanoCpus','CpuQuota','CpuPeriod','ReadonlyRootfs','CapDrop','SecurityOpt','NetworkMode','PortBindings','Tmpfs')}
        receipt['resource_config'].update({'User': initial['Config']['User'], 'GOMEMLIMIT': '192MiB', 'MAX_MESSAGE_BYTES': 16777216, 'MAX_BODY_BYTES': 16777216, 'WORK_MAX_BYTES': LIMIT, 'spool_mount_type': 'volume'})
        before = capture(cgroup, pid, spool)
        log = (out / 'driver-stderr.log').open('w')
        # Fixture/driver shares only isolated netns; separate cgroup, same immutable runtime image, helper entrypoint.
        process = subprocess.Popen(['podman', '--remote=false', 'run', '--name', driver, *common,
            '--network=container:' + app, '--memory=192m', '--memory-swap=192m',
            '-e', 'GOMEMLIMIT=128MiB', '-e', 'GOMAXPROCS=2', '-e', 'BRIDGE_ACCEPTANCE_NETNS=1',
            '--volume', str(ROOT / 'probe') + ':/probe:ro', '--entrypoint=/probe', image],
            env=env, stdout=subprocess.PIPE, stderr=log, text=True)
        created.append(driver)
        # Prove driver separate cgroup and shared isolated netns while alive.
        for _ in range(50):
            exists = subprocess.run(['podman', '--remote=false', 'container', 'exists', driver], env=env, capture_output=True, timeout=10)
            assert exists.returncode in (0, 1), 'driver existence unknown'
            ds = inspect(driver) if exists.returncode == 0 else None
            if ds and ds['State']['Running']:
                break
            if process.poll() is not None:
                raise RuntimeError('driver exited before isolation proof')
            time.sleep(.1)
        else:
            raise RuntimeError('driver not running for isolation proof')
        dpid = ds['State']['Pid']
        drelative = next(x.split(':', 2)[2] for x in Path(f'/proc/{dpid}/cgroup').read_text().splitlines() if x.startswith('0::'))
        assert drelative != relative, 'driver must not share app cgroup'
        app_netns = cmd('unshare', 'readlink', f'/proc/{pid}/ns/net').strip()
        assert app_netns == cmd('unshare', 'readlink', f'/proc/{dpid}/ns/net').strip()
        assert app_netns != os.readlink('/proc/self/ns/net'), 'host netns forbidden'
        app_time = cmd('unshare', 'readlink', f'/proc/{pid}/ns/time').strip()
        assert app_time == cmd('unshare', 'readlink', f'/proc/{dpid}/ns/time').strip()
        assert app_time == os.readlink('/proc/self/ns/time'), 'shared CLOCK_MONOTONIC time namespace not proven'
        receipt['isolation'] = {'distinct_app_driver_cgroups': True, 'shared_private_netns': True, 'host_netns': False, 'shared_host_time_namespace': True}
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        deadline = time.monotonic() + 260
        next_sample = 0
        while time.monotonic() < deadline:
            if time.monotonic() >= next_sample:
                sample = capture(cgroup, pid, spool)
                for k in ('oom','oom_kill','oom_group_kill'):
                    assert sample['memory.events'].get(k,0) == before['memory.events'].get(k,0)
                live = inspect(app)
                assert live['State']['Running'] and not live['State']['OOMKilled'] and live['RestartCount'] == 0
                next_sample = time.monotonic() + .25
            if selector.select(.1):
                line = process.stdout.readline()
                if not line:
                    break
                receipt['phases'].append(json.loads(line))
            if process.poll() is not None:
                for line in process.stdout:
                    receipt['phases'].append(json.loads(line))
                break
        assert process.wait(timeout=5) == 0, 'driver failed/deadline exceeded'
        driver_state = inspect(driver)
        assert driver_state['Image'] == engine_id and not driver_state['State']['OOMKilled'] and driver_state['RestartCount'] == 0
        expected = ['65_completed_open','large_normal','blocked_hold_start','blocked_hold_complete','drained','large_blocked_cancel','passed']
        assert [p['phase'] for p in receipt['phases']] == expected, 'incomplete workload oracle'
        assert receipt['phases'][4]['delta_bytes_verified'] > 16777216
        assert receipt['phases'][3]['stable_seconds'] > 30
        receipt['blocked_large_spool_proof'] = verify_blocked_spool(receipt['samples'], receipt['phases'])
        verify_tcp_cleanup(receipt['phases'][-1])
        receipt['after'] = capture(cgroup, pid, spool)
        for k in ('oom', 'oom_kill', 'oom_group_kill'):
            assert receipt['after']['memory.events'].get(k, 0) == before['memory.events'].get(k, 0)
        cmd('exec', app, '/responses-ws-sse-bridge', 'healthcheck')
        started = time.monotonic()
        cmd('stop', '--time=10', app)
        stopped = inspect(app)
        receipt['stop_seconds'] = time.monotonic() - started
        assert receipt['stop_seconds'] < 12 and stopped['State']['ExitCode'] == 0
        assert not stopped['State']['OOMKilled'] and stopped['RestartCount'] == 0
        receipt['status'] = 'WORKLOAD_PASSED_NOT_DEPLOYMENT_ACCEPTANCE'
        receipt['remaining_gate'] = 'Parent independently compares fresh deployment volume Statfs (including blocks/headroom), identity, exact CI, and this receipt before accepting.'
    except Exception as exc:
        import traceback
        receipt['status'] = 'FAILED_CLOSED'
        receipt['error'] = str(exc)
        receipt['error_traceback'] = traceback.format_exc()
    finally:
        if log:
            log.close()
        # Preserve own logs and recovery names before attempting cleanup; never broad prune/rm.
        receipt['recovery'] = {'app': app, 'driver': driver, 'volume': volume, 'label': label + run_id}
        for name in reversed(created):
            try:
                assert owned(name), 'cleanup ownership mismatch'
                logs = subprocess.run(['podman', '--remote=false', 'logs', name], env=env, capture_output=True, text=True, timeout=20)
                (out / (name.split('-')[2] + '-logs.txt')).write_text(logs.stdout + logs.stderr)
                assert logs.returncode == 0, 'owned-container logs unavailable'
                state = inspect(name)
                (out / (name.split('-')[2] + '-state.json')).write_text(json.dumps({'Image':state['Image'],'State':state['State'],'RestartCount':state['RestartCount']},indent=2))
                cmd('rm', '--force', name)
            except Exception as exc:
                receipt.setdefault('cleanup_errors', []).append(str(exc))
        if process and process.poll() is None:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait(timeout=5)
        if volume_created:
            try:
                vi = json.loads(cmd('volume', 'inspect', volume))[0]
                assert vi['Labels'][label[:-1]] == run_id
                cmd('volume', 'rm', volume)
            except Exception as exc:
                receipt.setdefault('cleanup_errors', []).append(str(exc))
        if receipt.get('cleanup_errors'):
            receipt['status'] = 'FAILED_CLOSED'
        (out / 'receipt.json').write_text(json.dumps(receipt, indent=2))
        print(json.dumps({'status':receipt['status'],'receipt':str(out / 'receipt.json')}))
    return 0 if receipt['status'] == 'WORKLOAD_PASSED_NOT_DEPLOYMENT_ACCEPTANCE' else 1


if __name__ == '__main__':
    if len(sys.argv) != 4:
        sys.exit('usage: accept.py REGISTRY/IMAGE:FULL_SOURCE_SHA@sha256:DIGEST FULL_SOURCE_SHA APPROVAL.json (future authorized run only)')
    try:
        sys.exit(execute(*sys.argv[1:]))
    except Exception as exc:
        print(json.dumps({'status':'FAILED_CLOSED_PREFLIGHT','error':str(exc)}))
        sys.exit(1)
