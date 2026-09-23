"""`install/worker/join.py` — the parts that are wrong only on the platform you skipped.

Loaded by path because the joining script deliberately ships as a standalone file next to
the installers it drives, not as part of an installed package: the machine running it has
nothing installed yet.

The argument-flavour test below exists because the first version of this script passed
GNU-style `--coordinator` to both installers, and `install.ps1` declares PowerShell
parameters (`-CoordinatorUrl`) which will not bind a double-dash name. That failure is
invisible on Linux and macOS and would have surfaced as "missing mandatory parameter" on
the first real Windows run.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
JOIN_PY = REPO / "install" / "worker" / "join.py"


def _load():
    spec = importlib.util.spec_from_file_location("cbk_join", JOIN_PY)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def join():
    assert JOIN_PY.is_file(), f"join script missing at {JOIN_PY}"
    return _load()


def _args(**over):
    base = {
        "coordinator": "http://coordinator.local:8018",
        "redis_url": "redis://:pw@coordinator.local:6379/0",
        "model": "qwen2.5:7b",
        "model_server": "http://127.0.0.1:11434/v1",
        "model_manager": "auto",
        "profile": "dedicated",
        "artifact": Path("/tmp/cbk.pyz"),
        "token": "tok-123",
    }
    base.update(over)
    return base


def test_linux_gets_gnu_style_flags(join):
    cmd = join.installer_command(REPO, system="Linux", **_args())
    assert cmd[0] == "bash" and cmd[1].endswith("install.sh")
    assert "--coordinator" in cmd and "--redis-url" in cmd
    assert "--model-server" in cmd
    assert not any(a.startswith("-Coordinator") for a in cmd)


def test_windows_gets_powershell_parameter_names(join):
    """install.ps1's param block is -CoordinatorUrl / -RedisUrl / -ModelServerUrl. A
    double-dash name does not bind to those, so passing the Linux flavour here fails with
    a missing-mandatory-parameter error on the very first real run."""
    cmd = join.installer_command(REPO, system="Windows", **_args())
    assert cmd[0] == "powershell"
    assert cmd[cmd.index("-File") + 1].endswith("install.ps1")
    for expected in ("-CoordinatorUrl", "-RedisUrl", "-Model", "-ModelServerUrl",
                     "-ModelManager", "-Profile", "-Artifact", "-Token"):
        assert expected in cmd, f"{expected} missing from the Windows invocation"
    assert not any(a.startswith("--") for a in cmd), \
        "no GNU-style flag may reach install.ps1"


def test_every_value_survives_into_both_flavours(join):
    """A name that binds but drops its value is the same outage, quieter."""
    args = _args()
    for system in ("Linux", "Windows"):
        cmd = join.installer_command(REPO, system=system, **args)
        for value in (args["coordinator"], args["redis_url"], args["model"],
                      args["model_server"], args["model_manager"], args["profile"],
                      args["token"], str(args["artifact"])):
            assert value in cmd, f"{value!r} missing on {system}"


def test_capability_warning_names_the_unroutable_tier(join, tmp_path, capsys):
    """The onboarding failure that is otherwise silent: capabilities are proposed from
    probed RAM, so a 32GB+ machine is offered tiers a small fleet never defined — it then
    enrols fine, heartbeats fine, and no job ever routes."""
    state = tmp_path / "node.json"
    state.write_text('{"node_id":"n","node_key":"k","server":"s",'
                     '"capabilities":["8b-extract","32b-reason"]}')

    join.check_capabilities(["8b-extract"], state)

    out = capsys.readouterr().out
    assert "32b-reason" in out
    assert "NOT in the coordinator's registry" in out
    assert "8b-extract" in out, "should say what the coordinator does know"


def test_no_warning_when_every_tier_is_known(join, tmp_path, capsys):
    state = tmp_path / "node.json"
    state.write_text('{"node_id":"n","node_key":"k","server":"s",'
                     '"capabilities":["8b-extract"]}')

    join.check_capabilities(["8b-extract", "32b-reason"], state)

    out = capsys.readouterr().out
    assert "NOT in the coordinator's registry" not in out


def test_a_missing_node_state_does_not_fail_the_join(join, tmp_path, capsys):
    """The check is advisory. A node that installed correctly must not be reported as
    failed just because the state file moved."""
    join.check_capabilities(["8b-extract"], tmp_path / "absent.json")
    assert "skipping the capability check" in capsys.readouterr().out



# --- the Windows wrapper must not become a second copy of the config -------------------

WIN_INSTALLER = REPO / "install/worker/install.ps1"


def _installer_text() -> str:
    return WIN_INSTALLER.read_text(encoding="utf-8")


def test_the_wrapper_reads_worker_env_rather_than_baking_it():
    """It used to inline every variable at install time, which made the wrapper - not
    worker.env - the config the worker actually ran on. Editing the documented file then
    changed nothing, silently: observed live as a node still serving its old model after
    worker.env said otherwise, and it put the broker credential on disk twice."""
    text = _installer_text()
    assert 'for /f "usebackq eol=# tokens=1,* delims==" %%a in ("$EnvFile")' in text, \
        "the wrapper must source worker.env at launch"
    assert '$envLines' not in text, "the baked-variable path is still present"


def test_the_wrapper_keeps_everything_after_the_first_equals():
    """A broker credential can contain `=`. `tokens=1,*` keeps the remainder as the value;
    a plain `tokens=1,2` would silently truncate it."""
    assert "tokens=1,* delims==" in _installer_text()


def test_worker_env_is_written_without_a_bom():
    """PowerShell 5.1's `Set-Content -Encoding UTF8` emits a BOM. Two readers choke on it:
    cmd's for/f folds it into the FIRST variable's NAME, so CBK_REDIS_URL is never set;
    and the worker reads plain UTF-8 and fails with 'Unexpected UTF-8 BOM'. Both seen on a
    live node."""
    text = _installer_text()
    assert "UTF8Encoding($false)" in text, "worker.env must be written BOM-less"
    assert "Set-Content -Path $EnvFile -Encoding UTF8" not in text, \
        "worker.env is still written with the BOM-emitting encoder"


def test_the_profile_reaches_both_installers(join):
    """Without it a dedicated box joins as `shared`, and `cbk pause` then evicts its
    running job and drops its models — giving back a machine nobody wanted back. The
    profile is settable only at enrolment, so getting it wrong here means re-enrolling."""
    for system, flag in (("Linux", "--profile"), ("Windows", "-Profile")):
        cmd = join.installer_command(REPO, system=system, **_args(profile="dedicated"))
        assert flag in cmd, f"{flag} missing on {system}"
        assert cmd[cmd.index(flag) + 1] == "dedicated"


# --- verifying the artifact before it becomes a root service ------------------------


def _staged(tmp_path, body=b"pretend-zipapp"):
    import hashlib
    p = tmp_path / "cbk.pyz"
    p.write_bytes(body)
    return p, hashlib.sha256(body).hexdigest()


def test_a_tampered_artifact_is_refused(tmp_path):
    """The whole point. This file is about to be installed and run as a service, usually
    as root; the only previous check was `st_size != 0`."""
    join = _load()
    art, _ = _staged(tmp_path)

    with pytest.raises(SystemExit):
        join.verify_artifact(art, {"artifact_sha256": "0" * 64}, None)


def test_a_matching_digest_passes(tmp_path):
    join = _load()
    art, digest = _staged(tmp_path)
    join.verify_artifact(art, {"artifact_sha256": digest}, None)  # no raise


def test_an_old_coordinator_without_a_digest_warns_rather_than_failing(tmp_path, capsys):
    """Refusing outright would strand a fleet mid-upgrade, so this is the status quo plus
    a loud warning — not a hard stop."""
    join = _load()
    art, _ = _staged(tmp_path)

    join.verify_artifact(art, {}, None)

    assert "could not be verified" in capsys.readouterr().out


def test_a_digest_alone_is_reported_as_insufficient_when_a_signature_exists(
    tmp_path, capsys
):
    """A digest served over the same connection as the artifact proves nothing against an
    on-path attacker. If the coordinator signed the artifact and the operator did not
    supply a key, say so rather than implying the check was meaningful."""
    join = _load()
    art, digest = _staged(tmp_path)

    join.verify_artifact(
        art, {"artifact_sha256": digest, "artifact_signature": "irrelevant"}, None)

    out = capsys.readouterr().out
    assert "on-path attacker" in out and "--pubkey-file" in out


def test_a_good_signature_verifies_and_a_bad_one_is_refused(tmp_path):
    """End to end against the coordinator's own signer, so the two sides cannot drift on
    the payload they agree to sign."""
    from clusterbuck.signing import generate_keypair, public_pem, sign_bootstrap

    join = _load()
    art, digest = _staged(tmp_path)
    key = generate_keypair()
    cfg = {
        "artifact_sha256": digest,
        "artifact_version": "0.18.0",
        "artifact_signature": sign_bootstrap(key, sha256=digest, version="0.18.0"),
    }

    join.verify_artifact(art, cfg, public_pem(key))  # no raise

    other = generate_keypair()
    with pytest.raises(SystemExit):
        join.verify_artifact(art, cfg, public_pem(other))


def test_a_key_without_a_signature_is_refused_rather_than_silently_downgraded(tmp_path):
    """Asking for signature verification and getting none back is the interesting case:
    it is what an attacker stripping the field looks like."""
    join = _load()
    from clusterbuck.signing import generate_keypair, public_pem

    art, digest = _staged(tmp_path)
    with pytest.raises(SystemExit):
        join.verify_artifact(art, {"artifact_sha256": digest},
                             public_pem(generate_keypair()))


def test_the_pubkey_reaches_the_installer_through_the_environment_not_argv(tmp_path):
    """argv is world-readable through `ps` for the life of the call. The key is not
    secret, but this is the hook the next secret would be bolted onto."""
    join = _load()
    env = join.installer_env("-----BEGIN PUBLIC KEY-----\nx\n-----END PUBLIC KEY-----")
    assert env["CBK_UPDATE_PUBKEY_PEM"].startswith("-----BEGIN PUBLIC KEY-----")
