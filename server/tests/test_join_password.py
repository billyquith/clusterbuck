"""`join.py`'s password prompt — the Ctrl+V trap.

`getpass` reads raw keystrokes on Windows (msvcrt.getwch), so Ctrl+V is delivered as the
control character 0x16 instead of pasting. The prompt accepted that one-character
"password" happily, and it only failed four layers later: a control character makes the
HTTP header unparseable, so the coordinator answered `400 Invalid HTTP request received.`
— an error naming the network and nothing else. A genuinely wrong password is a clean 401.

Loaded by path, like test_join_script.py, because the joining script ships standalone next
to the installers it drives: the machine running it has nothing installed yet.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
JOIN_PY = REPO / "install" / "worker" / "join.py"

CTRL_V = "\x16"


@pytest.fixture()
def join():
    spec = importlib.util.spec_from_file_location("cbk_join_pw", JOIN_PY)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _answers(monkeypatch, join, *values):
    supplied = iter(values)
    monkeypatch.setattr(join.getpass, "getpass", lambda _prompt="": next(supplied))


def _interactive(monkeypatch, join, yes=True):
    monkeypatch.setattr(join.sys, "stdin", type("S", (), {"isatty": lambda self: yes})())


def test_a_keystroke_is_refused_and_the_user_gets_another_go(join, monkeypatch):
    """Re-prompting matters: exiting would mean re-running a join that has already
    exchanged a token and downloaded an artifact."""
    _interactive(monkeypatch, join)
    _answers(monkeypatch, join, CTRL_V, "a-real-password")
    assert join.read_password() == "a-real-password"


def test_the_refusal_explains_the_actual_cause(join, monkeypatch, capsys):
    """The old failure mode sent people to look at the network. The message has to name
    Ctrl+V, or the next person loses the same hour."""
    _interactive(monkeypatch, join)
    _answers(monkeypatch, join, CTRL_V, "fine")
    join.read_password()
    out = capsys.readouterr().out
    assert "Ctrl+V" in out and "RIGHT-CLICK" in out


def test_surrounding_whitespace_is_stripped(join, monkeypatch):
    """Password managers append a newline. Left on, it authenticates as the wrong
    password — a clean 401, which looks exactly like not knowing the secret."""
    _interactive(monkeypatch, join)
    _answers(monkeypatch, join, "  secret" + "\n")
    assert join.read_password() == "secret"


def test_an_empty_answer_is_not_accepted(join, monkeypatch):
    _interactive(monkeypatch, join)
    _answers(monkeypatch, join, "", "  ", "eventually")
    assert join.read_password() == "eventually"


def test_it_gives_up_rather_than_spinning_on_a_pipe(join, monkeypatch):
    """Non-interactive stdin has no second answer to give, so re-prompting would loop on
    EOF instead of failing."""
    _interactive(monkeypatch, join, yes=False)
    _answers(monkeypatch, join, "")
    with pytest.raises(SystemExit):
        join.read_password()


def test_it_stops_after_a_bounded_number_of_attempts(join, monkeypatch):
    """Without a bound a stuck terminal that keeps returning the same control character
    would prompt forever."""
    _interactive(monkeypatch, join)
    _answers(monkeypatch, join, CTRL_V, CTRL_V, CTRL_V)
    with pytest.raises(SystemExit):
        join.read_password(attempts=3)
