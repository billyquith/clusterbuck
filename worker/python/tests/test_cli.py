"""Argument parsing.

The surface has to match the .NET worker's, because an operator (and the e2e harness) drives
whichever implementation a node happens to have installed.
"""

from __future__ import annotations

import pytest

from cbk_worker.cli import Out, build_parser


@pytest.fixture()
def parser():
    return build_parser()


@pytest.mark.parametrize("argv", [
    ["enroll", "--token", "T", "--server", "http://a:1"],     # after the verb
    ["--server", "http://a:1", "enroll", "--token", "T"],     # before the verb
])
def test_server_is_accepted_on_either_side_of_the_verb(parser, argv):
    """The .NET parser scans the whole argv, so both orders occur in the wild.

    Regression: argparse originally accepted only the pre-verb form, and every e2e script
    passes it post-verb — `cbk enroll --token … --server …` died with "unrecognized
    arguments".
    """
    args = parser.parse_args(argv)
    assert args.verb == "enroll" and args.server == "http://a:1"


def test_a_verb_without_server_does_not_clobber_a_preceding_one(parser):
    """The argparse trap this guards: a subparser whose --server defaulted to None would
    overwrite the value already parsed from before the verb."""
    args = parser.parse_args(["--server", "http://a:1", "work"])
    assert args.server == "http://a:1"


def test_short_and_long_flags_both_work(parser):
    a = parser.parse_args(["work", "-c", "8b-extract,32b-reason", "--model", "m"])
    assert a.capabilities == "8b-extract,32b-reason" and a.model == "m"
    b = parser.parse_args(["enroll", "-t", "tok"])
    assert b.token == "tok"
    c = parser.parse_args(["submit", "-p", "hello"])
    assert c.prompt == "hello"


def test_all_seven_verbs_parse(parser):
    verbs = {
        "work": ["work"],
        "submit": ["submit", "-p", "x"],
        "status": ["status", "job_1"],
        "fleet": ["fleet"],
        "enroll": ["enroll", "-t", "tok"],
        "pause": ["pause"],
        "resume": ["resume"],
    }
    for verb, argv in verbs.items():
        assert parser.parse_args(argv).verb == verb


def test_no_verb_leaves_verb_unset_so_main_can_print_usage(parser):
    assert parser.parse_args([]).verb is None


def test_submit_requires_a_prompt(parser):
    with pytest.raises(SystemExit):
        parser.parse_args(["submit"])


def test_enroll_requires_a_token(parser):
    with pytest.raises(SystemExit):
        parser.parse_args(["enroll"])


def test_invalid_enum_choices_are_rejected(parser):
    parser.parse_args(["submit", "-p", "x", "--urgency", "urgent"])
    with pytest.raises(SystemExit):
        parser.parse_args(["submit", "-p", "x", "--urgency", "whenever"])
    with pytest.raises(SystemExit):
        parser.parse_args(["submit", "-p", "x", "--privacy", "anywhere"])


def test_table_aligns_columns_and_tolerates_short_rows(capsys):
    Out.table(["a", "bbbb"], [["xxxxx", "y"], ["z"]])
    lines = capsys.readouterr().out.splitlines()
    # Header padded to the widest cell in each column; a short row does not raise.
    assert lines[0].startswith("a    ")
    assert len(lines) == 3
