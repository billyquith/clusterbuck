#!/usr/bin/env python3
"""Validate the wire contract itself: every schema compiles, every fixture matches.

Runs with only `jsonschema` installed, so CI can gate the contract without building either
component. This checks the schemas are internally well-formed and that the shared fixtures
agree with them; the *cross-language* agreement (that each side's own types round-trip
through these schemas) is asserted by the conformance tests in server/tests and
worker/tests, which is where drift between the two implementations would surface.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from jsonschema import Draft202012Validator

HERE = Path(__file__).resolve().parent
EXAMPLES = HERE / "examples"


def main() -> int:
    schemas = sorted(HERE.glob("*.schema.json"))
    if not schemas:
        print("no schemas found", file=sys.stderr)
        return 1

    failures: list[str] = []
    validators: dict[str, Draft202012Validator] = {}

    for path in schemas:
        try:
            schema = json.loads(path.read_text())
            Draft202012Validator.check_schema(schema)
            validators[path.name] = Draft202012Validator(schema)
            print(f"  ok  schema {path.name}")
        except Exception as e:  # noqa: BLE001 - report and continue
            failures.append(f"{path.name}: {e}")
            print(f"FAIL  schema {path.name}: {e}")

    # examples/<stem>.valid.json must validate; .invalid.json must not.
    for fixture in sorted(EXAMPLES.glob("*.json")):
        if ".valid" in fixture.name:
            stem, expect_valid = fixture.name.split(".valid")[0], True
        elif ".invalid" in fixture.name:
            stem, expect_valid = fixture.name.split(".invalid")[0], False
        else:
            continue
        name = f"{stem}.schema.json"
        validator = validators.get(name)
        if validator is None:
            failures.append(f"{fixture.name}: no schema {name}")
            print(f"FAIL  {fixture.name}: no matching schema {name}")
            continue
        errors = list(validator.iter_errors(json.loads(fixture.read_text())))
        if expect_valid and errors:
            failures.append(f"{fixture.name}: expected valid, got {errors[0].message}")
            print(f"FAIL  {fixture.name}: {errors[0].message}")
        elif not expect_valid and not errors:
            failures.append(f"{fixture.name}: expected INVALID but it passed")
            print(f"FAIL  {fixture.name}: expected to be rejected, but it validated")
        else:
            verdict = "valid" if expect_valid else "correctly rejected"
            print(f"  ok  {fixture.name} ({verdict})")

    print()
    if failures:
        print(f"{len(failures)} contract failure(s)")
        return 1
    print(f"contract ok — {len(validators)} schemas, "
          f"{len(list(EXAMPLES.glob('*.json')))} fixtures")
    return 0


if __name__ == "__main__":
    sys.exit(main())
