# clusterbuck

A **LAN LLM worker network**. Submit inference jobs from any tool on the home
network; clusterbuck runs them on whichever machine is capable and available —
routing live requests now, or queuing patient work until a worker is contactable
(waking one if worth it). *(Name: "pass the buck" — the broker hands each job to
whichever worker is up.)*

Domain-agnostic infrastructure: clusterbuck only ever sees jobs, capabilities, and
results — never anything about the client applications that use it.

**Status:** design only. See [DESIGN.md](DESIGN.md) for the overview, and
[`docs/`](docs/) for detail — [architecture](docs/architecture.md),
[protocols](docs/protocols.md), [deployment](docs/deployment.md),
[decisions](docs/decisions.md).

## License

Licensed under the Apache License, Version 2.0. Copyright 2026 Nick Trout.
See [LICENSE](LICENSE) and [NOTICE](NOTICE).
