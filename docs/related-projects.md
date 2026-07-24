# Related projects

A living survey of projects in the same neighbourhood as clusterbuck — inference
engines, gateways, and distributed-inference systems — with a note on how each relates
and why it doesn't, on its own, serve clusterbuck's purpose. This is a **collaborative,
work-in-progress** document; add entries as we find them (template at the bottom). Links
point at each project's canonical repo or site; verify version-specific claims before
relying on them.

## The yardstick

clusterbuck's niche is narrow and specific. A project is "suitable as-is" only if it
covers **all** of this:

1. **Intermittent nodes** — machines that sleep, roam, or leave the LAN, treated as normal.
2. **Heterogeneous hardware** — a Pi, a Mac, an NVIDIA box, mixed RAM/GPU, all welcome.
3. **Sleep/wake awareness** — schedule wake windows and/or **Wake-on-LAN** on demand.
4. **Patient queuing** — work waits in a durable queue for a capable machine, rather than
   failing over immediately; per-job patience policy.
5. **Coarse-grained, per-job routing** — send a *job* to whichever whole machine is
   capable and up; **not** sharding one model across nodes.
6. **Model-server-agnostic** — treat the engine (Ollama/vLLM/llama.cpp/…) as swappable.
7. **LAN-first, lightweight** — no always-on cluster or datacenter interconnect assumed.

Most projects fail (1)/(3)/(4): they assume nodes are **always on** during inference.
Several others (exo, Petals, llama.cpp RPC) do the opposite of (5) — they split a *single*
model across nodes, which *requires* those nodes to be up simultaneously.

## Summary

| Project | Category | Relative to clusterbuck | Suitable as-is? | One-line reason |
|---|---|---|---|---|
| [llama.cpp](https://github.com/ggml-org/llama.cpp) | Engine | Below (a model server) | Component | Single-node engine; no cross-machine orchestration |
| [Ollama](https://github.com/ollama/ollama) | Engine | Below | Component | Single-node engine + model mgmt; no fleet/queue/wake |
| [LM Studio](https://lmstudio.ai) | Engine | Below | Component | Single-node desktop engine w/ OpenAI server |
| [vLLM](https://github.com/vllm-project/vllm) | Engine | Below | Component | High-throughput single-node/GPU-cluster serving; always-on, CUDA-first |
| [HF TGI](https://github.com/huggingface/text-generation-inference) | Engine | Below | Component | Production single-node/replica serving; always-on |
| [NVIDIA Triton](https://github.com/triton-inference-server/server) | Engine | Below | Component | Datacenter serving; always-on, heavyweight |
| [MLX](https://github.com/ml-explore/mlx) / [mlx-lm](https://github.com/ml-explore/mlx-lm) | Engine | Below | Component | Apple-Silicon engine; single-node |
| [LocalAI](https://github.com/mudler/LocalAI) | Engine/aggregator | Below/beside | Component | Single-host OpenAI drop-in over many backends; no intermittent fleet |
| [LiteLLM](https://github.com/BerriAI/litellm) | Gateway | Beside (adopted) | **Adopted** | Sync routing/fallback; no durable queue or wake |
| [OpenRouter](https://openrouter.ai) | Cloud gateway | Beside | No | Hosted commercial routing; not local/LAN |
| [exo](https://github.com/exo-explore/exo) | Distributed | Overlaps | No | Shards one model across devices; needs them online together; no queue/wake |
| [Petals](https://github.com/bigscience-workshop/petals) | Distributed | Overlaps | No | BitTorrent-style model sharding over WAN; nodes must be up during inference |
| [hivemind](https://github.com/learning-at-home/hivemind) | Distributed lib | Below/overlaps | No | Low-level decentralized DL (training-leaning); not a job broker |
| [llama.cpp RPC](https://github.com/ggml-org/llama.cpp/tree/master/examples/rpc) | Distributed | Overlaps | No | Tensor-splits a model across nodes; always-on, fast link |
| [GPUStack](https://github.com/gpustack/gpustack) | Cluster manager | Overlaps | Partial/No | Manages heterogeneous GPU workers; assumes registered always-on nodes; no sleep/wake |
| [Kalavai](https://kalavai.net) | Cluster pooling | Overlaps | No | Pools machines into a cluster; not intermittency/wake-oriented |
| [Ray Serve](https://docs.ray.io/en/latest/serve/index.html) | Serving framework | Above engine | No | Scalable serving on an always-on Ray cluster |
| [KServe](https://github.com/kserve/kserve) / K8s | Orchestrator | Above | No | Kubernetes model serving; always-on, enterprise |
| [Celery](https://github.com/celery/celery) / [RQ](https://github.com/rq/rq) / [Dramatiq](https://github.com/Bogdanp/dramatiq) | Task queue | Substrate | Building block | General job queues; not LLM/fleet/wake-aware |
| [Temporal](https://github.com/temporalio/temporal) | Workflow engine | Substrate | Building block | Durable workflows; not LLM/fleet-aware |

## Engines / model servers (the layer *below* clusterbuck)

These run inference on one machine. In clusterbuck they are the swappable **model
server** a worker calls over the OpenAI HTTP API — not competitors.

### [vLLM](https://github.com/vllm-project/vllm)
High-throughput inference engine: continuous batching, PagedAttention KV cache,
tensor/pipeline parallelism. **Why not suitable:** it optimises *within* a node (or a
tight, always-on, fast-interconnect GPU cluster) and is CUDA-first; it has no concept of
intermittent machines, queuing, or wake. It's an excellent *worker engine* on a GPU node,
one layer below clusterbuck.

### [llama.cpp](https://github.com/ggml-org/llama.cpp) (+ `llama-server`)
Portable C/C++ inference (GGUF), broad hardware support incl. Apple Metal. **Why not
suitable:** single-node engine; no fleet, queue, or wake. Ideal as a worker's model
server, especially on modest hardware.

### [Ollama](https://github.com/ollama/ollama)
Friendly model management + OpenAI-compatible server over llama.cpp. Auto load/unload of
models. **Why not suitable:** single-node; no cross-machine routing or intermittency
handling. A natural default worker engine.

### [LM Studio](https://lmstudio.ai)
Desktop app with an OpenAI-compatible local server and an MLX backend (fast on Apple
Silicon). **Why not suitable:** single-node/desktop; no fleet orchestration.

### [Hugging Face TGI](https://github.com/huggingface/text-generation-inference)
Production-grade single-node/replica serving (batching, quantization). **Why not
suitable:** assumes always-on replicas; no intermittent-fleet or wake logic.

### [NVIDIA Triton Inference Server](https://github.com/triton-inference-server/server)
Datacenter multi-model/multi-framework serving. **Why not suitable:** always-on,
NVIDIA/heavyweight; opposite of a sleepy LAN fleet.

### [MLX](https://github.com/ml-explore/mlx) / [mlx-lm](https://github.com/ml-explore/mlx-lm)
Apple-Silicon array framework with a serving CLI; best tokens/sec on Macs. **Why not
suitable:** single-node engine. A good worker engine on Mac nodes.

### [LocalAI](https://github.com/mudler/LocalAI)
Self-hosted OpenAI drop-in aggregating many backends on one host. **Why not suitable:**
single-host; aggregates *backends*, not *intermittent machines*; no queue/wake.

## Gateways / routers (the sync-plane layer)

### [LiteLLM](https://github.com/BerriAI/litellm) — *adopted*
OpenAI-compatible proxy: routing, health checks, load-balancing, cloud fallback,
cooldowns. clusterbuck **uses** this for the synchronous "answer now" plane. **Why not
sufficient alone:** request/response only — no durable job queue and no notion of holding
work until a machine wakes. clusterbuck adds exactly that (the async plane + coordinator).

### [OpenRouter](https://openrouter.ai)
Hosted gateway/marketplace routing to many commercial + open models. **Why not suitable:**
cloud service; not local/LAN, and the point of clusterbuck is to prefer local, private,
free compute.

## Distributed / decentralized inference (the closest comparators)

The most-related work — and the clearest illustration of clusterbuck's different goal.
The recurring mismatch: these split **one model across nodes** (model/pipeline
parallelism), which requires the nodes to be **online at the same time**; clusterbuck
routes **whole jobs to whole machines** and embraces machines being offline.

### [exo](https://github.com/exo-explore/exo)
Runs larger models across a cluster of everyday, heterogeneous devices (incl. Macs) by
dynamically partitioning the model to available resources; peer discovery;
OpenAI-compatible API. **Why not suitable:** its purpose is aggregating memory to run a
*single* model bigger than any one device — so the devices must be up together, connected,
and fast enough. No patient queue, no sleep/wake/Wake-on-LAN, no per-job routing to a
single capable box. Solves a different problem (capacity via sharding) than clusterbuck
(availability via queuing).

### [Petals](https://github.com/bigscience-workshop/petals)
BitTorrent-style collaborative inference/fine-tuning of large models: each participant
serves some layers, pipelined over the (W)LAN/internet. **Why not suitable:** nodes must
be online during inference; latency/parallelism model assumes participation, not
intermittency; again single-model sharding, not job routing.

### [hivemind](https://github.com/learning-at-home/hivemind) (learning@home)
Library for decentralized deep learning over unreliable, heterogeneous nodes (training-
leaning; the substrate under Petals). **Why not suitable:** a low-level primitives library,
not a job broker; oriented to collaborative training/DHT rather than routing patient
inference jobs to sleepy LAN machines. (Also the name clash noted during naming.)

### [llama.cpp RPC backend](https://github.com/ggml-org/llama.cpp/tree/master/examples/rpc)
Splits a single model across machines via an RPC backend (tensor split). **Why not
suitable:** model sharding requiring all nodes up with a fast link; no queue/wake.

### [GPUStack](https://github.com/gpustack/gpustack)
Open-source manager for a cluster of heterogeneous GPUs/accelerators (Apple, NVIDIA, …)
serving models behind an OpenAI-compatible API, with scheduling across workers. **Why not
suitable (as-is):** the closest to a "manager," but it assumes **registered, always-on
worker nodes** and cluster-style scheduling; it isn't built around machines that sleep/
roam, Wake-on-LAN, or a patient job queue with per-job patience policies. Worth watching /
possibly borrowing ideas from.

### [Kalavai](https://kalavai.net)
Pools multiple machines (self-hosted or crowd-sourced) into a shared LLM cluster. **Why
not suitable:** cluster-pooling oriented; not designed around intermittency, wake, or
LAN-local patient queuing.

### [Ray Serve](https://docs.ray.io/en/latest/serve/index.html)
Scalable model serving with autoscaling replicas on a Ray cluster. **Why not suitable:**
assumes an always-on Ray cluster; heavyweight; no intermittency/wake concept.

### [KServe](https://github.com/kserve/kserve) / Kubernetes serving
Enterprise model serving on Kubernetes (autoscaling, canaries). **Why not suitable:**
always-on, cloud/enterprise; wrong weight class and wrong availability model for a home
LAN of sleepy machines.

## General task queues (clusterbuck's conceptual substrate)

clusterbuck *is* a task queue with LLM-aware routing + wake. These are what it's built
from conceptually (and Redis is the chosen broker), not alternatives to the whole system.

### [Celery](https://github.com/celery/celery) / [RQ](https://github.com/rq/rq) / [Dramatiq](https://github.com/Bogdanp/dramatiq)
Mature distributed task queues (Python). **Relationship:** the pull-based worker model is
exactly right; clusterbuck's async plane is this pattern specialised for LLM capabilities
+ wake. **Why not the whole answer:** general-purpose and Python-centric (a
cross-platform-distribution concern for our nodes — see [decisions.md](decisions.md));
no LLM/capability/wake awareness. clusterbuck implements the equivalent on Redis in C#.

### [Temporal](https://github.com/temporalio/temporal)
Durable workflow orchestration with retries/timeouts. **Relationship:** great primitives
for reliability. **Why not the whole answer:** a heavyweight workflow engine, not
LLM/fleet/wake-aware; more than this needs.

## Where that leaves clusterbuck

No surveyed project targets the exact intersection: **coarse-grained routing of patient
inference jobs across intermittent, heterogeneous, sleep/wake LAN machines, engine-
agnostic, LAN-first.** The engines slot in *below* it; LiteLLM covers the sync half; the
distributed-inference projects solve *capacity-by-sharding* rather than
*availability-by-queuing*; the cluster managers assume always-on nodes. That gap is the
reason clusterbuck exists.

---

## Entry template

```
### [<Project>](<url>)
<1–3 sentences: what it is.>
**Relationship:** <component below / gateway beside / distributed comparator / substrate>
**Why not suitable (as-is):** <which yardstick points it fails, concretely.>
```
