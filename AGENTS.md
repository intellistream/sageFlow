# Codex Project Instructions: ICPP Demo

This repository is one part of the ICPP 2026 BriskSnapShot/SAGEFlow demo
workspace. Full cross-repository context is in:

```text
/home/zero/intelliStream/icpp-demo/AGENT_PROJECT_ARCHITECTURE.md
```

## Workspace

```text
/home/zero/intelliStream/icpp-demo
├── SAGE/                 # SAGE orchestration/runtime context and demo plan
├── sageFlow/             # C++ vector stream runtime, Python bindings, paper
├── sage-examples/        # demo pipeline, datasets, configs, experiments
└── brisksnapshot-ui/     # live demo UI and backend
```

Always check `git status --short` in the repository you edit. The workspace
root is not a git repository.

## Core Pipeline

```text
public vulnerability event
  -> schema/normalization
  -> embedding layer
  -> SageFlow persistent vector window
  -> vector join pairs and clusters
  -> bounded evidence contract
  -> provider-agnostic generator/LLM service
  -> traceable user-facing answer
```

Keep these roles distinct:

- SAGE is the outer AI pipeline/orchestration story.
- SageFlow is the inner vector streaming runtime.
- Contracts are the bounded evidence handoff.
- The UI must make data flow, runtime state, evidence, and generator output visible.

## Key Files

```text
SAGE/docs/icpp-demo-revision-plan.md
sageFlow/sage_flow/bindings.cpp
sageFlow/docs/paper-icpp-demo/acmart-primary/main.tex
sage-examples/configs/icpp_demo_zhipu.json
sage-examples/apps/src/sage/apps/sageflow_service_demo/
brisksnapshot-ui/backend/live_demo_server.py
brisksnapshot-ui/src/App.tsx
brisksnapshot-ui/src/components/
```

## Current Evidence

- Runtime paper evidence:
  `sage-examples/data/icpp_demo/results/nvd_2024_q1_3k_clustered_runtime/summary.json`
- LLM/generator evidence:
  `sage-examples/data/icpp_demo/results/vuln_public_100_zhipu_llm/summary.json`
- Runtime uses real NVD 2024-Q1 records and real
  `BAAI/bge-small-en-v1.5` embeddings.
- Current generator config uses an OpenAI-compatible endpoint. Do not hard-code
  one provider or model as a system assumption.

## Rules

- Do not fabricate experiment data.
- Do not commit or print API keys.
- Do not present prepared fallback data as live paper evidence.
- Do not claim linear scaling; p=2 is the current best measured clustered
  runtime point.
- Keep the ICPP demo paper within 4 pages.
- If live state lacks a metric, display/document it as unavailable rather than
  inventing counters, latency, tokens, or throughput.

