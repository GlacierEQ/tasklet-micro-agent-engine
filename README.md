# Tasklet Micro Agent Engine — Lightweight Agent Execution Runtime ⚡

> **Lightweight micro-agent execution engine for ultra-low overhead task dispatching.**

[![Python](https://img.shields.io/badge/Python-3.9+-blue)]()
[![Domain](https://img.shields.io/badge/Domain-Micro--Agents-purple)]()

---

## 🎯 For Recruiters & Hiring Managers

This repository implements the **Tasklet Micro Agent Engine** — a minimal, fast execution runtime designed for micro-agents running atomic tasks. It demonstrates:

- **Micro-agent state machine** executing isolated single-purpose tasklets
- **Sub-millisecond task initialization** without heavy agent framework overhead
- **Memory-efficient tasklet queues** suitable for high-density sidecar deployments
- **Python async execution loop** with structured result passing

**Why this matters**: Not every agent task requires a heavy multi-turn framework. Lightweight tasklet runtimes enable high-throughput execution of simple tool calls and subtasks.

---

## 🔬 For Engineers & Technical Reviewers

### Core Components

| Component | Language | Purpose |
|---|---|---|
| `src/tasklet_engine.py` | Python | Micro-agent execution runtime and tasklet queue |
| `tests/` | Python | Tasklet execution test suite |

---

## 🤖 ML/AI & Programmatic Mesh Integration

- **MCP Tool**: `run_tasklet()` — atomic task execution tool for swarm orchestrators
- **Mastermind Sidecar**: Telemetry bridge to APEX Highway mesh
- **SHA-256 Integrity**: Tracked in `.integrity/file_hashes.json`

---

## ⚡ Quick Start

```bash
python3 src/tasklet_engine.py
```
