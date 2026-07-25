# HELIX strand — `tasklet-micro-agent-engine`

## Law

- **Piston:** local loop: in → work → out
- **Spiral:** pistons in series; every output is the next input
- **Double helix:** two separate spiral engines pointed at the same star
- **Mutual acceleration:** Alpha truth feeds Omega ops; Omega evidence feeds Alpha refinement

## This leaf

| Pair | Spiral | Role | Star |
|------|--------|------|------|
| `reasoning_pressure` | **omega** | Priority heap scheduler under concurrency cap | Reasoning under pressure — prune KV, schedule tasklets |
| `agent_platform` | **omega** | Bounded micro-tasklet ops | Agent platform — coordinate agents under safety/pressure |

### How this accelerates its twin

- **reasoning_pressure:** consumes Alpha output; returns severity/proof that refines Alpha.
- **agent_platform:** consumes Alpha output; returns severity/proof that refines Alpha.

## Runtime

```bash
python3 ~/GlacierEQ_Swarm/automations/jobapp_helix_spiral.py run --all
python3 ~/GlacierEQ_Swarm/automations/jobapp_solidify_flipper.py
```

Portfolio doctrine: `~/job-app/HELIX.md`
