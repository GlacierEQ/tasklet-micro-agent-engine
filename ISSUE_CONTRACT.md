# Issue Contract — `tasklet-micro-agent-engine`

## Pain
Unbounded micro-agents thrash; need priority scheduling, suspend/resume, honest concurrency rejects.

## Claim
Heap priority scheduler with concurrency cap, suspend/resume, no fabricated efficiency %.

## Proof
```bash
python3 job-app/helix/proofs/proof_tasklet.py
```

## Done when
Proof exits 0. Architecture (strand/integrity/helix) is **not** a substitute for this proof.

## Anti-claim
Not a full distributed agent OS.
