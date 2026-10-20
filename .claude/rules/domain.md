---
paths:
  - "src/ems/domain/**"
---

# Domain core (`src/ems/domain/`)

The domain core is the bit-exact reference that every actor and strategy is checked against, so purity and
determinism are correctness requirements here (ADR 0004, principles 2 and 6).

- No I/O and no runtime imports: nothing from `restate`, `httpx`, Kafka clients, `pydantic_settings`, the
  filesystem, the network or environment variables. The post-edit hook flags Restate imports.
- No hidden inputs. Time arrives as the interval index `t` (UTC quarter-hours) or as an explicit timestamp
  argument, never from the wall clock; randomness arrives as an explicit, seeded generator.
- Explicit units in names or types: `p_kw`, `e_kwh`, `soc_kwh`, `price_pln_per_kwh`, `dt_h`. Power becomes
  energy only through `dt_h`.
- Deterministic arithmetic: identical inputs give bit-identical outputs on every run. Keep the order of
  floating-point operations stable and never let `set` order or `hash()` influence a result.
- The Battery model clamps or rejects infeasible commands; no strategy bypasses the physical limits.
- `Policy.decide(state, observation) -> Action` is pure; the same policy code runs in shadow and live mode.
- Every change ships unit tests, and `hypothesis` property tests for invariants: SoC within bounds, energy
  balance including efficiency losses, additivity of physical flows over steps.
- Time model, units, battery equations, settlement and net-billing rules (with their OPEN/VERIFY items) are in
  `docs/context/12-domain-model.md`. Do not implement a net-billing rule that is still marked VERIFY there;
  ask for the `energy-domain-reviewer` first.
