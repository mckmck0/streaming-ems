# Actor contract (template for plan.md)

Copy this section into `plan.md` for every feature that adds or changes an actor, handler, message or
timer. Replace every `<…>`; write "none" rather than deleting a row.

## Actor contract

### Actors

| Actor (Virtual Object) | Key | State (field: type, unit) | Owner of facts | New / changed |
|---|---|---|---|---|
| `<Name>` | `<[run_id:]location_id \| [run_id:]market_key>` | `<field: type [unit]>` | `<facts from the ownership table>` | `<new \| changed>` |

### Handlers

| Actor.handler | Kind (exclusive \| shared) | Input message (version) | Writes | Sends (one-way) | Calls (request-response, shared only) |
|---|---|---|---|---|---|
| `<Actor.handler>` | `<kind>` | `<Message vN>` | `<state fields>` | `<Actor.handler: Message>` | `<Actor.shared_handler>` |

### Messages

| Message | Version | Producer → consumer | Fields added or changed | Additive? | Golden examples updated? |
|---|---|---|---|---|---|
| `<Message>` | `<vN>` | `<A → B>` | `<field: type [unit]>` | `<yes \| no → ADR>` | `<yes \| n/a>` |

### Timers

| Timer | Owner | Sim mode (data in state, fired at its step) | Live mode (`send_delay`) | Generation guard |
|---|---|---|---|---|
| `<what is scheduled>` | `<Actor>` | `<state field, or "none">` | `<delay, or "none">` | `<yes>` |

### Failure modes

| Failure | Expected behaviour | Test (tier) |
|---|---|---|
| SDK service `kill -9` mid-replay | `<…>` | `<fault>` |
| Restate server `kill -9` mid-replay | `<…>` | `<fault>` |
| Duplicate delivery | `<…>` | `<integration>` |
| Out-of-order / late message | `<…>` | `<integration>` |
| Stale view | `<…>` | `<e2e>` |

### Rule check

| Rule | Holds? | Evidence |
|---|---|---|
| R1/R2: exclusive handlers reached only by one-way messages; request-response only to shared handlers | `<yes/no>` | `<call graph>` |
| R3: non-determinism journaled | | |
| R4: idempotent ingress | | |
| R5: no waiting in exclusive handlers | | |
| R6: generation numbers on self-rescheduling jobs | | |
| R7: envelope fields; messages applied in `t` or sequence order, early ones held back, reorderings counted | | |
| Ordering (ADR 0017): steps released only when every input watermark covers them; lateness and dedup judged inside the record's own stream | | |
| Single writer per fact (ownership table updated) | | |
