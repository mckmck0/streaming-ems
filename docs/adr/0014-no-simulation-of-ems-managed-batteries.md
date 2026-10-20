# 0014. The generator never simulates EMS-managed batteries

- Status: accepted
- Date: 2026-10-20
- Spec: specs/001-data-pipeline-core

## Context
Plan 1's generator simulated each site's battery with its own policy and emitted grid flows including it. In
the EMS the Battery actor is the single writer of real SoC and strategies close the loop on it (P3).

## Decision
Sites declare `bess.control: ems | site`. For `ems` sites (incl. the test site) the generator emits only
uncontrolled flows (PV, base load, heat pump, EV) plus weather and prices; no battery and no grid measurements.
A battery model with its own policy runs only for unmanaged background sites (spec 003), and it reuses the
domain battery equations rather than defining a second model.

## Consequences
Grid import/export and the bill of managed sites are EMS facts (Battery, Meter actors). Battery parameters of
managed sites are catalog metadata for the actors.

## Alternatives considered
Simulate everywhere (strategies could not change the grid flows); no BESS at all (no realistic background
sites).
