# 0012. Price topic keyed by market

- Status: accepted (changes the brief's Tariff key from zone `PL` to market)
- Date: 2026-10-20
- Spec: specs/001-data-pipeline-core

## Context
Several price series exist for the PL zone: PSE RCE (net-billing reference, PLN), PSE csdac (SDAC day-ahead,
PLN), ENTSO-E A44 (EUR). The brief keyed one Tariff object per zone; Plan 1 keys by market.

## Decision
`ems.price.v1` keys are market keys: `PL:RCE`, `PL:SDAC`, `ENTSOE:PL`, `PL:TGE` (reserved). With a Kafka
subscription the key becomes the Tariff object key, so there is one Tariff object per market.

## Consequences
Strategies name the market they read; currencies never mix inside one object. The actor-skeleton spec updates
the brief's `Tariff` key.

## Alternatives considered
Zone key `PL` with the market in the payload (one object owning all curves, mixed currencies).
