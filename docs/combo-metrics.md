# Combo telemetry

The server records combo telemetry only after the corresponding authoritative
state is durable: ordinary collections follow a fenced `FoodEaten`
publication, recovery catch-up collections follow the fenced checkpoint that
absorbs their deltas, and terminal-tick collections follow the fenced
completion snapshot. Failed or fenced writes do not increment the metrics, and
replay consumers do not record them again.

## OpenTelemetry instruments

| Instrument | Type/unit | Meaning |
| --- | --- | --- |
| `snaketron.combo.food_collections` | monotonic counter / `1` | Food items collected |
| `snaketron.combo.points_awarded` | monotonic counter / `1` | Total physical-growth points awarded by those collections |
| `snaketron.combo.chain_depth` | histogram / `1` | Chain depth after each collection |
| `snaketron.combo.remaining_window_at_collection` | histogram / `ms` | Time left in the prior combo window when food was collected; `0` denotes a new chain |

Every instrument uses the same bounded attributes:

- `game.type`: `duel`, `2v2`, `other-team`, `solo`, `free-for-all`, or `custom`
- `game.queue_mode`: `quickmatch` or `competitive`
- `game.team_side`: `team-0`, `team-1`, or `unknown`
- `food.value`: `1`, `2`, `3`, or the defensive fallback `other`
- `boost.active`: `true` or `false`

No game, user, player, command, or snake identifier is used as a label. The
aggregate collection and points counters are also mirrored to CloudWatch EMF
as `ComboFoodCollections` and `ComboPointsAwarded`.

## Suggested Grafana panels

These examples assume the Prometheus exporter converts dots in instrument and
attribute names to underscores and appends `_total` to counters. Confirm the
translated names in the target collector before importing a dashboard.

**Collection rate by mode and food value**

```promql
sum by (game_type, game_queue_mode, food_value) (
  rate(snaketron_combo_food_collections_total[$__rate_interval])
)
```

**Average points per food**

```promql
sum by (game_type, game_queue_mode) (
  rate(snaketron_combo_points_awarded_total[$__rate_interval])
)
/
sum by (game_type, game_queue_mode) (
  rate(snaketron_combo_food_collections_total[$__rate_interval])
)
```

**Enhanced-food share**

```promql
sum by (game_type, game_queue_mode) (
  rate(snaketron_combo_food_collections_total{food_value=~"2|3"}[$__rate_interval])
)
/
sum by (game_type, game_queue_mode) (
  rate(snaketron_combo_food_collections_total[$__rate_interval])
)
```

**Median and p95 chain depth**

```promql
histogram_quantile(
  0.50,
  sum by (le, game_type, game_queue_mode) (
    rate(snaketron_combo_chain_depth_bucket[$__rate_interval])
  )
)
```

Use the same query with `0.95` for p95.

**Median remaining combo window at collection**

```promql
histogram_quantile(
  0.50,
  sum by (le, game_type, game_queue_mode) (
    rate(snaketron_combo_remaining_window_at_collection_milliseconds_bucket{food_value=~"2|3"}[$__rate_interval])
  )
)
```

Depending on the exporter, the unit suffix may instead be `_ms`; inspect the
exported series once and adjust the metric name only. Filtering to enhanced
food removes the expected zero-valued first pickup of every chain, so this
panel measures successful continuation timing rather than chain starts.

**Boost-assisted collection share**

```promql
sum by (game_type, game_queue_mode) (
  rate(snaketron_combo_food_collections_total{boost_active="true"}[$__rate_interval])
)
/
sum by (game_type, game_queue_mode) (
  rate(snaketron_combo_food_collections_total[$__rate_interval])
)
```

## Reading the dashboard

- A low enhanced-food share together with a median remaining window near zero
  suggests the two-second window is too tight for that mode or map.
- A high p95 chain depth and average points per food approaching `3` suggest
  sustained combos are common; compare game duration and death-rate panels to
  decide whether the extra physical growth is producing the desired action.
- A large gap between boosted and unboosted collection share suggests combo
  success depends heavily on Boost rather than route planning alone.
- Compare by game type and queue mode before changing the global window. If one
  mode is the outlier, retain one player-facing rule and first investigate its
  map density, speed, and food placement.
