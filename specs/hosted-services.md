# Spec: Hosted Services (runtime plugin injection)

**Status:** Draft for review. Nothing in this document is built yet.
**Repo:** This describes a change to the **public** Snaketron game crate. Its
first consumer is the private `snaketron-io` deployment repo, whose analytics
pipeline is specified in `snaketron-io/specs/game-events-analytics-prd.md`.

---

## 1. Purpose

The game server needs to run background work that is **deployment policy, not
game mechanics** — today an S3→Iceberg analytics fold, tomorrow whatever else
the operator needs. That work must not live in the public game crate, and it
must not be a separate deployable either: it needs the server's Valkey client,
its region and task identity, its cluster leases, and above all its shutdown
budget.

So the public crate provides the **mechanism** — a supervised, scoped,
gracefully-shut-down slot for externally-supplied long-running work — and the
operator provides the **policy** by registering implementations.

**Non-goal:** a general extension system. There is no dynamic loading, no ABI,
no scripting. A hosted service is ordinary Rust compiled into the binary.

---

## 2. Exclusion: what it is, and what it can actually guarantee

Read this before the API. Exclusion is **configurable per service**, and the
guarantee you get depends on the downstream resource — not on the framework
alone.

### 2.1 The key

A service declares the key by which its instances must be exclusive:

```rust
fn exclusion_key(&self, ctx: &ServiceContext) -> Option<ExclusionKey>;
```

- **`None`** — no exclusion. Every eligible task runs an instance. This is the
  cheap default and the right answer for stateless per-task work.
- **`Some(key)`** — at most one instance per key, gated by a lease.

```rust
pub struct ExclusionKey {
    pub domain: ExclusionDomain,   // Region | Global
    pub key: String,               // e.g. "iceberg-committer/game_events"
}
```

The key subsumes what would otherwise be a scope enum: `None` means run
everywhere; `Region` means one per (region, key); `Global` means one per key
across all regions. It also allows *partitioned* exclusion — a committer keyed
by table name runs one instance per table, concurrently, without any of them
colliding.

The extractor takes `&ServiceContext`, so a key may depend on region,
environment, or config. It is computed once per instance. **Per-work-item
locking is out of scope** — that is a distributed lock manager, not a service
scope, and it should not be smuggled in through this interface.

### 2.2 The three guarantees

| Configuration | Guarantee | Overlap possible? |
| --- | --- | --- |
| `None` | None. N instances by design. | Always, intentionally |
| `Some(key)`, unfenced writes | **Fail-closed best effort.** Steady state is one instance. | Yes — GC pause, Valkey blip, clock skew |
| `Some(key)` + epoch threaded into conditional writes | **Effective exclusion at that resource.** A stale instance's writes are *rejected*. | Two may run; only one can take effect |

The middle row is what a lease alone buys, and it is the row people
mis-read as the bottom row. The framework cannot close that gap by itself:
stopping a paused process is not something a lease holder in another datacentre
can do. What it *can* do is make the stale writer's effects fail.

### 2.3 Why the existing token is not enough

`LeaseToken` is `{ boot_id, acquisition_id: Uuid }`, encoded
`"{boot_id}:{acquisition_id}"` (`partition_lease.rs:20-34`). It is a **random
identity token**: it answers "is this still my lease?" but not "is this lease
newer than that one?"

That distinction decides everything downstream. Equality alone is sufficient
only when the check is **atomic with the effect** — which is exactly why the
existing fenced usage works: `guard.encoded_token()` is passed as an `ARGV` into
a Lua script that compares it and applies the effect in one round trip
(`partition_lease.rs:191/215/242/269`). A stale holder that still believes it
holds the lease passes an equality check against its own token; it fails only
because the *stored* token has moved on.

For an external resource that cannot see Valkey, equality is useless — you would
have to read the current token first, and the read/write pair is not atomic.
Real fencing needs a **monotonically increasing epoch**, which the codebase does
not currently have (§6.1).

### 2.4 Per-resource reality

| Resource | Mechanism | Result |
| --- | --- | --- |
| Valkey | token equality inside a Lua script | **Fenced today.** Existing pattern |
| DynamoDB | `ConditionExpression` on the epoch, in the same `UpdateItem` as the effect | **Fenceable.** Needs the typed conditional path (§6.2) |
| Iceberg / S3 Tables | epoch stored in table properties, commit conditioned on it | **Fenceable.** Requires monotonic epoch — equality is insufficient |
| S3 raw objects | none available (`If-Match` is ETag-only) | **Not fenceable — and does not need to be.** Keys are content-derived and writes use `If-None-Match`, so a stale writer writes byte-identical content (analytics PRD R4.7) |

That last row is the important one: the right response to "this resource cannot
be fenced" is usually to make the write idempotent, not to strengthen the lease.

### 2.5 Rules

- **HS-1.** A service with `exclusion_key() == None` must tolerate N concurrent
  instances.
- **HS-2.** A service with a key but **unfenced** effects must be idempotent
  under double-execution. The framework guarantees "usually one."
- **HS-3.** A service needing true exclusion must thread `lease.epoch()` into
  every external write that supports conditional execution, and must treat a
  rejected write as "I am no longer the holder" — not as a retryable error.
- **HS-4.** The supervisor **fails closed**: the child token is cancelled at
  `ttl − operation_timeout` measured from the **last confirmed renewal**, never
  from wall-clock optimism. This needs an acquisition timestamp, which
  `CoordinatorLeaseStore` does not expose today (§6.1).
- **HS-5.** A service whose effects can be neither fenced nor made idempotent
  must not use an exclusion key, because the key would imply a guarantee it does
  not have.

## 3. API

A new crate, `snaketron-service-api`, holds the public surface so the game
crate's internals (`RedisConnection`, `DynamoDatabase`, `GameBus`) never leak
into the operator's dependency graph.

### 3.1 The trait

```rust
#[async_trait]
pub trait HostedService: Send + 'static {
    /// Run until `cancel` fires, then return promptly.
    ///
    /// Returning `Ok(())` before cancellation is treated as an unexpected exit
    /// and is subject to the restart policy — a service that has genuinely
    /// finished should park on `cancel.cancelled()`.
    async fn run(self: Box<Self>, cancel: CancellationToken) -> Result<(), ServiceError>;
}

#[async_trait]
pub trait HostedServiceFactory: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    /// The key by which instances must be mutually exclusive, or `None` for no
    /// exclusion. Computed once per instance from context, so a service may key
    /// by region, environment, or configuration. See §2.
    fn exclusion_key(&self, ctx: &ServiceContext) -> Option<ExclusionKey> {
        let _ = ctx;
        None
    }

    fn failure_policy(&self) -> FailurePolicy { FailurePolicy::default() }

    /// Build a fresh instance. Called once per start and once per restart, so
    /// a service may hold non-reusable state without an internal reset path.
    async fn build(&self, ctx: ServiceContext) -> Result<Box<dyn HostedService>, ServiceError>;
}
```

**Why a factory plus a consuming `run`, not `start`/`stop`.** A `stop()` that
must interrupt a running `start()` forces every implementation to invent its own
cancellation channel. One consuming `run(self: Box<Self>, cancel)` makes the
service's lifetime the natural unit: it owns its state, the token is the only
stop signal, and cleanup is straight-line code before the return. Restart is
then "build a new one," which needs no reset method and cannot resurrect
poisoned state.

**Why `#[async_trait]`.** Native async-fn-in-trait is stable for *static*
dispatch, and RPITIT (`fn run(&self) -> impl Future<Output = …> + Send;`) can
express the `Send` bound that `tokio::spawn` requires. But `dyn Trait` with an
async method is still rejected (E0038), and the registry is
`Vec<Arc<dyn HostedServiceFactory>>`. So `#[async_trait]` remains correct; its
cost is one `Box` allocation per call on a per-process-lifetime path.

### 3.2 The lease handle

When `exclusion_key()` returns `Some`, the supervisor acquires the lease before
building the service and exposes it on the context:

```rust
pub struct LeaseHandle {
    /// Monotonically increasing across acquisitions of this key. THIS is the
    /// fencing token — thread it into conditional writes (HS-3).
    pub fn epoch(&self) -> u64;
    /// Identity token, for effects checked atomically inside Valkey.
    pub fn token(&self) -> &LeaseToken;
    /// Last confirmed renewal. Fail-closed decisions derive from this, never
    /// from `Instant::now()` optimism.
    pub fn last_renewed_at(&self) -> Instant;
}
```

`ExclusionDomain::Region` resolves to a Valkey lease; `Global` to a DynamoDB
lease, because Valkey does not span regions (§6.2).

The analytics Iceberg committer keys `Global` for a concrete reason: many
concurrent committers drive `metadata.json` past S3 Tables' 50 MB ceiling, which
bricks the table. A regional key would elect two.

**A service without an exclusion key must keep its state per-`GameServer`
instance, never in a process-wide `static`.** Tests start several `GameServer`s
in one process, and a `static` would make them interfere.

### 3.3 Context

```rust
pub struct ServiceContext {
    pub environment: Environment,     // prod | dev
    pub region: RegionId,             // use1 | euw1
    pub aws_region: String,
    pub identity: TaskIdentity,       // server_id, boot_id, task_boot_id
    pub kv: Arc<dyn KeyValueStore>,   // narrow trait, not RedisConnection
    pub lifecycle: Arc<dyn LifecycleView>,
    pub config: ServiceConfig,        // prefixed env vars, parsed
    extensions: Extensions,           // escape hatch, see below
}

impl ServiceContext {
    /// Signal that this service is ready. Purely informational today; it does
    /// not gate the task's own readiness, because analytics must never keep a
    /// game server out of the load balancer.
    pub fn mark_ready(&self);
    /// Resolves when the task begins draining, before the cancel token fires.
    pub async fn on_drain(&self);
    pub fn extension<T: Clone + Send + Sync + 'static>(&self) -> Option<T>;
}
```

Dependencies are passed as a **concrete struct of narrow object-safe traits**
rather than a typemap. The plugin lives in another repo, so a missing dependency
must be a compile error, not a 3am `None`. The `extensions` escape hatch exists
for genuinely optional host-specific handles; note it can only hold `Clone`
values, which rules out non-cloneable handles — park an `Arc<dyn Trait>` instead.

`server_id` is an `i32` from a DynamoDB atomic counter, widened to `u64` in
`GameServer::start`; `TaskIdentity` exposes it at its real width plus the
`task_boot_id` string already used in logs.

### 3.4 Hooks

Four, deliberately:

| Hook | Why it earns its place |
| --- | --- |
| `Factory::build` (fallible) | Dependency resolution fails at boot, not mid-flight |
| `run(cancel)` | The only stop signal; cleanup is straight-line before return |
| `ctx.mark_ready()` | Operator visibility into slow-starting services |
| `ctx.on_drain()` | A chance to flush *before* the hard cancel |

Explicitly **not** included: `on_lease_acquired` / `on_lease_lost`. Leadership is
a **lifetime**, not a pair of callbacks — the supervisor acquires the lease,
builds the service, and drives `run` under a child token that is cancelled the
instant renewal fails. Losing leadership *is* cancellation, so the callbacks
would be a second, redundant, easily-desynchronized representation of the same
fact.

A service that must *observe* its own fencing epoch reads `lease.epoch()`; it
does not need a callback to be told the epoch changed, because an epoch change
means a different instance holds the lease and this one has been cancelled.

---

## 4. Supervision and failure policy

```rust
pub enum FailurePolicy {
    /// Default. Rebuild with jittered exponential backoff.
    Restart { max_consecutive: u32 },   // default 10
    /// Log and leave it stopped.
    Disable,
    /// Escalate to host shutdown. Almost nothing should use this.
    FailHost,
}
```

- **A plugin fault disables the plugin, not the host.** The game server must
  keep serving players when an analytics service is crashlooping. `FailHost`
  exists for a service whose absence genuinely makes the task incorrect; nothing
  in the analytics pipeline qualifies.
- **Panics are caught and counted.** `tokio::spawn` surfaces a panic as
  `JoinError::is_panic()` — but only if the handle is awaited. The default panic
  hook still prints at the panic site either way; what dropping the handle loses
  is *programmatic* observation, so the supervisor cannot restart, count, or
  attribute the failure. Every hosted-service handle is therefore retained and
  awaited.
- **Backoff is jittered** to avoid a fleet-wide synchronized retry after a
  shared dependency recovers.
- Exceeding `max_consecutive` disables the service and emits a bounded,
  identity-free metric. It never takes the task down.

---

## 5. Graceful shutdown

The existing sequence is a 3-step, single-budget teardown governed by
`SNAKETRON_SHUTDOWN_DEADLINE_MS` (default 45 s), ending in one
`cancellation_token.cancel()` and a `FuturesUnordered` join that `abort()`s on
timeout.

Hosted services join that budget on the same terms:

1. `ctx.on_drain()` resolves when draining starts — the flush window.
2. The child token is cancelled.
3. Each service gets a **per-service `stop_budget`, clamped by the remaining
   global deadline**, so one slow service delays only itself. A single shared
   budget with no attribution is the mistake worth avoiding here: when it
   expires, you learn that shutdown was slow but not who caused it.
4. Services still running are `abort()`ed and named in the log.

**A hosted service must be registered in the supervised `handles` vec.** The
codebase already has four detached background tasks that are never joined, and
one — the lobby update forwarder (`lobby_manager.rs:186-240`) — takes no
`CancellationToken` at all and loops unconditionally. That is precisely the
shape a hosted service must not have, because a detached analytics service would
silently lose its final flush.

---

## 6. Integration points

### 6.1 Lease parameterization (prerequisite)

`CoordinatorLeaseStore` is hardcoded to `namespace.assignment_lease()`. Making
it usable needs, per the survey, roughly a 20-line change: one field, one
`RedisKeys` function, one `ClusterNamespace` method. `ClusterNamespace` is the
single choke point for cluster keys and already validates its charset, so the
new lease names inherit that.

It must **additionally** gain two things the current type lacks:

1. **An acquisition / last-renewal timestamp**, to satisfy HS-4. Only
   `PartitionLeaseGuard` carries one today, and without it there is no correct
   moment to fail closed.
2. **A monotonic epoch**, to satisfy HS-3. The existing token is a random
   `Uuid` (§2.3), so it cannot order two acquisitions. The epoch is an `INCR` on
   a companion key, performed inside the same Lua script that takes the lease so
   acquisition and numbering cannot diverge. Only a *new* acquisition increments
   it; renewals do not.

### 6.2 Global lease (new)

No cross-region primitive exists — no shared Valkey, no DynamoDB Global Table,
no cross-region membership view. The only shared substrate is the US-only
DynamoDB main table, which every task reaches because `AWS_REGION` is pinned to
`us-east-1` for all regions (`cdk/lib/fargate-stack.ts:420`).

A DynamoDB lease is implementable — the codebase has extensive
`ConditionExpression` optimistic locking to copy — with three hazards the Valkey
leases do not have:

1. **No server-side clock.** Expiry must be a client-supplied timestamp compared
   inside the condition, so safety depends on bounded us-east-1 ↔ eu-west-1
   clock skew. Skew must be measured and budgeted, not assumed.
2. **DynamoDB TTL cannot expire a lease** — it is asynchronous and may lag up to
   48 hours. TTL is for cleanup only; correctness must come from the condition.
3. **The codebase classifies DynamoDB failures by re-reading**, not by typed
   `ConditionalCheckFailedException` — there are zero occurrences of that
   exception type anywhere. The lease needs the typed path, which is new.

The epoch for a global key uses the same atomic-counter pattern the codebase
already has for `generate_id_for_entity`
(`SET #counter = if_not_exists(#counter, :initial) + :increment` with
`ReturnValue::AllNew`), so monotonicity needs no new mechanism — only the typed
conditional path does.

For the Iceberg committer specifically, the layering is: the lease keeps the
steady state at one committer so `metadata.json` stays small (an efficiency
property), the epoch makes a stale committer's commit *fail* rather than
double-write (HS-3), and R8.8's marker plus `event_id` dedup make even an
un-fenced overlap harmless (HS-2). All three, because each covers a case the
others do not.

### 6.3 Where the registry attaches

`GameServer::start` pushes exactly 8 `JoinHandle<()>` into `handles`. The
natural attachment point is beside
`handles.push(spawn_resilience_metrics(…, cancellation_token.child_token()))`
(`game_server.rs:590`) — already the exact shape a plugin needs: a
`JoinHandle<()>`, a `child_token()`, and a `TaskLifecycle` clone. Every
dependency `ServiceContext` requires is already in scope at that line.

`GameServerConfig` gains one field:

```rust
pub hosted_services: Vec<Arc<dyn HostedServiceFactory>>,   // default: empty
```

This mirrors how `db: Arc<dyn Database>` and `jwt_verifier: Arc<dyn JwtVerifier>`
are already injected, so it is idiomatic rather than novel. An empty vec is the
default and changes nothing.

---

## 7. Who owns the binary

Two viable arrangements, and the choice is not obvious:

**(A) The public `main.rs` stays the composition root.** Registration happens
behind a config flag; implementations live in the public crate. Zero build
changes.

**(B) `snaketron-io` owns a binary crate depending on `server` by path.** This
is what "snaketron-io passes services into Snaketron" means literally, and it
keeps deployment-specific code out of the public repo. The Docker context moves
from `./snaketron` to the repo root and three build call sites change ~two lines
each; **CDK needs no change**, because the server container never overrides
`command`/`entryPoint`.

(B) has two real costs that must be paid deliberately:

1. **A second, independently-resolved `Cargo.lock`.** The public manifest pins
   `tokio-util`, `tokio-tungstenite`, and `tungstenite` at `"*"`, so the shipped
   binary is not provably the binary the nextest archive tested. **Fix the `"*"`
   pins before adopting (B)** — this is worth doing regardless.
2. **Submodule bumps become potential compile failures** that the public repo's
   CI cannot see. Mitigated by a snaketron-io CI job building the private binary
   against the pinned submodule — which that repo should have anyway.

**Recommendation:** ship the trait and the registry now (they are identical
under both), start with (A) to keep the deploy pipeline untouched, and move to
(B) when the first implementation genuinely must stay private. The seam is the
valuable part and it is the same either way; the composition root is a
one-file change to defer.

---

## 8. Testing

- A `HostedService` is testable standalone: construct a `ServiceContext` from
  in-memory fakes of the narrow traits, drive `run` with a token you control.
- The registry needs host-level tests for: restart-with-backoff on repeated
  failure, disable after `max_consecutive`, panic isolation, cancellation
  promptness, and `stop_budget` expiry naming the offender.
- **Exclusion tests are the ones that matter**, and each guarantee in §2.2 needs
  its own:
  - `exclusion_key() == None` → N instances all run.
  - `Some(key)` → at most one runs in steady state; on holder death another
    takes over within the lease TTL.
  - **Different keys do not contend** — two instances with distinct keys both
    run, which is the whole point of a keyed rather than global singleton.
  - **Forced overlap** (suspend the holder past its TTL without releasing) →
    an unfenced service double-executes and its idempotency absorbs it; a fenced
    service's stale write is *rejected* on epoch. This is the test that proves
    the difference between the middle and bottom rows of §2.2, and without it
    the distinction is only documentation.
  - Epoch monotonicity across a sequence of acquisitions, including contended
    ones.
- Tests must start several `GameServer`s in one process to catch accidental
  process-wide state (§3.2).

---

## 9. Open questions

1. **Clock skew budget** for the DynamoDB global lease (§6.2). Must be measured
   between us-east-1 and eu-west-1 before the lease TTL is chosen.
2. **CPU isolation.** A hosted service shares the game container's 2 vCPU with a
   100 ms tick loop. CPU-bound work (Parquet encoding) must use
   `spawn_blocking`, and the p99-latency comparison that guards the analytics
   pipeline should cover the elected task specifically, not the fleet average.
3. **Should election prefer a draining or least-loaded task?** Electing a task
   that is actively serving players concentrates any jitter on those players.
   Probably not worth the complexity, but it should be a recorded decision.
4. **`"*"` version pins** in `snaketron/server/Cargo.toml` (§7) — worth fixing
   independently of this spec.
