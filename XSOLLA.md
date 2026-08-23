# Xsolla

Snakebux are sold through Xsolla, who is the merchant of record. This server
never sees a card number, a billing address, or anything else that would put it
in scope for card data — what it owns is the decision about how many Bux a
player has.

That decision has exactly two entry points, and everything here is about making
both of them safe:

- **a checkout**, minted server-to-server so the browser cannot assert who is
  buying or what it costs; and
- **a settlement webhook**, which is the only message in the system that mints
  currency.

## The shape of the integration

Headless. No Sitebuilder, no ready-made store, no Xsolla Login, no Xsolla JS
SDK — the client loads nothing from them.

```
browser                     snaketron server                 xsolla
   │                               │                            │
   │  POST /api/wallet/xsolla/checkout-token                    │
   │  { sku }                      │                            │
   │──────────────────────────────>│                            │
   │                               │  POST /merchants/{id}/token│
   │                               │  (basic auth, our creds)   │
   │                               │───────────────────────────>│
   │                               │<───────────────────────────│
   │                               │  { token, link_to_ps }     │
   │<──────────────────────────────│                            │
   │  { token, paymentUrl, … }     │                            │
   │                               │                            │
   │  window.open(paymentUrl) ───────────────────────────────-─>│
   │                               │                            │
   │                               │  POST /api/wallet/xsolla/webhook
   │                               │  Authorization: Signature <sha1>
   │                               │<───────────────────────────│
   │                               │  ledger row + balance ADD  │
```

The player's identity and the price are bound into the token request on the
server. The SKU is the only client-supplied value that reaches Xsolla, and it
has already been checked against the pack table before it gets there.

## Publisher account setup

**Integration settings → keep "Use webhooks".** That is this integration. "Use
API" is the opposite model and nothing here implements it.

Leave all three Payments checkboxes **unchecked**:

| Option | Why |
| --- | --- |
| Use external ID | We send `settings.external_id` for reconciliation only. Idempotency is keyed on Xsolla's `transaction.id`. |
| Verify external_id field in project | Validates external IDs against ones registered in the project. We register none, so this can make token creation fail. |
| Use public user ID | Changes which identifier Xsolla uses for the user. We put the numeric Snaketron user id in `user.id` and read it back on settlement. |

**Webhooks tab.** Set the callback URL to `https://<your-host>/api/wallet/xsolla/webhook`
and copy the **project secret key**. That secret is what signs webhooks and is
**a different value from the API key** — mixing the two up is the most common
way this integration fails, and it fails silently as a 401 on every settlement.

**API keys tab.** The API key is shown once. It authenticates the token call.

## Environment

| Variable | Required | Notes |
| --- | --- | --- |
| `SNAKETRON_XSOLLA_MERCHANT_ID` | yes | Account-level. Forms the token API path. |
| `SNAKETRON_XSOLLA_PROJECT_ID` | yes | Numeric project id. **Not** the merchant id. |
| `SNAKETRON_XSOLLA_API_KEY` | yes | From the API keys tab. |
| `SNAKETRON_XSOLLA_WEBHOOK_SECRET` | yes | Project secret key, from the Webhooks tab. |
| `SNAKETRON_XSOLLA_SANDBOX` | no | Defaults to `false`. See below. |
| `SNAKETRON_XSOLLA_RETURN_URL` | no | Where Pay Station sends the player afterwards. |
| `SNAKETRON_XSOLLA_WEBHOOK_ALLOWED_IPS` | no | Comma-separated IPs or CIDRs; replaces the built-in list. `*` disables pinning. |

Configuration is **all or nothing**. With none of the four set, the server
starts with payments disabled and the shop reports that Snakebux cannot be
bought — the normal state for development and CI. With *some* set, the server
refuses to start, because a half-configured merchant account is a deploy
mistake that should be caught then rather than by the first player who clicks
buy.

### Sandbox

`SNAKETRON_XSOLLA_SANDBOX=true` does two things: tokens are minted with
`settings.mode: "sandbox"` and the browser is sent to
`sandbox-secure.xsolla.com`, and settlements marked `dry_run` are honoured.

It defaults to **false**, and that default is load-bearing. Xsolla marks
sandbox transactions with `dry_run`, and a production deployment that credited
those would be minting Snakebux for free. A production server refuses a
`dry_run` settlement with `INVALID_PARAMETER` — visibly, in the publisher
account, rather than silently.

The corollary is that a sandbox deployment credits **real balances** with test
money. That is what makes an end-to-end test possible, and it is why sandbox
must never be enabled on a server whose database anyone cares about.

### Webhook source pinning

Settlements are pinned to the addresses Xsolla publishes, by default. Leaving
`SNAKETRON_XSOLLA_WEBHOOK_ALLOWED_IPS` unset applies the built-in list in
`DEFAULT_WEBHOOK_SOURCES` (`server/src/xsolla.rs`):

```
185.30.20.0/24  185.30.21.0/24  185.30.22.0/24  185.30.23.0/24
34.102.38.178   34.94.43.207    35.236.73.234   34.94.69.44    34.102.22.197
```

The Login-product addresses in Xsolla's documentation are deliberately absent:
nothing here receives Login webhooks.

Setting the variable **replaces** that list rather than adding to it, which is
the escape hatch if Xsolla changes the ranges before the constant is updated. A
refused delivery logs the address it came from, so the fix is visible:

```bash
export SNAKETRON_XSOLLA_WEBHOOK_ALLOWED_IPS="185.30.20.0/24,203.0.113.4"
export SNAKETRON_XSOLLA_WEBHOOK_ALLOWED_IPS="*"   # opt out entirely
```

Use `*` only for a deployment behind a proxy that rewrites the source address;
the signature is then the only authentication.

The address is read from `X-Forwarded-For` (left-most hop), then `X-Real-IP`.
That is only meaningful because everything reaching this process has passed
through the load balancer; a directly-exposed server would be reading a header
the caller writes.

**A sandbox deployment additionally accepts loopback and delivery that never
passed through a proxy**, because that is what `scripts/xsolla-webhook.sh`
looks like. Production does neither — an unproxied request is not something
Xsolla could have sent. So local testing needs no allowlist configuration at
all, and no habit that would follow you to production.

## Running a sandbox purchase end to end

1. **Export the five variables** above with `SANDBOX=true`, then start the
   server. It logs `Snakebux payments enabled` with the merchant id, project
   id, and sandbox flag. If it logs `payments disabled`, the variables are not
   reaching the process.

2. **Expose the webhook.** Xsolla has to reach your server, so a local run
   needs a tunnel:

   ```bash
   ngrok http 8080
   ```

   Put `https://<tunnel>/api/wallet/xsolla/webhook` in the Webhooks tab.

3. **Verify the webhook path before involving Xsolla.** No allowlist setup is
   needed: a sandbox deployment accepts local delivery.

   The settlement is the slow part of the loop and the part most likely to be misconfigured, so test
   it directly:

   ```bash
   ./scripts/xsolla-webhook.sh payment 42 bux-500
   ```

   `204` means the credit applied — check the balance moved by 500. A `401`
   means `SNAKETRON_XSOLLA_WEBHOOK_SECRET` does not match what the script
   signed with. Re-running the same command with `--transaction <id>` twice is
   the idempotency check: the second is still a `204` and the balance must not
   move again.

4. **Buy a pack in the UI.** Open the wallet, pick a pack. The modal says
   "Test mode: no real money will be charged" when the server is sandboxed. Pay
   Station opens in a new tab; use Xsolla's test card (`4111 1111 1111 1111`,
   any future expiry, any CVV).

5. **Watch the balance.** It moves when the webhook lands, not when the tab
   closes — settlement is asynchronous. The wallet re-reads the balance
   whenever the window regains focus, so switching back is usually enough.

6. **Buy a skin** with the Bux. That path (`POST /api/skins/:id/purchase`) is
   independent of Xsolla: it debits the same ledger with a client-minted UUID
   key in the `PURCHASE` namespace.

7. **Refund it** from the Xsolla dashboard. The reversal writes a `REFUND` row
   and subtracts the original credit. The balance may go negative — that is
   correct, and it blocks further spending until repaid. Ownership already
   granted is not revoked; Bux are fungible and per-item clawback is a support
   action.

## What the webhook does with each notification

Status codes are Xsolla's contract, not ours: it retries anything that is not
2xx and stops on a 400 carrying its error envelope.

| Notification | Response |
| --- | --- |
| `user_validation` | `204` if the user exists and is not a guest; `400 INVALID_USER` otherwise. Answering "fine" to everything makes the check worthless. |
| `payment` | Credits the SKU's configured value. `204`. |
| `refund` | Subtracts what the original credit granted, read from its ledger row. `204`. |
| duplicate of any of the above | `204`. A duplicate is a success from the provider's point of view. |
| anything else | `204`, unread. An unrecognised message is not a failure. |
| bad signature | `401 INVALID_SIGNATURE`. |
| unlisted source | `403 INVALID_PARAMETER`, with the address logged. |
| `dry_run` on a production deployment | `400 INVALID_PARAMETER`. |

## Distribution gating

CrazyGames prohibits exposing in-app purchases without portal approval
(`CRAZYGAMES.md`). The portal build is served an empty pack list and a `403`
from the checkout endpoint; the wallet then says Snakebux cannot be bought in
this version and that existing Bux still work.

The distribution comes from the `x-snaketron-distribution` header, which the
client asserts exactly as it already asserts it on the WebSocket handshake.
This is routing, not authorization — the gate exists so the CrazyGames *build*
does not present an unapproved surface, not to stop a determined caller. If the
portal later approves purchases, `purchases_allowed` in
`server/src/api/wallet.rs` becomes one arm of a match.

## Where things live

| | |
| --- | --- |
| `server/src/xsolla.rs` | Config, SHA-1 signature verification, IP prefixes, token API client, notification types. |
| `server/src/api/wallet.rs` | The two HTTP handlers, the distribution gate, and the settlement decisions. |
| `server/src/wallet.rs` | The ledger: namespacing, idempotency keys, request fingerprints. |
| `scripts/xsolla-webhook.sh` | Signs and delivers a notification locally. |
| `client/web/components/WalletModal.tsx` | The shop. |
