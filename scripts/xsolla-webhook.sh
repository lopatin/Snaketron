#!/bin/bash
# Deliver a correctly-signed Xsolla notification to a local server.
#
# Why this exists: the settlement webhook is the only thing that mints
# Snakebux, and during a sandbox test it is also the slowest thing to observe —
# it arrives from Xsolla's infrastructure, to a public URL, minutes after the
# click. This sends the same message from here, signed the same way, so the
# credit path can be exercised and re-exercised in a second.
#
# It is a test instrument, not a back door: it needs the project's webhook
# secret, which is the same thing the real signature is made from. Anyone who
# can run this could already forge a settlement.
#
#   ./scripts/xsolla-webhook.sh payment 42 bux-500
#   ./scripts/xsolla-webhook.sh refund  42 bux-500 --transaction 991
#   ./scripts/xsolla-webhook.sh user_validation 42
#
# Environment:
#   SNAKETRON_XSOLLA_WEBHOOK_SECRET  required; the project's secret key
#   SNAKETRON_XSOLLA_SANDBOX         if true (default), sends dry_run: 1
#   SNAKETRON_URL                    default http://localhost:8080
set -euo pipefail

KIND="${1:-payment}"
USER_ID="${2:-1}"
SKU="${3:-bux-500}"
TRANSACTION=""

shift $(( $# > 3 ? 3 : $# )) || true
while [ $# -gt 0 ]; do
  case "$1" in
    --transaction) TRANSACTION="$2"; shift 2 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

SECRET="${SNAKETRON_XSOLLA_WEBHOOK_SECRET:-}"
if [ -z "$SECRET" ]; then
  echo "SNAKETRON_XSOLLA_WEBHOOK_SECRET is not set." >&2
  echo "It is the project secret key from the Webhooks tab, NOT the API key." >&2
  exit 1
fi

URL="${SNAKETRON_URL:-http://localhost:8080}/api/wallet/xsolla/webhook"
SANDBOX="${SNAKETRON_XSOLLA_SANDBOX:-true}"

# A payment and its refund must share a transaction id, because that id is the
# idempotency key the reversal looks the original credit up by.
if [ -z "$TRANSACTION" ]; then
  TRANSACTION="$(date +%s)$$"
fi

# dry_run is what marks a sandbox transaction. The server refuses to credit one
# unless it is itself configured for sandbox, so this mirrors that flag rather
# than always sending it.
if [ "$SANDBOX" = "true" ]; then
  DRY_RUN=1
else
  DRY_RUN=0
fi

case "$KIND" in
  user_validation)
    BODY="$(printf '{"notification_type":"user_validation","user":{"id":"%s"}}' "$USER_ID")"
    ;;
  payment|refund)
    BODY="$(printf '{"notification_type":"%s","user":{"id":"%s","country":"US"},"transaction":{"id":%s,"external_id":"local-test","dry_run":%s},"purchase":{"checkout":{"currency":"USD","amount":1.99}},"custom_parameters":{"sku":"%s","snaketron_user_id":%s}}' \
      "$KIND" "$USER_ID" "$TRANSACTION" "$DRY_RUN" "$SKU" "$USER_ID")"
    ;;
  *)
    echo "kind must be payment, refund, or user_validation" >&2
    exit 2
    ;;
esac

# Xsolla signs sha1(body + secret) and presents it as `Authorization: Signature`.
SIGNATURE="$(printf '%s%s' "$BODY" "$SECRET" | openssl dgst -sha1 -hex | awk '{print $NF}')"

echo "→ $KIND  user=$USER_ID  sku=$SKU  transaction=$TRANSACTION  dry_run=$DRY_RUN"
echo "→ $URL"

STATUS="$(curl -sS -o /tmp/xsolla-webhook-response -w '%{http_code}' \
  -X POST "$URL" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Signature $SIGNATURE" \
  --data-binary "$BODY")"

echo "← HTTP $STATUS"
if [ -s /tmp/xsolla-webhook-response ]; then
  cat /tmp/xsolla-webhook-response
  echo
fi

# 204 is the server accepting it. Anything else carries Xsolla's error envelope
# explaining why it was refused, which is the same thing the publisher account
# would show.
case "$STATUS" in
  204) echo "✓ accepted"; exit 0 ;;
  *)   echo "✗ refused"; exit 1 ;;
esac
