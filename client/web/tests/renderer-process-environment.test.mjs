import assert from "node:assert/strict";
import test from "node:test";

import {
  browserRuntimeEnvironment,
  browserRuntimeEnvironmentNames,
  fetchPrivateSkin,
} from "./renderer-process-environment.mjs";

test("Chromium receives runtime paths but no capture or factory secrets", () => {
  const source = {
    HOME: "/tmp/factory-home",
    PATH: "/usr/bin:/bin",
    SNAKETRON_FACTORY_SERVICE_TOKEN: "private-skin-token",
    SNAKETRON_FACTORY_RENDERER_BUNDLE_MANIFEST: '{"private":"attestation"}',
    SNAKETRON_FACTORY_RENDERER_BUNDLE_SHA256: "attestation-pin",
    CUSTOM_GEMINI_CREDENTIAL: "provider-secret",
    CUSTOM_WORKER_CREDENTIAL: "worker-secret",
    CUSTOM_WEBHOOK_CREDENTIAL: "webhook-secret",
    CUSTOM_OPERATOR_CREDENTIAL: "operator-secret",
  };

  assert.deepEqual(browserRuntimeEnvironment(source), {
    HOME: "/tmp/factory-home",
    PATH: "/usr/bin:/bin",
  });
  assert.ok(
    !browserRuntimeEnvironmentNames.includes("SNAKETRON_FACTORY_SERVICE_TOKEN"),
  );
  assert.ok(
    !browserRuntimeEnvironmentNames.some((name) => name.includes("TOKEN")),
  );
});

test("capture Node can fetch the private document without giving Chromium the token", async () => {
  const calls = [];
  const request = {
    get: async (url, options) => {
      calls.push({ url, options });
      return {
        ok: () => true,
        text: async () => '{"schema_version":2}',
      };
    },
  };

  const document = await fetchPrivateSkin(
    request,
    "https://api.test/",
    "sha256:private/ref",
    "service-only-token",
  );

  assert.equal(document, '{"schema_version":2}');
  assert.deepEqual(calls, [
    {
      url: "https://api.test/api/skins/by-ref/sha256%3Aprivate%2Fref",
      options: { headers: { Authorization: "Bearer service-only-token" } },
    },
  ]);
  assert.deepEqual(
    browserRuntimeEnvironment({
      SNAKETRON_FACTORY_SERVICE_TOKEN: "service-only-token",
    }),
    {},
  );
});
