import React, { FormEvent, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { useRuntimeConfig } from '../contexts/RuntimeConfigContext';
import {
  api,
  isApiError,
} from '../services/api';
import type { RuntimeConfig, RuntimeConfigAuditPage, RuntimeConfigRecord } from '../types';
import type { SkinSummary } from '../types/generated';
import { MatchHistoryList } from './MatchHistoryList';
import { getWasm, initWasm } from '../wasm';
import { ensureAuthoredSkins } from '../utils/authoredSkins';

type AdminSection = 'overview' | 'history' | 'skins' | 'configuration' | 'audit';

const SECTION_LABELS: Array<{ id: AdminSection; label: string; compactLabel: string }> = [
  { id: 'overview', label: 'Overview', compactLabel: 'Overview' },
  { id: 'history', label: 'Match history', compactLabel: 'Matches' },
  { id: 'skins', label: 'Skins', compactLabel: 'Skins' },
  { id: 'configuration', label: 'Configuration', compactLabel: 'Config' },
  { id: 'audit', label: 'Audit', compactLabel: 'Audit' },
];

const AD_DISTRIBUTIONS = [
  { id: 'web', label: 'Website' },
  { id: 'crazygames', label: 'CrazyGames' },
  { id: 'itch', label: 'itch.io' },
] as const;

const enabledAdDistributions = (config: RuntimeConfig): string[] => (
  AD_DISTRIBUTIONS
    .filter(({ id }) => config.ads.distributions[id].enabled)
    .map(({ label }) => label)
);

const dateTime = (value: number): string => {
  if (!value) return 'Initial defaults';
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: 'medium',
    timeStyle: 'short',
  }).format(new Date(value));
};

const errorMessage = (error: unknown): string => {
  if (isApiError(error)) {
    const data = error.response.data;
    if (data && typeof data === 'object') {
      const record = data as Record<string, unknown>;
      if (typeof record.message === 'string') return record.message;
      if (typeof record.error === 'string') return record.error;
    }
    if (error.response.status === 409) {
      return 'Configuration changed in another session. Reload it before saving again.';
    }
  }
  return error instanceof Error ? error.message : 'The request could not be completed.';
};

const validateConfig = (config: RuntimeConfig): string | null => {
  if (config.announcement.enabled && !config.announcement.message.trim()) {
    return 'Add an announcement message or turn the announcement off.';
  }
  if (config.announcement.message.length > 280) {
    return 'Announcement messages cannot exceed 280 characters.';
  }
  if (
    !Number.isInteger(config.ads.minimumGamesPlayed)
    || config.ads.minimumGamesPlayed < 0
    || config.ads.minimumGamesPlayed > 10000
  ) {
    return 'Minimum games played must be a whole number from 0 to 10,000.';
  }
  if (
    !Number.isInteger(config.ads.minimumIntervalMinutes)
    || config.ads.minimumIntervalMinutes < 1
    || config.ads.minimumIntervalMinutes > 1440
  ) {
    return 'Ad interval must be a whole number from 1 to 1,440 minutes.';
  }
  const { snapshotRetentionDays, summaryRetentionDays } = config.history;
  if (
    !Number.isInteger(snapshotRetentionDays)
    || snapshotRetentionDays < 1
    || snapshotRetentionDays > 3650
    || !Number.isInteger(summaryRetentionDays)
    || summaryRetentionDays < 1
    || summaryRetentionDays > 3650
  ) {
    return 'History retention must be a whole number from 1 to 3,650 days.';
  }
  if (summaryRetentionDays < snapshotRetentionDays) {
    return 'Summary retention must be at least as long as snapshot retention.';
  }
  return null;
};

const AdminOverview: React.FC<{ record: RuntimeConfigRecord | null }> = ({ record }) => {
  const config = record?.config;
  const distributions = config ? enabledAdDistributions(config) : [];
  return (
    <section className="admin-section" aria-labelledby="admin-overview-title">
      <div className="admin-section-heading">
        <div>
          <p className="admin-eyebrow">Live controls</p>
          <h2 id="admin-overview-title">Overview</h2>
        </div>
        <p>A compact readout of the settings currently shaping the arena.</p>
      </div>
      <dl className="admin-status-ledger">
        <div>
          <dt>Announcement</dt>
          <dd data-tone={config?.announcement.enabled ? 'active' : 'quiet'}>
            {config?.announcement.enabled ? 'Published' : 'Off'}
          </dd>
          <p>{config?.announcement.enabled
            ? config.announcement.message
            : 'No player-facing service notice.'}</p>
        </div>
        <div>
          <dt>Pre-match video ads</dt>
          <dd data-tone={config?.ads.enabled ? 'active' : 'quiet'}>
            {config?.ads.enabled ? 'Enabled' : 'Disabled'}
          </dd>
          <p>{config
            ? `${distributions.length > 0
              ? `Distributions: ${distributions.join(', ')}`
              : 'No distributions enabled'} · ${config.ads.minimumGamesPlayed} game minimum · ${config.ads.minimumIntervalMinutes} minute interval.`
            : 'Loading the current runtime record.'}</p>
        </div>
        <div>
          <dt>History retention</dt>
          <dd>{config ? `${config.history.summaryRetentionDays} days` : '—'}</dd>
          <p>{config
            ? `Snapshots ${config.history.snapshotRetentionDays} days · summaries ${config.history.summaryRetentionDays} days.`
            : 'Loading retention policy.'}</p>
        </div>
        <div>
          <dt>Configuration version</dt>
          <dd>{record ? `v${record.version}` : '—'}</dd>
          <p>{record
            ? `Updated ${dateTime(record.updatedAtMs)}${record.updatedBy ? ` by ${record.updatedBy.username}` : ''}.`
            : 'Waiting for the authoritative record.'}</p>
        </div>
      </dl>
    </section>
  );
};

const AdminHistory: React.FC = () => {
  const loadHistory = useCallback((cursor: string | null) => api.getAdminMatchHistory({
    cursor,
    limit: 25,
  }), []);

  return (
    <section className="admin-section" aria-labelledby="admin-history-title">
      <div className="admin-section-heading">
        <div>
          <p className="admin-eyebrow">Completed games</p>
          <h2 id="admin-history-title">Match history</h2>
        </div>
        <p>Inspect the same immutable result records players see in their History modal.</p>
      </div>
      <p className="admin-history-scope">
        Newest first · 25 records per page · player details included
      </p>
      <MatchHistoryList
        variant="admin"
        loadPage={loadHistory}
        emptyMessage="No completed matches have been recorded."
      />
    </section>
  );
};

const AdminConfiguration: React.FC<{
  record: RuntimeConfigRecord | null;
  setRecord: (record: RuntimeConfigRecord) => void;
  reload: () => Promise<void>;
}> = ({ record, setRecord, reload }) => {
  const { applyRecord } = useRuntimeConfig();
  const [draft, setDraft] = useState<RuntimeConfig | null>(record?.config ?? null);
  const [saving, setSaving] = useState(false);
  const [status, setStatus] = useState<{ tone: 'success' | 'error'; message: string } | null>(null);

  useEffect(() => {
    setDraft(record?.config ?? null);
  }, [record]);

  if (!draft || !record) {
    return (
      <section className="admin-section" aria-busy="true">
        <div className="admin-inline-status">Loading configuration…</div>
      </section>
    );
  }

  const save = async (event: FormEvent) => {
    event.preventDefault();
    const validationError = validateConfig(draft);
    if (validationError) {
      setStatus({ tone: 'error', message: validationError });
      return;
    }
    setSaving(true);
    setStatus(null);
    try {
      const nextRecord = await api.updateAdminRuntimeConfig(draft, record.version);
      setRecord(nextRecord);
      applyRecord(nextRecord);
      setStatus({ tone: 'success', message: `Configuration v${nextRecord.version} is live.` });
    } catch (error) {
      setStatus({ tone: 'error', message: errorMessage(error) });
    } finally {
      setSaving(false);
    }
  };

  const numberValue = (value: string): number => Number.parseInt(value, 10) || 0;

  return (
    <section className="admin-section" aria-labelledby="admin-config-title">
      <div className="admin-section-heading">
        <div>
          <p className="admin-eyebrow">Version {record.version}</p>
          <h2 id="admin-config-title">Configuration</h2>
        </div>
        <p>Changes take effect without a deploy. Save uses version matching to prevent silent overwrites.</p>
      </div>
      <form className="admin-config-form" onSubmit={save}>
        <fieldset>
          <legend>Player announcement</legend>
          <label className="admin-toggle-row">
            <span>
              <strong>Publish banner</strong>
              <small>Show a compact notice above the arena.</small>
            </span>
            <input
              type="checkbox"
              checked={draft.announcement.enabled}
              onChange={(event) => setDraft({
                ...draft,
                announcement: { ...draft.announcement, enabled: event.target.checked },
              })}
            />
          </label>
          <label className="admin-field is-full">
            <span>Message</span>
            <input
              type="text"
              maxLength={280}
              value={draft.announcement.message}
              onChange={(event) => setDraft({
                ...draft,
                announcement: { ...draft.announcement, message: event.target.value },
              })}
            />
            <small>{draft.announcement.message.length}/280</small>
          </label>
        </fieldset>

        <fieldset>
          <legend>Pre-match video advertising</legend>
          <label className="admin-toggle-row">
            <span>
              <strong>Enable pre-match video ads</strong>
              <small>The server decides whether an eligible lobby enters an ad break before matchmaking.</small>
            </span>
            <input
              type="checkbox"
              aria-label="Enable pre-match video ads"
              checked={draft.ads.enabled}
              onChange={(event) => setDraft({
                ...draft,
                ads: { ...draft.ads, enabled: event.target.checked },
              })}
            />
          </label>
          <div className="admin-field-grid">
            {AD_DISTRIBUTIONS.map(({ id, label }) => (
              <label className="admin-toggle-row" key={id}>
                <span>
                  <strong>{label}</strong>
                  <small>Allow the server to schedule ads for this distribution.</small>
                </span>
                <input
                  type="checkbox"
                  aria-label={`Enable ads for ${label}`}
                  checked={draft.ads.distributions[id].enabled}
                  onChange={(event) => setDraft({
                    ...draft,
                    ads: {
                      ...draft.ads,
                      distributions: {
                        ...draft.ads.distributions,
                        [id]: {
                          ...draft.ads.distributions[id],
                          enabled: event.target.checked,
                        },
                      },
                    },
                  })}
                />
              </label>
            ))}
            <label className="admin-field">
              <span>Minimum games played</span>
              <span className="admin-number-input">
                <input
                  type="number"
                  min={0}
                  max={10000}
                  step={1}
                  value={draft.ads.minimumGamesPlayed}
                  onChange={(event) => setDraft({
                    ...draft,
                    ads: { ...draft.ads, minimumGamesPlayed: numberValue(event.target.value) },
                  })}
                />
                <em>games</em>
              </span>
              <small>Every member of the lobby must have completed at least this many games.</small>
            </label>
            <label className="admin-field">
              <span>Minimum interval</span>
              <span className="admin-number-input">
                <input
                  type="number"
                  min={1}
                  max={1440}
                  step={1}
                  value={draft.ads.minimumIntervalMinutes}
                  onChange={(event) => setDraft({
                    ...draft,
                    ads: { ...draft.ads, minimumIntervalMinutes: numberValue(event.target.value) },
                  })}
                />
                <em>minutes</em>
              </span>
              <small>
                The server enforces this cooldown per targeted player; if any targeted lobby
                member is still inside it, the whole lobby skips the break.
              </small>
            </label>
            <small className="admin-retention-note">
              Deployment settings choose the provider for each distribution. These controls only
              authorize server-scheduled lobby ad breaks within those capabilities.
            </small>
          </div>
        </fieldset>

        <fieldset>
          <legend>History retention</legend>
          <div className="admin-field-grid">
            <label className="admin-field">
              <span>Game snapshots</span>
              <span className="admin-number-input">
                <input
                  type="number"
                  min={1}
                  max={3650}
                  step={1}
                  value={draft.history.snapshotRetentionDays}
                  onChange={(event) => setDraft({
                    ...draft,
                    history: {
                      ...draft.history,
                      snapshotRetentionDays: numberValue(event.target.value),
                    },
                  })}
                />
                <em>days</em>
              </span>
            </label>
            <label className="admin-field">
              <span>Match summaries</span>
              <span className="admin-number-input">
                <input
                  type="number"
                  min={1}
                  max={3650}
                  step={1}
                  value={draft.history.summaryRetentionDays}
                  onChange={(event) => setDraft({
                    ...draft,
                    history: {
                      ...draft.history,
                      summaryRetentionDays: numberValue(event.target.value),
                    },
                  })}
                />
                <em>days</em>
              </span>
            </label>
            <small className="admin-retention-note">
              Applies to newly completed games; existing expiration dates stay unchanged.
            </small>
          </div>
        </fieldset>

        {status && (
          <p className={`admin-save-status is-${status.tone}`} role={status.tone === 'error' ? 'alert' : 'status'}>
            {status.message}
          </p>
        )}
        <div className="admin-form-actions">
          <button type="button" onClick={() => void reload()} disabled={saving}>Reload</button>
          <button type="submit" className="is-primary" disabled={saving}>
            {saving ? 'Saving…' : 'Publish configuration'}
          </button>
        </div>
      </form>
    </section>
  );
};

const AdminAudit: React.FC = () => {
  const [page, setPage] = useState<RuntimeConfigAuditPage>({ entries: [], nextCursor: null });
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async (cursor: string | null, append: boolean) => {
    append ? setLoadingMore(true) : setLoading(true);
    setError(null);
    try {
      const next = await api.getAdminRuntimeConfigAudit(cursor);
      setPage((current) => ({
        entries: append ? [...current.entries, ...next.entries] : next.entries,
        nextCursor: next.nextCursor,
      }));
    } catch (nextError) {
      setError(errorMessage(nextError));
    } finally {
      setLoading(false);
      setLoadingMore(false);
    }
  }, []);

  useEffect(() => {
    void load(null, false);
  }, [load]);

  return (
    <section className="admin-section" aria-labelledby="admin-audit-title">
      <div className="admin-section-heading">
        <div>
          <p className="admin-eyebrow">Operator changes</p>
          <h2 id="admin-audit-title">Audit</h2>
        </div>
        <p>Every published runtime configuration remains attributable and reviewable.</p>
      </div>
      {loading ? (
        <div className="admin-inline-status" role="status">Loading audit trail…</div>
      ) : error && page.entries.length === 0 ? (
        <div className="admin-inline-status is-error" role="alert">
          <span>{error}</span>
          <button type="button" onClick={() => void load(null, false)}>Try again</button>
        </div>
      ) : page.entries.length === 0 ? (
        <div className="admin-inline-status">No configuration changes have been recorded.</div>
      ) : (
        <>
          <ol className="admin-audit-list">
            {page.entries.map((entry, index) => (
              <li key={`${entry.version}-${entry.updatedAtMs}-${index}`}>
                <span className="admin-audit-node" aria-hidden="true" />
                <div>
                  <strong>Version {entry.version}</strong>
                  <span>{dateTime(entry.updatedAtMs)}</span>
                </div>
                <p>{entry.updatedBy
                  ? `${entry.updatedBy.username} (#${entry.updatedBy.userId})`
                  : 'System defaults'}</p>
                <dl>
                  <div><dt>Notice</dt><dd>{entry.config.announcement.enabled ? 'On' : 'Off'}</dd></div>
                  <div>
                    <dt>Pre-match ads</dt>
                    <dd>{entry.config.ads.enabled
                      ? `Enabled · ${enabledAdDistributions(entry.config).join(', ') || 'no distributions'} · ${entry.config.ads.minimumGamesPlayed}+ games · ${entry.config.ads.minimumIntervalMinutes}m interval`
                      : 'Disabled'}</dd>
                  </div>
                  <div><dt>Summary</dt><dd>{entry.config.history.summaryRetentionDays}d</dd></div>
                </dl>
              </li>
            ))}
          </ol>
          <div className="admin-audit-more">
            {error && <span role="alert">{error}</span>}
            {page.nextCursor && (
              <button
                type="button"
                disabled={loadingMore}
                onClick={() => void load(page.nextCursor, true)}
              >
                {loadingMore ? 'Loading…' : 'Load older changes'}
              </button>
            )}
          </div>
        </>
      )}
    </section>
  );
};

/**
 * The review queue.
 *
 * The validator has already enforced everything structural before a skin gets
 * here — palettes, contrast, the boost band, budgets. What is left is what a
 * machine cannot see: what a texture depicts, what a name says, and whether
 * either is someone else's property. So this view is deliberately about
 * *looking*, and the two decisions it offers are approve and take down.
 */
const AdminSkins: React.FC = () => {
  const [skins, setSkins] = useState<SkinSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [busyId, setBusyId] = useState<number | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const page = await api.getSkinReviewQueue();
      setSkins(page.skins);
    } catch (nextError) {
      setError(errorMessage(nextError));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const decide = useCallback(
    async (
      skin: SkinSummary,
      publication: 'published' | 'unpublished' | 'disabled' | 'private',
    ) => {
      setBusyId(skin.skinId);
      setError(null);
      try {
        await api.setSkinPublication(skin.skinId, publication, {
          // Approve what was submitted, not whatever the head is now: the
          // creator may have pushed a revision since asking.
          revision: publication === 'published'
            ? skin.pendingRevision ?? undefined
            : undefined,
        });
        await load();
      } catch (nextError) {
        setError(errorMessage(nextError));
      } finally {
        setBusyId(null);
      }
    },
    [load],
  );

  return (
    <section className="admin-section" aria-labelledby="admin-skins-title">
      <div className="admin-section-heading">
        <div>
          <p className="admin-eyebrow">Player content</p>
          <h2 id="admin-skins-title">Skins</h2>
        </div>
        <p>
          Everything structural has already been checked. Look at what the
          machine cannot: what it depicts, what it says, and whose it is.
        </p>
      </div>

      {error ? (
        <div className="admin-inline-status is-error" role="alert">
          <span>{error}</span>
          <button type="button" onClick={() => void load()}>Try again</button>
        </div>
      ) : null}

      {loading ? (
        <div className="admin-inline-status" role="status">Loading the review queue…</div>
      ) : skins.length === 0 ? (
        <div className="admin-inline-status">Nothing is waiting for review.</div>
      ) : (
        <ul className="admin-skin-queue">
          {skins.map((skin) => (
            <li key={skin.skinId} data-testid={`admin-skin-${skin.skinId}`}>
              <div className="admin-skin-preview">
                {skin.contentRef ? (
                  <AdminSkinPreview contentRef={skin.contentRef} name={skin.name} />
                ) : (
                  <span className="admin-skin-preview-missing">No preview</span>
                )}
              </div>
              <div className="admin-skin-meta">
                <strong>{skin.name}</strong>
                <span>
                  {skin.creatorUsername ?? `user #${skin.creatorUserId}`}
                  {' · '}
                  revision {skin.pendingRevision ?? skin.headRevision}
                  {' · '}
                  {skin.publication}
                </span>
              </div>
              <div className="admin-skin-actions">
                <button
                  type="button"
                  className="game-shell-button is-primary"
                  disabled={busyId === skin.skinId}
                  onClick={() => void decide(skin, 'published')}
                >
                  Approve
                </button>
                <button
                  type="button"
                  className="game-shell-button"
                  disabled={busyId === skin.skinId}
                  onClick={() => void decide(skin, 'private')}
                >
                  Reject
                </button>
                {/* Distinct from reject on purpose: this one reaches replays
                    and everyone already wearing it. */}
                <button
                  type="button"
                  className="game-shell-button is-destructive"
                  disabled={busyId === skin.skinId}
                  onClick={() => void decide(skin, 'disabled')}
                >
                  Take down
                </button>
              </div>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
};

/**
 * One queued skin, painted by the real renderer.
 *
 * A reviewer has to see what players will see, so this fetches and registers
 * the document exactly the way a match does rather than approximating it from
 * the skin's metadata.
 */
const AdminSkinPreview: React.FC<{ contentRef: string; name: string }> = ({
  contentRef,
  name,
}) => {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      await initWasm();
      ensureAuthoredSkins({ 0: contentRef });
      // Registration is a fetch away; poll briefly rather than blocking the
      // whole queue on one slow document.
      for (let attempt = 0; attempt < 20 && !cancelled; attempt += 1) {
        const wasm = getWasm();
        if (wasm?.authoredSkinIsRegistered(contentRef)) {
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 150));
      }
      const wasm = getWasm();
      const canvas = canvasRef.current;
      if (cancelled || !wasm || !canvas) {
        return;
      }
      try {
        wasm.renderSkinFixture(
          canvas,
          contentRef,
          'straight_horizontal',
          'own',
          12,
          false,
          false,
          640,
          true,
        );
      } catch {
        // An unpaintable skin is itself review-relevant; the empty tile says so.
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [contentRef]);

  return (
    <canvas
      ref={canvasRef}
      width={150}
      height={70}
      role="img"
      aria-label={`${name} preview`}
    />
  );
};

const AdminPage: React.FC = () => {
  const { user } = useAuth();
  const [section, setSection] = useState<AdminSection>('overview');
  const [record, setRecord] = useState<RuntimeConfigRecord | null>(null);
  const recordVersion = useRef(-1);
  const [loadingConfig, setLoadingConfig] = useState(true);
  const [configError, setConfigError] = useState<string | null>(null);

  const applyRecord = useCallback((nextRecord: RuntimeConfigRecord) => {
    // A slow reload must never overwrite a newer record returned by a save.
    if (nextRecord.version < recordVersion.current) return;
    recordVersion.current = nextRecord.version;
    setRecord(nextRecord);
  }, []);

  const loadConfig = useCallback(async () => {
    setLoadingConfig(true);
    setConfigError(null);
    try {
      applyRecord(await api.getAdminRuntimeConfig());
    } catch (error) {
      setConfigError(errorMessage(error));
    } finally {
      setLoadingConfig(false);
    }
  }, [applyRecord]);

  useEffect(() => {
    void loadConfig();
  }, [loadConfig]);

  const activeSection = useMemo(() => {
    if (section === 'history') return <AdminHistory />;
    if (section === 'configuration') {
      return (
        <AdminConfiguration
          record={record}
          setRecord={applyRecord}
          reload={loadConfig}
        />
      );
    }
    if (section === 'skins') return <AdminSkins />;
    if (section === 'audit') return <AdminAudit />;
    return <AdminOverview record={record} />;
  }, [loadConfig, record, section]);

  return (
    <main className="admin-page">
      <header className="admin-header">
        <Link to="/" className="admin-wordmark" aria-label="Snaketron home">
          <span>Snaketron</span>
          <small>Control room</small>
        </Link>
        <div className="admin-operator">
          <span>Operator</span>
          <strong>{user?.username}</strong>
          <Link to="/">Return to arena</Link>
        </div>
      </header>

      <div className="admin-shell">
        <nav className="admin-arena-rail" aria-label="Admin sections">
          {SECTION_LABELS.map((item, index) => (
            <button
              key={item.id}
              type="button"
              aria-label={item.label}
              className={section === item.id ? 'is-active' : undefined}
              aria-current={section === item.id ? 'page' : undefined}
              onClick={() => setSection(item.id)}
            >
              <span className="admin-rail-node" aria-hidden="true">{String(index + 1).padStart(2, '0')}</span>
              <span className="admin-rail-label admin-rail-label-full" aria-hidden="true">{item.label}</span>
              <span className="admin-rail-label admin-rail-label-compact" aria-hidden="true">{item.compactLabel}</span>
            </button>
          ))}
        </nav>

        {configError && (
          <div className="admin-config-alert" role="alert">
            <span>{configError}</span>
            <button type="button" onClick={() => void loadConfig()}>Retry</button>
          </div>
        )}
        {loadingConfig && !record && section !== 'history' && section !== 'audit'
          ? <div className="admin-inline-status" role="status">Loading control record…</div>
          : activeSection}
      </div>
    </main>
  );
};

export default AdminPage;
