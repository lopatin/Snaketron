import React, { FormEvent, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { useRuntimeConfig } from '../contexts/RuntimeConfigContext';
import {
  api,
  isApiError,
} from '../services/api';
import type { RuntimeConfig, RuntimeConfigAuditPage, RuntimeConfigRecord } from '../types';
import type { AdminSkinReview } from '../types/generated';
import { MatchHistoryList } from './MatchHistoryList';
import { getWasm, initWasm, whenSkinAssetsSettle } from '../wasm';
import { ensureAuthoredSkins } from '../utils/authoredSkins';
import {
  DEFAULT_SKIN_PERIOD_MS,
  adminPublishDisabled,
  adminPreviewTargetKey,
  adminRejectActionLabel,
  advanceSkinTimeline,
  initialSkinPreviewPlaying,
  shortContentRef,
  skinAnimationPeriodMs,
  skinDocumentUsesImages,
  skinPreviewAssetError,
} from '../utils/adminSkinPreview';
import type { AdminSkinPreviewState, SkinAssetStatus } from '../utils/adminSkinPreview';

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
 * *looking*, with every decision tied to the immutable bytes on screen.
 */
const AdminSkins: React.FC = () => {
  const [skins, setSkins] = useState<AdminSkinReview[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [busyId, setBusyId] = useState<number | null>(null);
  const [reviewNotes, setReviewNotes] = useState<Record<number, string>>({});
  const [previewStates, setPreviewStates] = useState<Record<string, AdminSkinPreviewState>>({});

  const recordPreviewState = useCallback((
    targetKey: string,
    previewState: AdminSkinPreviewState,
  ) => {
    setPreviewStates((current) => (
      current[targetKey] === previewState
        ? current
        : { ...current, [targetKey]: previewState }
    ));
  }, []);

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
      skin: AdminSkinReview,
      publication: 'published' | 'unpublished' | 'disabled' | 'private',
    ) => {
      setBusyId(skin.skinId);
      setError(null);
      try {
        const reason = reviewNotes[skin.skinId]?.trim() || undefined;
        await api.setSkinPublication(
          skin.skinId,
          publication === 'published'
            ? {
                decision: 'publish',
                // Publish the bytes on screen, not a creator's moving head.
                revision: skin.pendingRevision,
                contentRef: skin.pendingContentRef,
                reason,
              }
            : publication === 'private'
              ? {
                  decision: 'reject',
                  revision: skin.pendingRevision,
                  contentRef: skin.pendingContentRef,
                  reason,
                }
              : { decision: 'setPublication', publication, reason },
        );
        await load();
      } catch (nextError) {
        setError(errorMessage(nextError));
      } finally {
        setBusyId(null);
      }
    },
    [load, reviewNotes],
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
                <AdminSkinPreview
                  contentRef={skin.pendingContentRef}
                  name={skin.name}
                  targetKey={adminPreviewTargetKey(
                    skin.skinId,
                    skin.pendingRevision,
                    skin.pendingContentRef,
                  )}
                  onStateChange={recordPreviewState}
                />
              </div>
              <div className="admin-skin-meta">
                <strong>{skin.name}</strong>
                <span>
                  {skin.creatorUsername ?? `user #${skin.creatorUserId}`}
                  {' · '}
                  {skin.publication}
                </span>
                <span
                  className="admin-skin-target"
                  title={skin.pendingContentRef}
                  data-testid={`admin-skin-target-${skin.skinId}`}
                >
                  Pending revision {skin.pendingRevision}
                  {' · '}
                  {shortContentRef(skin.pendingContentRef)}
                </span>
              </div>
              <div className="admin-skin-actions">
                <label className="admin-skin-review-note">
                  <span>Review note</span>
                  <input
                    type="text"
                    value={reviewNotes[skin.skinId] ?? ''}
                    maxLength={500}
                    placeholder="Optional audit reason"
                    onChange={(event) => setReviewNotes((current) => ({
                      ...current,
                      [skin.skinId]: event.target.value,
                    }))}
                  />
                </label>
                <button
                  type="button"
                  className="game-shell-button is-primary"
                  disabled={adminPublishDisabled(
                    busyId === skin.skinId,
                    previewStates[adminPreviewTargetKey(
                      skin.skinId,
                      skin.pendingRevision,
                      skin.pendingContentRef,
                    )],
                  )}
                  title={
                    previewStates[adminPreviewTargetKey(
                      skin.skinId,
                      skin.pendingRevision,
                      skin.pendingContentRef,
                    )] === 'ready'
                      ? undefined
                      : 'Publish is available once the exact preview renders successfully.'
                  }
                  onClick={() => void decide(skin, 'published')}
                >
                  Publish this revision
                </button>
                <button
                  type="button"
                  className="game-shell-button"
                  disabled={busyId === skin.skinId}
                  onClick={() => void decide(skin, 'private')}
                >
                  {adminRejectActionLabel(skin.publishedRevision)}
                </button>
                {/* Distinct from reject on purpose: this one reaches replays
                    and everyone already wearing it. */}
                {skin.publishedRevision !== null ? (
                  <button
                    type="button"
                    className="game-shell-button is-destructive"
                    disabled={busyId === skin.skinId}
                    onClick={() => void decide(skin, 'disabled')}
                  >
                    Take down published skin
                  </button>
                ) : null}
              </div>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
};

const ADMIN_PREVIEW_CELL = 14;
const ADMIN_PREVIEW_PAD = 8;

interface AdminSkinPreviewProps {
  contentRef: string;
  name: string;
  targetKey: string;
  onStateChange: (targetKey: string, state: AdminSkinPreviewState) => void;
}

const previewFailureMessage = (cause: unknown): string => (
  cause instanceof Error && cause.message
    ? cause.message
    : 'The exact skin document could not be rendered.'
);

/**
 * One queued skin, painted by the real renderer.
 *
 * A reviewer has to see what players will see, so this fetches and registers
 * the document exactly the way a match does, and frames it the way the browse
 * page does — a long body, cropped to itself. Motion begins on explicit play
 * so a 50-item queue does not silently run 50 render loops at once.
 */
const AdminSkinPreview: React.FC<AdminSkinPreviewProps> = ({
  contentRef,
  name,
  targetKey,
  onStateChange,
}) => {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const timelineRef = useRef<HTMLInputElement | null>(null);
  const timelineOutputRef = useRef<HTMLOutputElement | null>(null);
  const timeMsRef = useRef(0);
  const [periodMs, setPeriodMs] = useState(DEFAULT_SKIN_PERIOD_MS);
  const [previewState, setPreviewState] = useState<AdminSkinPreviewState>('loading');
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [loadedDocument, setLoadedDocument] = useState<{
    contentRef: string;
    usesImages: boolean;
  } | null>(null);
  const [prefersReducedMotion, setPrefersReducedMotion] = useState(() => (
    typeof window !== 'undefined'
    && typeof window.matchMedia === 'function'
    && window.matchMedia('(prefers-reduced-motion: reduce)').matches
  ));
  const [playing, setPlaying] = useState(initialSkinPreviewPlaying);
  const [layout, setLayout] = useState({
    canvasWidth: 320,
    canvasHeight: 90,
    cropWidth: 300,
    cropHeight: 40,
    offsetX: 0,
    offsetY: 48,
  });

  const updatePreviewState = useCallback((
    state: AdminSkinPreviewState,
    message: string | null = null,
  ) => {
    setPreviewState(state);
    setPreviewError(message);
    onStateChange(targetKey, state);
  }, [onStateChange, targetKey]);

  const paint = useCallback((timeMs: number) => {
    const wasm = getWasm();
    const canvas = canvasRef.current;
    if (!wasm || !canvas) {
      throw new Error('The skin renderer is unavailable.');
    }
    wasm.renderSkinFixture(
      canvas,
      contentRef,
      'longer_than_head_gradient',
      'own',
      ADMIN_PREVIEW_CELL,
      false,
      false,
      timeMs,
      false,
    );
  }, [contentRef]);

  const failPreview = useCallback((cause: unknown) => {
    setPlaying(false);
    updatePreviewState('error', previewFailureMessage(cause));
  }, [updatePreviewState]);

  useEffect(() => {
    if (typeof window.matchMedia !== 'function') return undefined;
    const query = window.matchMedia('(prefers-reduced-motion: reduce)');
    const onChange = (event: MediaQueryListEvent) => {
      setPrefersReducedMotion(event.matches);
      if (event.matches) setPlaying(false);
    };
    query.addEventListener('change', onChange);
    return () => query.removeEventListener('change', onChange);
  }, []);

  useEffect(() => {
    let cancelled = false;
    updatePreviewState('loading');
    setLoadedDocument(null);
    setPlaying(false);
    timeMsRef.current = 0;
    void (async () => {
      try {
        await initWasm();
        const [document] = await Promise.all([
          api.getSkinDocument(contentRef),
          ensureAuthoredSkins({ 0: contentRef }),
        ]);
        const wasm = getWasm();
        if (!wasm?.authoredSkinIsRegistered(contentRef)) {
          throw new Error('The exact skin document could not be registered.');
        }
        const bounds = JSON.parse(
          wasm.skinFixtureBounds(
            contentRef,
            'longer_than_head_gradient',
            ADMIN_PREVIEW_CELL,
            false,
          ),
        ) as { x: number; y: number; width: number; height: number };
        if (
          !Number.isFinite(bounds.width)
          || !Number.isFinite(bounds.height)
          || bounds.width <= 0
          || bounds.height <= 0
        ) {
          throw new Error('The renderer returned no visible preview bounds.');
        }
        if (cancelled) return;
        setPeriodMs(skinAnimationPeriodMs(document));
        setLayout({
          canvasWidth: Math.ceil(bounds.x + bounds.width + ADMIN_PREVIEW_PAD),
          canvasHeight: Math.ceil(bounds.y + bounds.height + ADMIN_PREVIEW_PAD),
          cropWidth: Math.ceil(bounds.width + ADMIN_PREVIEW_PAD * 2),
          cropHeight: Math.ceil(bounds.height + ADMIN_PREVIEW_PAD * 2),
          offsetX: Math.round(bounds.x - ADMIN_PREVIEW_PAD),
          offsetY: Math.round(bounds.y - ADMIN_PREVIEW_PAD),
        });
        setLoadedDocument({
          contentRef,
          usesImages: skinDocumentUsesImages(document),
        });
      } catch (cause) {
        if (!cancelled) failPreview(cause);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [contentRef, failPreview, updatePreviewState]);

  const syncTimeline = useCallback((timeMs: number) => {
    const timeline = timelineRef.current;
    if (timeline) {
      timeline.value = String(Math.round(timeMs));
      timeline.setAttribute('aria-valuetext', `${(timeMs / 1_000).toFixed(2)} seconds`);
    }
    if (timelineOutputRef.current) {
      timelineOutputRef.current.textContent = `${(timeMs / 1_000).toFixed(2)}s`;
    }
  }, []);

  // A first paint requests lazy textures. Only the repaint after those assets
  // settle can prove that the exact submitted pixels, rather than a procedural
  // fallback, reached this canvas.
  useEffect(() => {
    if (loadedDocument?.contentRef !== contentRef) return undefined;
    let cancelled = false;
    void (async () => {
      try {
        paint(0);
        await whenSkinAssetsSettle(contentRef);
        if (cancelled) return;
        const wasm = getWasm();
        if (!wasm) throw new Error('The skin renderer is unavailable.');
        const beforeFinalPaint = JSON.parse(wasm.skinAssetStatus(contentRef)) as SkinAssetStatus;
        paint(0);
        const finalStatus = JSON.parse(wasm.skinAssetStatus(contentRef)) as SkinAssetStatus;
        const assetError = skinPreviewAssetError(
          finalStatus,
          loadedDocument.usesImages,
          beforeFinalPaint.drawCalls,
        );
        if (assetError) throw new Error(assetError);
        if (!cancelled) updatePreviewState('ready');
      } catch (cause) {
        if (!cancelled) failPreview(cause);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [
    contentRef,
    failPreview,
    layout.canvasHeight,
    layout.canvasWidth,
    loadedDocument,
    paint,
    updatePreviewState,
  ]);

  useEffect(() => {
    timeMsRef.current %= periodMs;
    syncTimeline(timeMsRef.current);
  }, [periodMs, syncTimeline]);

  useEffect(() => {
    if (!playing || previewState !== 'ready') return undefined;
    let previous: number | null = null;
    let frame = requestAnimationFrame(function loop(now: number) {
      if (previous !== null) {
        timeMsRef.current = advanceSkinTimeline(timeMsRef.current, now - previous, periodMs);
        syncTimeline(timeMsRef.current);
        try {
          paint(timeMsRef.current);
        } catch (cause) {
          failPreview(cause);
          return;
        }
      }
      previous = now;
      frame = requestAnimationFrame(loop);
    });
    return () => cancelAnimationFrame(frame);
  }, [failPreview, paint, periodMs, playing, previewState, syncTimeline]);

  const scrub = useCallback((event: React.FormEvent<HTMLInputElement>) => {
    const next = Number(event.currentTarget.value);
    if (!Number.isFinite(next)) return;
    timeMsRef.current = next;
    syncTimeline(next);
    try {
      paint(next);
    } catch (cause) {
      failPreview(cause);
    }
  }, [failPreview, paint, syncTimeline]);

  return (
    <div className="admin-skin-specimen">
      <div
        className="admin-skin-crop"
        style={{ width: layout.cropWidth, height: layout.cropHeight }}
      >
        <canvas
          ref={canvasRef}
          width={layout.canvasWidth}
          height={layout.canvasHeight}
          style={{ marginLeft: -layout.offsetX, marginTop: -layout.offsetY }}
          role="img"
          aria-label={`${name} animated preview`}
        />
        {previewState !== 'ready' ? (
          <span
            className={`admin-skin-preview-state is-${previewState}`}
            role={previewState === 'error' ? 'alert' : 'status'}
          >
            {previewState === 'error' ? 'Preview unavailable' : 'Loading exact preview…'}
          </span>
        ) : null}
      </div>
      <div className="admin-skin-motion">
        <button
          type="button"
          className="admin-skin-play"
          aria-label={playing ? `Pause ${name} animation` : `Play ${name} animation`}
          aria-pressed={playing}
          disabled={previewState !== 'ready'}
          onClick={() => setPlaying((current) => !current)}
        >
          {playing ? 'Pause' : 'Play'}
        </button>
        <input
          ref={timelineRef}
          type="range"
          min="0"
          max={Math.max(1, Math.round(periodMs))}
          step="1"
          defaultValue="0"
          aria-label={`${name} animation timeline`}
          disabled={previewState !== 'ready'}
          onInput={scrub}
        />
        <output ref={timelineOutputRef}>0.00s</output>
      </div>
      <span
        className={`admin-skin-motion-note${prefersReducedMotion ? ' is-reduced' : ''}${previewState === 'error' ? ' is-error' : ''}`}
      >
        {previewState === 'error'
          ? `Preview unavailable · ${previewError ?? 'render failed'}`
          : prefersReducedMotion
            ? 'Reduced motion detected · autoplay paused'
            : previewState === 'loading'
              ? 'Verifying exact document and assets…'
              : `${(periodMs / 1_000).toFixed(2)}s loop`}
      </span>
    </div>
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
