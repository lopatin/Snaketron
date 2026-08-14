import React, { FormEvent, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { useRuntimeConfig } from '../contexts/RuntimeConfigContext';
import {
  api,
  isApiError,
} from '../services/api';
import type { RuntimeConfig, RuntimeConfigAuditPage, RuntimeConfigRecord } from '../types';
import { MatchHistoryList } from './MatchHistoryList';

type AdminSection = 'overview' | 'history' | 'configuration' | 'audit';

const SECTION_LABELS: Array<{ id: AdminSection; label: string; compactLabel: string }> = [
  { id: 'overview', label: 'Overview', compactLabel: 'Overview' },
  { id: 'history', label: 'Match history', compactLabel: 'Matches' },
  { id: 'configuration', label: 'Configuration', compactLabel: 'Config' },
  { id: 'audit', label: 'Audit', compactLabel: 'Audit' },
];

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
          <dt>Post-match ads</dt>
          <dd data-tone={config?.ads.postMatchEnabled ? 'active' : 'quiet'}>
            {config?.ads.postMatchEnabled ? 'Allowed' : 'Killed'}
          </dd>
          <p>{config
            ? `${config.ads.minimumIntervalMinutes} minute minimum interval. Build flags remain authoritative.`
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
          <legend>Advertising</legend>
          <label className="admin-toggle-row">
            <span>
              <strong>Allow post-match ads</strong>
              <small>This can only disable a build-time ad capability; it cannot enable one.</small>
            </span>
            <input
              type="checkbox"
              checked={draft.ads.postMatchEnabled}
              onChange={(event) => setDraft({
                ...draft,
                ads: { ...draft.ads, postMatchEnabled: event.target.checked },
              })}
            />
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
          </label>
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
                  <div><dt>Ads</dt><dd>{entry.config.ads.postMatchEnabled ? 'Allowed' : 'Killed'}</dd></div>
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
