import React, { createContext, useContext, useState, useEffect, useRef, useCallback, useMemo } from 'react';
import {
  WebSocketContextType,
  Lobby,
  LobbyMember,
  ChatMessage,
  ChatScope,
  LobbyPreferences,
  LobbyGameMode,
  LobbyState,
  User,
} from '../types';
import { clockSync } from '../utils/clockSync';
import { record as recordTrace } from '../utils/syncTrace';
import { useLatency } from './LatencyContext';
import { useAuth } from './AuthContext';
import {
  detectBestRegion,
  fetchRegionMetadata,
  loadRegionPreference,
  saveRegionPreference,
} from '../utils/regionPreference';
import {
  DEFAULT_LOBBY_PREFERENCES,
  loadStoredLobbyPreferences,
  persistStoredLobbyPreferences,
  sanitizeClientLobbyPreferences,
} from '../utils/lobbyPreferencesStorage';
import {
  advanceCandidateGameWatermark,
  candidateDeadlineDelayMs,
  activeGameIdFromPath,
  isCommandOwner,
  isSnapshotForGame,
  isTerminalSnapshotForGame,
  missingRequiredServerCapabilities,
  plannedDrainRemainingMs,
  reconnectDelayMs,
  replacementFailureAction,
  replacementReadyForPromotion,
} from '../services/websocketLifecycle';

interface WebSocketProviderProps {
  children: React.ReactNode;
}

interface MessageHandler {
  (message: { type: string; data: any }): void;
}

type SocketRole = 'active' | 'candidate' | 'retired';

interface SocketSlot {
  socket: WebSocket;
  generation: number;
  role: SocketRole;
  url: string;
  authenticated: boolean;
  capabilities: string[];
  authTokenSent: string | null;
  bufferedMessages: any[];
  expectedLobbyCode: string | null;
  expectedGameId: number | null;
  lobbyReady: boolean;
  gameReady: boolean;
  gameComplete: boolean;
  gameStreamWatermark: number | null;
  commandOutcomesReady: boolean;
  drainDeadlineMs: number | null;
  candidateAttempt: number;
  authStartedAtMs: number | null;
  authTimeoutId: ReturnType<typeof setTimeout> | null;
  gameWarmRetryTimeoutId: ReturnType<typeof setTimeout> | null;
  contextRestoreStartedAtMs: number | null;
  continuityProbeClientTime: number | null;
  continuityProbeActiveGeneration: number | null;
  continuityProbeConfirmed: boolean;
}

// Extend window interface for testing
declare global {
  interface Window {
    __wsInstance?: WebSocket;
    __wsContext?: WebSocketContextType;
  }
}

const WebSocketContext = createContext<WebSocketContextType | null>(null);

const LOBBY_STORAGE_KEY = 'snaketron:lastLobby';
const MAX_CHAT_HISTORY = 200;
const VALID_LOBBY_MODES: LobbyGameMode[] = ['duel', '2v2', 'solo', 'ffa'];
const VALID_LOBBY_STATES: LobbyState[] = ['waiting', 'queued', 'matched'];
const MAX_RECOVERY_METRIC_MS = 5 * 60 * 1000;
const AUTHENTICATION_TIMEOUT_MS = 5_000;

const clearAuthenticationTimeout = (slot: SocketSlot) => {
  if (slot.authTimeoutId !== null) {
    clearTimeout(slot.authTimeoutId);
    slot.authTimeoutId = null;
  }
};

const clearGameWarmRetryTimeout = (slot: SocketSlot) => {
  if (slot.gameWarmRetryTimeoutId !== null) {
    clearTimeout(slot.gameWarmRetryTimeoutId);
    slot.gameWarmRetryTimeoutId = null;
  }
};

const clearContinuityProof = (slot: SocketSlot) => {
  slot.continuityProbeClientTime = null;
  slot.continuityProbeActiveGeneration = null;
  slot.continuityProbeConfirmed = false;
};

const boundedMetricDuration = (startedAtMs: number, nowMs: number = Date.now()): number =>
  Math.max(0, Math.min(MAX_RECOVERY_METRIC_MS, Math.round(nowMs - startedAtMs)));

const recordWsMetric = (
  name: string,
  fields: Record<string, string | number>,
  nowMs: number = Date.now(),
) => {
  const details = Object.entries(fields)
    .map(([key, value]) => `${key}=${value}`)
    .join(' ');
  recordTrace({ Note: { ts_ms: nowMs, note: `ws_metric name=${name} ${details}` } });
};

const normalizeLobbyPreferences = (payload: any): LobbyPreferences => {
  const rawModes = Array.isArray(payload.selected_modes ?? payload.selectedModes)
    ? payload.selected_modes ?? payload.selectedModes
    : [];

  const normalized = new Set<LobbyGameMode>();
  for (const value of rawModes) {
    if (typeof value !== 'string') {
      continue;
    }
    const lower = value.trim().toLowerCase();
    if (VALID_LOBBY_MODES.includes(lower as LobbyGameMode)) {
      normalized.add(lower as LobbyGameMode);
    }
  }

  const ordered: LobbyGameMode[] = [];
  for (const mode of VALID_LOBBY_MODES) {
    if (normalized.has(mode)) {
      ordered.push(mode);
    }
  }

  return {
    selectedModes: ordered,
    competitive: Boolean(payload.competitive),
  };
};

interface StoredLobbyInfo {
  code: string;
  id?: number;
}

export const useWebSocket = (): WebSocketContextType => {
  const context = useContext(WebSocketContext);
  if (!context) {
    throw new Error('useWebSocket must be used within WebSocketProvider');
  }
  return context;
};

export const WebSocketProvider: React.FC<WebSocketProviderProps> = ({ children }) => {
  const storedPreferences = useMemo(() => loadStoredLobbyPreferences(), []);
  const [isConnected, setIsConnected] = useState(false);
  const [latencyMs, setLatencyMs] = useState<number>(0);
  const [currentRegionUrl, setCurrentRegionUrl] = useState<string | null>(null);
  const [currentLobby, setCurrentLobby] = useState<Lobby | null>(null);
  const [lobbyMembers, setLobbyMembers] = useState<LobbyMember[]>([]);
  const [lobbyChatMessages, setLobbyChatMessages] = useState<ChatMessage[]>([]);
  const [gameChatMessages, setGameChatMessages] = useState<ChatMessage[]>([]);
  const [lobbyPreferences, setLobbyPreferences] = useState<LobbyPreferences | null>(storedPreferences);
  const [isSessionAuthenticated, setIsSessionAuthenticated] = useState(false);
  const [serverCapabilities, setServerCapabilities] = useState<ReadonlySet<string>>(new Set());
  const currentLobbyRef = useRef<Lobby | null>(null);
  const desiredLobbyPreferencesRef = useRef<LobbyPreferences | null>(storedPreferences);
  const ws = useRef<WebSocket | null>(null);
  const activeSlotRef = useRef<SocketSlot | null>(null);
  const candidateSlotRef = useRef<SocketSlot | null>(null);
  const nextGenerationRef = useRef(0);
  const nextContinuityProbeClientTimeRef = useRef(0);
  const reconnectAttemptRef = useRef(0);
  const candidateAttemptRef = useRef(0);
  const candidateRetryTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const candidateDeadlineTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const reconnectEnabledRef = useRef(true);
  const openActiveRef = useRef<(url: string, onConnect?: () => void) => void>(() => {});
  const startCandidateRef = useRef<(deadlineMs: number) => void>(() => {});
  const messageHandlers = useRef<Map<string, MessageHandler[]>>(new Map());
  const reconnectTimeout = useRef<NodeJS.Timeout | null>(null);
  const inFlightRequestsByGenerationRef = useRef<Map<number, number>>(new Map());
  const latestGameStreamSeqRef = useRef<Map<number, number>>(new Map());
  const onConnectCallback = useRef<(() => void) | null>(null);
  const syncRequestTimes = useRef<Map<number, number>>(new Map());
  const isInitializingRef = useRef(false);
  const storedLobbyRef = useRef<StoredLobbyInfo | null>(null);
  const hasLoadedStoredLobbyRef = useRef(false);
  const restoreInProgressRef = useRef(false);
  const lobbyChatLobbyIdRef = useRef<number | null>(null);
  const gameChatIdRef = useRef<number | null>(null);
  const { settings: latencySettings } = useLatency();
  const latencySettingsRef = useRef(latencySettings);
  const { user, getToken } = useAuth();
  const hasEverConnectedRef = useRef(false);
  const authHandshakeRef = useRef(false);
  const lastAuthTokenRef = useRef<string | null>(null);
  const previousUserRef = useRef<User | null>(null);
  const plannedHandoffStartedAtRef = useRef<number | null>(null);
  const recoveryStartedAtRef = useRef<number | null>(null);
  const usableGapStartedAtRef = useRef<number | null>(null);

  useEffect(() => {
    latencySettingsRef.current = latencySettings;
  }, [latencySettings]);

  useEffect(() => {
    if (lobbyPreferences) {
      desiredLobbyPreferencesRef.current = lobbyPreferences;
    }
  }, [lobbyPreferences]);

  useEffect(() => {
    if (lobbyPreferences) {
      persistStoredLobbyPreferences(lobbyPreferences);
    }
  }, [lobbyPreferences]);

  const buildInitialLobbyPreferences = useCallback((): LobbyPreferences => {
    const fromRef = sanitizeClientLobbyPreferences(desiredLobbyPreferencesRef.current);
    if (fromRef) {
      return fromRef;
    }

    const stored = loadStoredLobbyPreferences();
    if (stored) {
      return stored;
    }

    return {
      selectedModes: [...DEFAULT_LOBBY_PREFERENCES.selectedModes],
      competitive: DEFAULT_LOBBY_PREFERENCES.competitive,
    };
  }, []);

  const setAuthHandshakeState = useCallback((value: boolean) => {
    if (authHandshakeRef.current !== value) {
      authHandshakeRef.current = value;
      setIsSessionAuthenticated(value);
    }
  }, []);

  const recordPlannedHandoffFailure = useCallback((reason: string, nowMs: number = Date.now()) => {
    const startedAtMs = plannedHandoffStartedAtRef.current;
    if (startedAtMs === null) {
      return;
    }
    recordWsMetric('planned_handoff_failure', {
      count: 1,
      duration_ms: boundedMetricDuration(startedAtMs, nowMs),
      reason,
    }, nowMs);
    plannedHandoffStartedAtRef.current = null;
  }, []);

  const completeActiveRecovery = useCallback((slot: SocketSlot, nowMs: number = Date.now()) => {
    if (
      activeSlotRef.current !== slot ||
      slot.role !== 'active' ||
      !slot.authenticated ||
      !slot.lobbyReady ||
      !slot.gameReady ||
      !slot.commandOutcomesReady
    ) {
      return;
    }
    const recoveryStartedAtMs = recoveryStartedAtRef.current;
    if (recoveryStartedAtMs !== null) {
      recordWsMetric('reconnect_duration_ms', {
        value: boundedMetricDuration(recoveryStartedAtMs, nowMs),
      }, nowMs);
      recoveryStartedAtRef.current = null;
    }
    const gapStartedAtMs = usableGapStartedAtRef.current;
    if (gapStartedAtMs !== null) {
      recordWsMetric('usable_session_gap_ms', {
        value: boundedMetricDuration(gapStartedAtMs, nowMs),
      }, nowMs);
      usableGapStartedAtRef.current = null;
    }
  }, []);

  useEffect(() => {
    if (hasLoadedStoredLobbyRef.current) {
      return;
    }

    if (typeof window === 'undefined') {
      hasLoadedStoredLobbyRef.current = true;
      return;
    }

    try {
      const raw = window.localStorage.getItem(LOBBY_STORAGE_KEY);
      if (raw) {
        const parsed = JSON.parse(raw);
        if (parsed && typeof parsed.code === 'string' && parsed.code.trim()) {
          storedLobbyRef.current = {
            code: parsed.code.toUpperCase(),
            id: typeof parsed.id === 'number' ? parsed.id : undefined,
          };
        }
      }
    } catch (error) {
      console.warn('Failed to load stored lobby info, clearing persisted data', error);
      try {
        window.localStorage.removeItem(LOBBY_STORAGE_KEY);
      } catch {
        // Ignore removal errors
      }
    } finally {
      hasLoadedStoredLobbyRef.current = true;
    }
  }, []);

  const persistLobby = useCallback((lobby: { id: number; code: string }) => {
    storedLobbyRef.current = { id: lobby.id, code: lobby.code.toUpperCase() };

    if (typeof window === 'undefined') {
      return;
    }

    try {
      window.localStorage.setItem(
        LOBBY_STORAGE_KEY,
        JSON.stringify({ id: lobby.id, code: lobby.code.toUpperCase() })
      );
    } catch (error) {
      console.warn('Failed to persist lobby info', error);
    }
  }, []);

  const clearPersistedLobby = useCallback(() => {
    storedLobbyRef.current = null;

    if (typeof window === 'undefined') {
      return;
    }

    try {
      window.localStorage.removeItem(LOBBY_STORAGE_KEY);
    } catch (error) {
      console.warn('Failed to clear stored lobby info', error);
    }
  }, []);

  const resetLobbyState = useCallback(() => {
    setCurrentLobby(null);
    currentLobbyRef.current = null;
    setLobbyMembers([]);
    // console.log('setLobbyPreferences', DEFAULT_LOBBY_PREFERENCES);
    // setLobbyPreferences(DEFAULT_LOBBY_PREFERENCES);
    clearPersistedLobby();
  }, [clearPersistedLobby]);

  const isLobbyMissingReason = useCallback((reason: string) => {
    if (!reason || typeof reason !== 'string') {
      return false;
    }
    const normalized = reason.toLowerCase();
    if (!normalized.includes('lobby')) {
      return false;
    }
    return (
      normalized.includes('not found') ||
      normalized.includes('does not exist') ||
      normalized.includes('missing')
    );
  }, []);

  const dispatchRawMessage = useCallback((slot: SocketSlot, rawMessage: any) => {
    if (activeSlotRef.current !== slot || slot.role !== 'active') {
      return;
    }

    console.log('WebSocket message received:', rawMessage);
    const gameId = Number(rawMessage?.GameEvent?.game_id);
    if (Number.isSafeInteger(gameId)) {
      const nextWatermark = advanceCandidateGameWatermark(
        latestGameStreamSeqRef.current.get(gameId) ?? null,
        rawMessage,
        gameId,
      );
      if (nextWatermark !== null) {
        // A takeover snapshot may legitimately re-anchor below events the old
        // owner published after its last checkpoint. Keeping Math.max here
        // would preserve a watermark from the retired authority and could
        // prevent a later healthy replacement socket from ever promoting.
        latestGameStreamSeqRef.current.set(gameId, nextWatermark);
      }
    }
    const nowMs = Date.now();
    if (
      !slot.lobbyReady &&
      slot.expectedLobbyCode &&
      (rawMessage?.JoinedLobby?.lobby_code?.toUpperCase() === slot.expectedLobbyCode ||
        rawMessage?.LobbyUpdate?.lobby_code?.toUpperCase() === slot.expectedLobbyCode)
    ) {
      slot.lobbyReady = true;
      if (slot.contextRestoreStartedAtMs !== null) {
        recordWsMetric('lobby_rejoin_latency_ms', {
          value: boundedMetricDuration(slot.contextRestoreStartedAtMs, nowMs),
          role: slot.role,
        }, nowMs);
      }
    }
    if (
      !slot.gameReady &&
      slot.expectedGameId !== null &&
      isSnapshotForGame(rawMessage, slot.expectedGameId)
    ) {
      slot.gameReady = true;
      if (slot.contextRestoreStartedAtMs !== null) {
        recordWsMetric('snapshot_rejoin_latency_ms', {
          value: boundedMetricDuration(slot.contextRestoreStartedAtMs, nowMs),
          role: slot.role,
        }, nowMs);
      }
    }
    if (
      !slot.commandOutcomesReady &&
      slot.expectedGameId !== null &&
      Number(rawMessage?.CommandOutcomesComplete?.game_id) === slot.expectedGameId
    ) {
      slot.commandOutcomesReady = true;
    }
    completeActiveRecovery(slot, nowMs);
    if (rawMessage?.Pong) {
      const { client_time, server_time } = rawMessage.Pong;
      const t1 = syncRequestTimes.current.get(client_time);
      if (t1) {
        syncRequestTimes.current.delete(client_time);
        const t3 = Date.now();
        const measurement = clockSync.processSyncResponse(t1, server_time, t3);
        recordTrace({
          Clock: {
            ts_ms: t3,
            drift_ms: measurement.offset,
            rtt_ms: measurement.rtt,
          },
        });
        setLatencyMs(Math.round((t3 - t1) / 2));
      }
      return;
    }

    let messageType: string | null = null;
    let messageData: any = undefined;
    if (typeof rawMessage === 'string') {
      messageType = rawMessage;
      messageData = null;
    } else if (rawMessage && typeof rawMessage === 'object') {
      const keys = Object.keys(rawMessage);
      if (keys.length === 1) {
        messageType = keys[0];
        messageData = rawMessage[messageType];
      }
    }
    if (!messageType) {
      console.warn('Unexpected WebSocket message shape', rawMessage);
      return;
    }
    const handlers = [...(messageHandlers.current.get(messageType) || [])];
    handlers.forEach((handler: MessageHandler) => {
      try {
        handler({ type: messageType!, data: messageData });
      } catch (error) {
        console.error(`WebSocket ${messageType} handler failed:`, error);
      }
    });
  }, [completeActiveRecovery]);

  const configureClockSync = useCallback((slot: SocketSlot) => {
    clockSync.setOnSyncRequest((clientTime) => {
      if (
        activeSlotRef.current === slot &&
        slot.role === 'active' &&
        slot.socket.readyState === WebSocket.OPEN
      ) {
        syncRequestTimes.current.set(clientTime, clientTime);
        slot.socket.send(JSON.stringify({ Ping: { client_time: clientTime } }));
      }
    });
    clockSync.start();
  }, []);

  const promoteCandidate = useCallback((
    candidate: SocketSlot,
    allowUnprovenTransportOverlap: boolean = false,
  ) => {
    if (candidateSlotRef.current !== candidate) {
      return;
    }
    const previous = activeSlotRef.current;
    const inFlightRequestCount = previous &&
      previous.role === 'active' &&
      previous.socket.readyState === WebSocket.OPEN
      ? inFlightRequestsByGenerationRef.current.get(previous.generation) ?? 0
      : 0;
    const desiredLobbyCode = (
      currentLobbyRef.current?.code ?? storedLobbyRef.current?.code ?? null
    )?.toUpperCase() ?? null;
    const desiredGameId = typeof window === 'undefined'
      ? null
      : activeGameIdFromPath(window.location.pathname);
    if (
      candidate.expectedLobbyCode !== desiredLobbyCode ||
      candidate.expectedGameId !== desiredGameId
    ) {
      candidate.socket.close(1000, 'client context changed during handoff');
      return;
    }
    const replacementIsReady = replacementReadyForPromotion({
      socketOpen: candidate.socket.readyState === WebSocket.OPEN,
      authenticated: candidate.authenticated,
      inFlightRequestCount,
      lobbyReady: candidate.lobbyReady,
      gameReady: candidate.gameReady,
      gameComplete: candidate.gameComplete,
      commandOutcomesReady: candidate.commandOutcomesReady,
      expectsGame: candidate.expectedGameId !== null,
      gameStreamWatermark: candidate.gameStreamWatermark,
    }, candidate.expectedGameId === null
      ? null
      : latestGameStreamSeqRef.current.get(candidate.expectedGameId) ?? null);
    if (!replacementIsReady) {
      clearContinuityProof(candidate);
      return;
    }

    const previousWasUsable = Boolean(
      previous &&
      previous.role === 'active' &&
      previous.authenticated &&
      previous.socket.readyState === WebSocket.OPEN,
    );
    if (previousWasUsable && previous) {
      if (
        !allowUnprovenTransportOverlap && (
          candidate.continuityProbeClientTime === null ||
          candidate.continuityProbeActiveGeneration !== previous.generation
        )
      ) {
        clearContinuityProof(candidate);
        const clientTime = --nextContinuityProbeClientTimeRef.current;
        candidate.continuityProbeClientTime = clientTime;
        candidate.continuityProbeActiveGeneration = previous.generation;
        try {
          previous.socket.send(JSON.stringify({ Ping: { client_time: clientTime } }));
        } catch (error) {
          clearContinuityProof(candidate);
          console.warn('Old WebSocket failed the planned-handoff continuity probe', error);
          try {
            previous.socket.close(1011, 'planned handoff continuity probe failed');
          } catch {
            // The normal close/recovery path will run if the transport is gone.
          }
        }
        return;
      }
      if (!candidate.continuityProbeConfirmed && !allowUnprovenTransportOverlap) {
        return;
      }
    }
    // This probe proves only that the old transport was still usable after the
    // replacement became fully ready. It is not a game-stream ordering fence:
    // the candidate has its own snapshot, outcome barrier, and replica stream.
    const transportOverlapProved = Boolean(
      previousWasUsable &&
      previous &&
      candidate.continuityProbeConfirmed &&
      candidate.continuityProbeActiveGeneration === previous.generation,
    );
    const nowMs = Date.now();
    clearGameWarmRetryTimeout(candidate);
    candidate.role = 'active';
    candidateSlotRef.current = null;
    activeSlotRef.current = candidate;
    ws.current = candidate.socket;
    setServerCapabilities(new Set(candidate.capabilities));
    reconnectAttemptRef.current = 0;
    candidateAttemptRef.current = 0;
    if (candidateDeadlineTimeoutRef.current) {
      clearTimeout(candidateDeadlineTimeoutRef.current);
      candidateDeadlineTimeoutRef.current = null;
    }
    if (candidateRetryTimeoutRef.current) {
      clearTimeout(candidateRetryTimeoutRef.current);
      candidateRetryTimeoutRef.current = null;
    }
    setCurrentRegionUrl(candidate.url);
    setIsConnected(true);
    setAuthHandshakeState(true);
    configureClockSync(candidate);
    if (typeof window !== 'undefined') {
      window.__wsInstance = candidate.socket;
    }

    const buffered = candidate.bufferedMessages.splice(0);
    buffered.forEach((message) => dispatchRawMessage(candidate, message));

    if (previous && previous !== candidate) {
      previous.role = 'retired';
      inFlightRequestsByGenerationRef.current.delete(previous.generation);
      try {
        previous.socket.close(1000, 'planned gateway handoff complete');
      } catch {
        // The server may have reached its drain deadline concurrently.
      }
    }
    const plannedStartedAtMs = plannedHandoffStartedAtRef.current;
    if (plannedStartedAtMs !== null) {
      if (transportOverlapProved) {
        recordWsMetric('planned_handoff_success', {
          count: 1,
          duration_ms: boundedMetricDuration(plannedStartedAtMs, nowMs),
          usable_session_gap_ms: 0,
        }, nowMs);
        plannedHandoffStartedAtRef.current = null;
      } else {
        recordPlannedHandoffFailure('old_socket_not_usable_at_promotion', nowMs);
        recoveryStartedAtRef.current ??= nowMs;
        usableGapStartedAtRef.current ??= nowMs;
      }
    }
    completeActiveRecovery(candidate, nowMs);
    recordTrace({ Note: { ts_ms: nowMs, note: `ws drain promoted generation ${candidate.generation}` } });
  }, [completeActiveRecovery, configureClockSync, dispatchRawMessage, recordPlannedHandoffFailure, setAuthHandshakeState]);

  const restoreCandidateContext = useCallback((candidate: SocketSlot) => {
    clearGameWarmRetryTimeout(candidate);
    candidate.contextRestoreStartedAtMs = Date.now();
    const lobbyCode = currentLobbyRef.current?.code ?? storedLobbyRef.current?.code ?? null;
    const gameId = typeof window === 'undefined' ? null : activeGameIdFromPath(window.location.pathname);
    candidate.expectedLobbyCode = lobbyCode?.toUpperCase() ?? null;
    candidate.expectedGameId = gameId;
    candidate.lobbyReady = candidate.expectedLobbyCode === null;
    candidate.gameReady = candidate.expectedGameId === null;
    candidate.gameComplete = false;
    candidate.gameStreamWatermark = null;
    candidate.commandOutcomesReady = candidate.expectedGameId === null;
    clearContinuityProof(candidate);

    if (candidate.expectedLobbyCode) {
      const preferences = buildInitialLobbyPreferences();
      candidate.socket.send(JSON.stringify({
        JoinLobby: {
          lobby_code: candidate.expectedLobbyCode,
          preferences: {
            selected_modes: preferences.selectedModes,
            competitive: preferences.competitive,
          },
        },
      }));
    }
    if (candidate.expectedGameId !== null) {
      candidate.socket.send(JSON.stringify({ JoinGame: candidate.expectedGameId }));
    }
    promoteCandidate(candidate);
  }, [buildInitialLobbyPreferences, promoteCandidate]);

  const recoverAfterCandidateFailure = useCallback((candidate: SocketSlot) => {
    clearGameWarmRetryTimeout(candidate);
    if (candidateDeadlineTimeoutRef.current) {
      clearTimeout(candidateDeadlineTimeoutRef.current);
      candidateDeadlineTimeoutRef.current = null;
    }
    const active = activeSlotRef.current;
    const hasUsableActive = Boolean(
      active &&
      active.role === 'active' &&
      (active.socket.readyState === WebSocket.OPEN ||
        active.socket.readyState === WebSocket.CONNECTING),
    );
    const action = replacementFailureAction(
      hasUsableActive,
      reconnectEnabledRef.current,
      candidate.drainDeadlineMs,
      Date.now(),
    );
    if (action === 'retry-candidate' && candidate.drainDeadlineMs !== null) {
      const attempt = candidateAttemptRef.current++;
      candidateRetryTimeoutRef.current = setTimeout(
        () => startCandidateRef.current(candidate.drainDeadlineMs!),
        reconnectDelayMs(attempt),
      );
    } else if (action === 'reconnect-active') {
      const attempt = reconnectAttemptRef.current++;
      reconnectTimeout.current = setTimeout(
        () => openActiveRef.current(candidate.url, onConnectCallback.current || undefined),
        reconnectDelayMs(attempt),
      );
    }
  }, []);

  const handleParsedMessage = useCallback((slot: SocketSlot, rawMessage: any) => {
    if (slot.role === 'retired') {
      return;
    }

    if (
      slot.role === 'active' &&
      activeSlotRef.current === slot &&
      rawMessage?.Pong
    ) {
      const candidate = candidateSlotRef.current;
      if (
        candidate &&
        candidate.continuityProbeActiveGeneration === slot.generation &&
        Number(rawMessage.Pong.client_time) === candidate.continuityProbeClientTime
      ) {
        dispatchRawMessage(slot, rawMessage);
        candidate.continuityProbeConfirmed = true;
        promoteCandidate(candidate);
        return;
      }
    }

    if (rawMessage?.Authenticated) {
      const nowMs = Date.now();
      const payload = rawMessage.Authenticated;
      slot.capabilities = Array.isArray(payload?.capabilities)
        ? payload.capabilities.filter((value: unknown): value is string => typeof value === 'string')
        : [];
      const missingCapabilities = missingRequiredServerCapabilities(slot.capabilities);
      if (missingCapabilities.length > 0) {
        console.error('Server is missing required WebSocket capabilities:', missingCapabilities);
        clearAuthenticationTimeout(slot);
        slot.authenticated = false;
        slot.socket.close(1002, 'unsupported server protocol');
        return;
      }
      clearAuthenticationTimeout(slot);
      slot.authenticated = true;
      if (slot.authStartedAtMs !== null) {
        recordWsMetric('auth_latency_ms', {
          value: boundedMetricDuration(slot.authStartedAtMs, nowMs),
          role: slot.role,
        }, nowMs);
        slot.authStartedAtMs = null;
      }
      if (slot.role === 'active' && activeSlotRef.current === slot) {
        reconnectAttemptRef.current = 0;
        setServerCapabilities(new Set(slot.capabilities));
        setAuthHandshakeState(true);
        const lobbyCode = currentLobbyRef.current?.code ?? storedLobbyRef.current?.code ?? null;
        const gameId = typeof window === 'undefined'
          ? null
          : activeGameIdFromPath(window.location.pathname);
        slot.contextRestoreStartedAtMs = nowMs;
        slot.expectedLobbyCode = lobbyCode?.toUpperCase() ?? null;
        slot.expectedGameId = gameId;
        slot.lobbyReady = slot.expectedLobbyCode === null;
        slot.gameReady = slot.expectedGameId === null;
        slot.commandOutcomesReady = slot.expectedGameId === null;
        if (lobbyCode) {
          const preferences = buildInitialLobbyPreferences();
          slot.socket.send(JSON.stringify({
            JoinLobby: {
              lobby_code: lobbyCode.toUpperCase(),
              preferences: {
                selected_modes: preferences.selectedModes,
                competitive: preferences.competitive,
              },
            },
          }));
        }
        dispatchRawMessage(slot, rawMessage);
        completeActiveRecovery(slot, nowMs);
      } else if (slot.role === 'candidate' && candidateSlotRef.current === slot) {
        restoreCandidateContext(slot);
      }
      return;
    }

    if (rawMessage?.Drain) {
      if (slot.role === 'active' && activeSlotRef.current === slot) {
        const serverDeadlineMs = Number(rawMessage.Drain.deadline_unix_ms);
        const nowMs = Date.now();
        const remainingMs = plannedDrainRemainingMs(
          serverDeadlineMs,
          nowMs,
          clockSync.getServerClockOffsetMs(),
        );
        if (remainingMs !== null) {
          if (plannedHandoffStartedAtRef.current === null) {
            plannedHandoffStartedAtRef.current = nowMs;
            recordWsMetric('planned_handoff_attempt', { count: 1 }, nowMs);
          }
          candidateAttemptRef.current = 0;
          if (candidateRetryTimeoutRef.current) {
            clearTimeout(candidateRetryTimeoutRef.current);
            candidateRetryTimeoutRef.current = null;
          }
          // Candidate retry/timer code uses the browser clock, so translate
          // the server deadline once instead of mixing the two clock domains.
          startCandidateRef.current(nowMs + remainingMs);
        }
      } else if (slot.role === 'candidate' && candidateSlotRef.current === slot) {
        slot.role = 'retired';
        candidateSlotRef.current = null;
        slot.socket.close(1012, 'candidate backend is draining');
        recoverAfterCandidateFailure(slot);
      }
      return;
    }

    if (slot.role === 'candidate' && candidateSlotRef.current === slot) {
      if (
        rawMessage?.GameWarming &&
        Number(rawMessage.GameWarming.game_id) === slot.expectedGameId
      ) {
        // The gateway is healthy and authenticated; only its local replica is
        // still catching up. Reuse this socket instead of repeating TCP, WS,
        // authentication, and lobby restoration on every warming response.
        clearGameWarmRetryTimeout(slot);
        const retryAfterMs = Math.max(
          100,
          Math.min(2000, Number(rawMessage.GameWarming.retry_after_ms) || 500),
        );
        slot.gameWarmRetryTimeoutId = setTimeout(() => {
          slot.gameWarmRetryTimeoutId = null;
          if (
            candidateSlotRef.current === slot &&
            slot.role === 'candidate' &&
            slot.authenticated &&
            slot.expectedGameId !== null &&
            slot.socket.readyState === WebSocket.OPEN &&
            (slot.drainDeadlineMs === null || Date.now() < slot.drainDeadlineMs)
          ) {
            slot.socket.send(JSON.stringify({ JoinGame: slot.expectedGameId }));
          }
        }, retryAfterMs);
        return;
      }
      if (rawMessage?.AccessDenied || rawMessage?.GameLoadFailed) {
        slot.role = 'retired';
        candidateSlotRef.current = null;
        slot.socket.close(1008, 'replacement context restore rejected');
        recoverAfterCandidateFailure(slot);
        return;
      }
      slot.bufferedMessages.push(rawMessage);
      if (slot.expectedGameId !== null) {
        slot.gameStreamWatermark = advanceCandidateGameWatermark(
          slot.gameStreamWatermark,
          rawMessage,
          slot.expectedGameId,
        );
      }
      if (
        slot.expectedLobbyCode &&
        (rawMessage?.JoinedLobby?.lobby_code?.toUpperCase() === slot.expectedLobbyCode ||
          rawMessage?.LobbyUpdate?.lobby_code?.toUpperCase() === slot.expectedLobbyCode)
      ) {
        if (!slot.lobbyReady && slot.contextRestoreStartedAtMs !== null) {
          const nowMs = Date.now();
          recordWsMetric('lobby_rejoin_latency_ms', {
            value: boundedMetricDuration(slot.contextRestoreStartedAtMs, nowMs),
            role: slot.role,
          }, nowMs);
        }
        slot.lobbyReady = true;
      }
      if (
        slot.expectedGameId !== null &&
        isSnapshotForGame(rawMessage, slot.expectedGameId)
      ) {
        clearGameWarmRetryTimeout(slot);
        if (!slot.gameReady && slot.contextRestoreStartedAtMs !== null) {
          const nowMs = Date.now();
          recordWsMetric('snapshot_rejoin_latency_ms', {
            value: boundedMetricDuration(slot.contextRestoreStartedAtMs, nowMs),
            role: slot.role,
          }, nowMs);
        }
        slot.gameReady = true;
        slot.gameComplete = isTerminalSnapshotForGame(rawMessage, slot.expectedGameId);
      }
      if (
        slot.expectedGameId !== null &&
        Number(rawMessage?.CommandOutcomesComplete?.game_id) === slot.expectedGameId
      ) {
        slot.commandOutcomesReady = true;
      }
      promoteCandidate(slot);
      return;
    }

    dispatchRawMessage(slot, rawMessage);
    const candidate = candidateSlotRef.current;
    if (candidate) {
      // An old-socket event can move the authoritative watermark ahead of a
      // candidate after its probe was sent. Revalidate on every active frame
      // so only a proof from the final overlapping readiness window counts.
      promoteCandidate(candidate);
    }
  }, [buildInitialLobbyPreferences, completeActiveRecovery, dispatchRawMessage, promoteCandidate, recoverAfterCandidateFailure, restoreCandidateContext, setAuthHandshakeState]);

  const attachSocketHandlers = useCallback((slot: SocketSlot, onConnect?: () => void) => {
    slot.socket.onopen = () => {
      if (
        (slot.role === 'active' && activeSlotRef.current !== slot) ||
        (slot.role === 'candidate' && candidateSlotRef.current !== slot) ||
        slot.role === 'retired'
      ) {
        slot.socket.close();
        return;
      }
      const token = getToken();
      clearAuthenticationTimeout(slot);
      slot.authTimeoutId = setTimeout(() => {
        if (
          slot.authenticated ||
          slot.role === 'retired' ||
          (slot.role === 'active' && activeSlotRef.current !== slot) ||
          (slot.role === 'candidate' && candidateSlotRef.current !== slot)
        ) {
          return;
        }
        slot.authTimeoutId = null;
        slot.socket.close(1013, 'authentication timed out');
      }, AUTHENTICATION_TIMEOUT_MS);
      if (token) {
        slot.authStartedAtMs = Date.now();
        slot.authTokenSent = token;
        slot.socket.send(JSON.stringify({ Token: token }));
        lastAuthTokenRef.current = token;
      }

      if (slot.role === 'active') {
        console.log('WebSocket connected to:', slot.url, 'generation:', slot.generation);
        if (hasEverConnectedRef.current) {
          recordTrace({ Note: { ts_ms: Date.now(), note: 'ws reconnected' } });
        }
        hasEverConnectedRef.current = true;
        setIsConnected(true);
        setAuthHandshakeState(false);
        configureClockSync(slot);
        if (reconnectTimeout.current) {
          clearTimeout(reconnectTimeout.current);
          reconnectTimeout.current = null;
        }
        if (typeof window !== 'undefined') {
          window.__wsInstance = slot.socket;
        }
        onConnect?.();
      }
    };

    slot.socket.onmessage = (event: MessageEvent) => {
      const processMessage = () => {
        if (slot.role === 'retired') {
          return;
        }
        try {
          handleParsedMessage(slot, JSON.parse(event.data));
        } catch (error) {
          console.error('Failed to parse WebSocket message:', error);
        }
      };
      const settings = latencySettingsRef.current;
      if (settings.enabled && settings.receiveDelayMs > 0) {
        setTimeout(processMessage, settings.receiveDelayMs);
      } else {
        processMessage();
      }
    };

    slot.socket.onerror = (error: Event) => {
      if (slot.role !== 'retired') {
        console.error('WebSocket error:', error);
      }
    };

    slot.socket.onclose = () => {
      clearAuthenticationTimeout(slot);
      clearGameWarmRetryTimeout(slot);
      if (slot.role === 'retired') {
        return;
      }
      if (slot.role === 'candidate') {
        if (candidateSlotRef.current !== slot) {
          return;
        }
        candidateSlotRef.current = null;
        recoverAfterCandidateFailure(slot);
        return;
      }
      if (activeSlotRef.current !== slot) {
        return;
      }

      const nowMs = Date.now();
      recordPlannedHandoffFailure('active_closed_before_promotion', nowMs);
      recoveryStartedAtRef.current = nowMs;
      usableGapStartedAtRef.current = nowMs;
      inFlightRequestsByGenerationRef.current.delete(slot.generation);
      activeSlotRef.current = null;
      ws.current = null;
      setIsConnected(false);
      setAuthHandshakeState(false);
      setServerCapabilities(new Set());
      lastAuthTokenRef.current = null;
      clockSync.reset();
      syncRequestTimes.current.clear();
      // A transport loss is not a request to leave. Keep the in-memory and
      // persisted lobby identity so the next authenticated socket can rejoin.
      recordTrace({ Note: { ts_ms: nowMs, note: 'ws disconnected, reconnect scheduled' } });
      const candidate = candidateSlotRef.current;
      if (candidate) {
        // The make-before-break proof failed, but a fully restored candidate
        // can still become the crash-recovery socket immediately.
        promoteCandidate(candidate);
        if (activeSlotRef.current === candidate) {
          return;
        }
      }
      if (!reconnectEnabledRef.current || candidateSlotRef.current) {
        return;
      }
      const attempt = reconnectAttemptRef.current++;
      const delay = reconnectDelayMs(attempt);
      reconnectTimeout.current = setTimeout(() => {
        openActiveRef.current(slot.url, onConnect);
      }, delay);
    };
  }, [configureClockSync, getToken, handleParsedMessage, promoteCandidate, recordPlannedHandoffFailure, recoverAfterCandidateFailure, setAuthHandshakeState]);

  const createSlot = useCallback((url: string, role: SocketRole): SocketSlot => {
    const socket = new WebSocket(url);
    return {
      socket,
      generation: ++nextGenerationRef.current,
      role,
      url,
      authenticated: false,
      capabilities: [],
      authTokenSent: null,
      bufferedMessages: [],
      expectedLobbyCode: null,
      expectedGameId: null,
      lobbyReady: false,
      gameReady: false,
      gameComplete: false,
      gameStreamWatermark: null,
      commandOutcomesReady: false,
      drainDeadlineMs: null,
      candidateAttempt: 0,
      authStartedAtMs: null,
      authTimeoutId: null,
      gameWarmRetryTimeoutId: null,
      contextRestoreStartedAtMs: null,
      continuityProbeClientTime: null,
      continuityProbeActiveGeneration: null,
      continuityProbeConfirmed: false,
    };
  }, []);

  const openActive = useCallback((url: string, onConnect?: () => void) => {
    if (!reconnectEnabledRef.current) {
      return;
    }
    const existing = activeSlotRef.current;
    if (
      existing &&
      (existing.socket.readyState === WebSocket.OPEN ||
        existing.socket.readyState === WebSocket.CONNECTING)
    ) {
      return;
    }
    try {
      const slot = createSlot(url, 'active');
      activeSlotRef.current = slot;
      ws.current = slot.socket;
      setCurrentRegionUrl(url);
      attachSocketHandlers(slot, onConnect);
    } catch (error) {
      console.error('Failed to create WebSocket:', error);
      if (reconnectEnabledRef.current) {
        const attempt = reconnectAttemptRef.current++;
        reconnectTimeout.current = setTimeout(
          () => openActiveRef.current(url, onConnect),
          reconnectDelayMs(attempt),
        );
      }
    }
  }, [attachSocketHandlers, createSlot]);
  openActiveRef.current = openActive;

  const startCandidate = useCallback((deadlineMs: number) => {
    if (!reconnectEnabledRef.current || candidateSlotRef.current || Date.now() >= deadlineMs) {
      return;
    }
    const url = activeSlotRef.current?.url ?? currentRegionUrl;
    if (!url) {
      return;
    }
    try {
      const slot = createSlot(url, 'candidate');
      slot.drainDeadlineMs = deadlineMs;
      slot.candidateAttempt = candidateAttemptRef.current;
      candidateSlotRef.current = slot;
      attachSocketHandlers(slot);
      const remainingMs = candidateDeadlineDelayMs(deadlineMs, Date.now());
      candidateDeadlineTimeoutRef.current = setTimeout(() => {
        if (candidateSlotRef.current !== slot || slot.role !== 'candidate') {
          return;
        }
        // The old task and this timer share a deadline. If the replacement is
        // already ready, keep it when this timer wins the race with the old
        // socket's close event. This is crash-style promotion (and therefore a
        // planned-handoff failure), not proof of a zero-gap planned handoff.
        promoteCandidate(slot, true);
        if (activeSlotRef.current === slot) {
          return;
        }
        slot.role = 'retired';
        candidateSlotRef.current = null;
        clearGameWarmRetryTimeout(slot);
        recordPlannedHandoffFailure('candidate_deadline', Date.now());
        try {
          slot.socket.close(1013, 'planned handoff deadline reached');
        } finally {
          recoverAfterCandidateFailure(slot);
        }
      }, remainingMs);
    } catch (error) {
      console.error('Failed to create replacement WebSocket:', error);
      const attempt = candidateAttemptRef.current++;
      candidateRetryTimeoutRef.current = setTimeout(
        () => startCandidateRef.current(deadlineMs),
        reconnectDelayMs(attempt),
      );
    }
  }, [attachSocketHandlers, createSlot, currentRegionUrl, promoteCandidate, recordPlannedHandoffFailure, recoverAfterCandidateFailure]);
  startCandidateRef.current = startCandidate;

  const connect = useCallback((url: string, onConnect?: () => void) => {
    reconnectEnabledRef.current = true;
    if (onConnect) {
      onConnectCallback.current = onConnect;
    }
    openActiveRef.current(url, onConnect);
  }, []);

  const disconnect = useCallback(() => {
    reconnectEnabledRef.current = false;
    clockSync.stop();
    if (reconnectTimeout.current) {
      clearTimeout(reconnectTimeout.current);
      reconnectTimeout.current = null;
    }
    if (candidateRetryTimeoutRef.current) {
      clearTimeout(candidateRetryTimeoutRef.current);
      candidateRetryTimeoutRef.current = null;
    }
    if (candidateDeadlineTimeoutRef.current) {
      clearTimeout(candidateDeadlineTimeoutRef.current);
      candidateDeadlineTimeoutRef.current = null;
    }
    syncRequestTimes.current.clear();
    const active = activeSlotRef.current;
    const candidate = candidateSlotRef.current;
    activeSlotRef.current = null;
    candidateSlotRef.current = null;
    ws.current = null;
    if (active) {
      active.role = 'retired';
      inFlightRequestsByGenerationRef.current.delete(active.generation);
      clearAuthenticationTimeout(active);
      clearGameWarmRetryTimeout(active);
      active.socket.close();
    }
    if (candidate) {
      candidate.role = 'retired';
      inFlightRequestsByGenerationRef.current.delete(candidate.generation);
      clearAuthenticationTimeout(candidate);
      clearGameWarmRetryTimeout(candidate);
      candidate.socket.close();
    }
    setIsConnected(false);
    setAuthHandshakeState(false);
    setServerCapabilities(new Set());
    plannedHandoffStartedAtRef.current = null;
    recoveryStartedAtRef.current = null;
    usableGapStartedAtRef.current = null;
  }, [setAuthHandshakeState]);

  const connectToRegion = useCallback((wsUrl: string, options?: { regionId?: string; origin?: string }) => {
    console.log('Switching to region:', wsUrl);
    disconnect();
    reconnectEnabledRef.current = true;
    reconnectAttemptRef.current = 0;
    if (options?.regionId) {
      saveRegionPreference({
        regionId: options.regionId,
        wsUrl,
        origin: options.origin,
        timestamp: Date.now(),
      });
    }
    connect(wsUrl, onConnectCallback.current || undefined);
  }, [connect, disconnect]);

  const sendMessage = useCallback((message: any) => {
    const target = activeSlotRef.current;
    const doSend = () => {
      if (
        target &&
        isCommandOwner(
          activeSlotRef.current?.generation ?? null,
          target.generation,
          target.role,
          target.socket.readyState,
        )
      ) {
        target.socket.send(JSON.stringify(message));
        console.log('WebSocket message sent:', message, 'generation:', target.generation);
      } else {
        console.error('WebSocket is not connected');
      }
    };
    if (latencySettings.enabled && latencySettings.sendDelayMs > 0) {
      setTimeout(doSend, latencySettings.sendDelayMs);
    } else {
      doSend();
    }
  }, [latencySettings]);

  const authenticateConnection = useCallback(() => {
    const slot = activeSlotRef.current;
    if (!slot || slot.socket.readyState !== WebSocket.OPEN) {
      return false;
    }
    const token = getToken();
    if (!token) {
      return false;
    }
    if (slot.authenticated && slot.authTokenSent === token) {
      return true;
    }
    if (slot.authTokenSent !== token) {
      slot.authStartedAtMs = Date.now();
      slot.authTokenSent = token;
      slot.socket.send(JSON.stringify({ Token: token }));
      lastAuthTokenRef.current = token;
    }
    return slot.authenticated;
  }, [getToken]);

  const sendChatMessage = useCallback((scope: ChatScope, message: string) => {
    const trimmed = message.trim();
    if (!trimmed) {
      return;
    }

    console.log(`Sending ${scope} chat message`, trimmed);
    sendMessage({ Chat: trimmed });
  }, [sendMessage]);

  const beginResponseTrackedRequest = useCallback(() => {
    const generation = activeSlotRef.current?.generation;
    if (generation === undefined) {
      return () => {};
    }
    const requests = inFlightRequestsByGenerationRef.current;
    requests.set(generation, (requests.get(generation) ?? 0) + 1);
    let finished = false;
    return () => {
      if (finished) {
        return;
      }
      finished = true;
      const remaining = Math.max(0, (requests.get(generation) ?? 0) - 1);
      if (remaining === 0) {
        requests.delete(generation);
      } else {
        requests.set(generation, remaining);
      }
      const candidate = candidateSlotRef.current;
      if (candidate) {
        promoteCandidate(candidate);
      }
    };
  }, [promoteCandidate]);

  const onMessage = useCallback((messageType: string, handler: MessageHandler) => {
    if (!messageHandlers.current.has(messageType)) {
      messageHandlers.current.set(messageType, []);
    }
    messageHandlers.current.get(messageType)!.push(handler);

    // Return cleanup function
    return () => {
      const handlers = messageHandlers.current.get(messageType) || [];
      const index = handlers.indexOf(handler);
      if (index > -1) {
        handlers.splice(index, 1);
      }
    };
  }, []);

  useEffect(() => {
    if (!isConnected) {
      setAuthHandshakeState(false);
      lastAuthTokenRef.current = null;
      return;
    }

    authenticateConnection();
  }, [isConnected, authenticateConnection, setAuthHandshakeState]);

  useEffect(() => {
    const previous = previousUserRef.current;
    const token = getToken();
    if (!token) {
      setAuthHandshakeState(false);
      lastAuthTokenRef.current = null;
      return;
    }

    const tokenChanged = lastAuthTokenRef.current && token !== lastAuthTokenRef.current;

    // If the auth token changes (guest -> real account, logout, etc.), force a reconnect
    // because the server only accepts authentication during the initial handshake.
    if (isConnected && tokenChanged) {
      console.log('Auth token changed, reconnecting WebSocket');
      previousUserRef.current = user;
      setAuthHandshakeState(false);
      const url = activeSlotRef.current?.url ?? currentRegionUrl;
      disconnect();
      if (url) {
        reconnectEnabledRef.current = true;
        connect(url, onConnectCallback.current || undefined);
      }
      return;
    }

    if (previous?.isGuest && user && !user.isGuest) {
      console.log('Guest transitioned to full user, reconnecting WebSocket');
      previousUserRef.current = user;
      const url = activeSlotRef.current?.url ?? currentRegionUrl;
      disconnect();
      if (url) {
        reconnectEnabledRef.current = true;
        connect(url, onConnectCallback.current || undefined);
      }
      return;
    }

    previousUserRef.current = user;

    if (!isConnected) {
      return;
    }

    // If token changed or session not yet authenticated, perform handshake
    if (!isSessionAuthenticated || token !== lastAuthTokenRef.current) {
      setAuthHandshakeState(false);
      authenticateConnection();
    }
  }, [
    user,
    isConnected,
    isSessionAuthenticated,
    currentRegionUrl,
    connect,
    disconnect,
    getToken,
    authenticateConnection,
    setAuthHandshakeState,
  ]);

  // Auto-connect to the preferred or closest region on mount
  useEffect(() => {
    let cancelled = false;
    let detectionAttempt = 0;
    let retryTimeout: ReturnType<typeof setTimeout> | null = null;

    const scheduleRetry = () => {
      if (cancelled || retryTimeout !== null) {
        return;
      }
      const delay = reconnectDelayMs(detectionAttempt++);
      retryTimeout = setTimeout(() => {
        retryTimeout = null;
        void ensureConnected();
      }, delay);
    };

    const ensureConnected = async () => {
      if (typeof window === 'undefined') {
        return;
      }

      if (ws.current && (ws.current.readyState === WebSocket.OPEN || ws.current.readyState === WebSocket.CONNECTING)) {
        return;
      }

      if (isInitializingRef.current) {
        return;
      }

      isInitializingRef.current = true;

      try {
        const storedPreference = loadRegionPreference();
        if (storedPreference?.regionId) {
          if (storedPreference.wsUrl) {
            if (!cancelled) {
              connectToRegion(storedPreference.wsUrl, {
                regionId: storedPreference.regionId,
                origin: storedPreference.origin,
              });
            }
            return;
          }

          try {
            const metadata = await fetchRegionMetadata();
            const matched = metadata.find(region => region.id === storedPreference.regionId);
            if (matched && !cancelled) {
              const repairedPreference = {
                regionId: matched.id,
                wsUrl: matched.ws_url,
                origin: matched.origin,
                timestamp: Date.now(),
              };
              saveRegionPreference(repairedPreference);
              connectToRegion(repairedPreference.wsUrl, {
                regionId: repairedPreference.regionId,
                origin: repairedPreference.origin,
              });
              return;
            }
          } catch (error) {
            console.error('Failed to repair legacy region preference:', error);
          }
        }

        const detected = await detectBestRegion();
        if (detected && !cancelled) {
          saveRegionPreference(detected.preference);
          connectToRegion(detected.preference.wsUrl!, {
            regionId: detected.preference.regionId,
            origin: detected.preference.origin,
          });
          return;
        }
        scheduleRetry();
      } finally {
        isInitializingRef.current = false;
      }
    };

    ensureConnected();

    return () => {
      cancelled = true;
      if (retryTimeout !== null) {
        clearTimeout(retryTimeout);
      }
    };
  }, [connectToRegion]);

  // Lobby methods
  const createLobby = useCallback(async () => {
    const requestedInitialPreferences = buildInitialLobbyPreferences();
    const initialPreferencesClone: LobbyPreferences = {
      selectedModes: [...requestedInitialPreferences.selectedModes],
      competitive: requestedInitialPreferences.competitive,
    };

    return new Promise<void>((resolve, reject) => {
      if (!ws.current || ws.current.readyState !== WebSocket.OPEN) {
        reject(new Error('WebSocket not connected'));
        return;
      }

      const finishRequest = beginResponseTrackedRequest();
      let settled = false;
      let timeoutId: ReturnType<typeof setTimeout> | null = null;

      // Set up one-time handler for LobbyCreated message
      const cleanup = onMessage('LobbyCreated', (message: any) => {
        if (settled) {
          return;
        }

        const { lobby_id, lobby_code } = message.data;
        const normalizedCode = lobby_code.toUpperCase();
        const newLobby: Lobby = {
          id: lobby_id,
          code: normalizedCode,
          hostUserId: user?.id ?? 0, // Optimistically assume creator is host
          region: '', // Will be set by LobbyUpdate
          state: 'waiting',
        };
        currentLobbyRef.current = newLobby;
        setCurrentLobby(newLobby);
        // console.log('setLobbyPreferences', DEFAULT_LOBBY_PREFERENCES);
        // setLobbyPreferences(DEFAULT_LOBBY_PREFERENCES);
        persistLobby({ id: lobby_id, code: normalizedCode });

        if (initialPreferencesClone.selectedModes.length > 0) {
          desiredLobbyPreferencesRef.current = initialPreferencesClone;
          setLobbyPreferences(initialPreferencesClone);
          sendMessage({
            UpdateLobbyPreferences: {
              selected_modes: initialPreferencesClone.selectedModes,
              competitive: initialPreferencesClone.competitive,
            },
          });
        }

        settled = true;
        cleanup();
        if (timeoutId) {
          clearTimeout(timeoutId);
        }
        finishRequest();
        resolve();
      });

      // Send CreateLobby message
      try {
        sendMessage('CreateLobby');
      } catch (error) {
        settled = true;
        cleanup();
        finishRequest();
        reject(error);
        return;
      }

      // Timeout after 5 seconds
      timeoutId = setTimeout(() => {
        if (settled) {
          return;
        }
        settled = true;
        cleanup();
        finishRequest();
        reject(new Error('Timeout waiting for lobby creation'));
      }, 5000);
    });
  }, [
    beginResponseTrackedRequest,
    onMessage,
    sendMessage,
    persistLobby,
    user?.id,
    buildInitialLobbyPreferences,
  ]);

  const joinLobby = useCallback(async (lobbyCode: string) => {
    const normalizedCode = lobbyCode.trim().toUpperCase();
    const joinPreferences = buildInitialLobbyPreferences();
    return new Promise<void>((resolve, reject) => {
      if (!ws.current || ws.current.readyState !== WebSocket.OPEN) {
        reject(new Error('WebSocket not connected'));
        return;
      }

      const finishRequest = beginResponseTrackedRequest();
      let settled = false;
      let timeoutId: ReturnType<typeof setTimeout> | null = null;

      const cleanupHandlers = () => {
        cleanupJoined();
        cleanupDenied();
        cleanupMismatch();
        cleanupUpdate();
        if (timeoutId) {
          clearTimeout(timeoutId);
          timeoutId = null;
        }
      };

      const handleSuccess = (lobbyId: number, hostUserId?: number) => {
        if (settled) {
          return;
        }
        settled = true;
        const joinedLobby: Lobby = {
          id: lobbyId,
          code: normalizedCode,
          hostUserId: hostUserId ?? 0, // Will be refined by future LobbyUpdate messages
          region: '', // Will be set by LobbyUpdate
          state: 'waiting',
        };
        currentLobbyRef.current = joinedLobby;
        setCurrentLobby(joinedLobby);
        // console.log('setLobbyPreferences', DEFAULT_LOBBY_PREFERENCES);
        // setLobbyPreferences(DEFAULT_LOBBY_PREFERENCES);
        persistLobby({ id: lobbyId, code: normalizedCode });
        cleanupHandlers();
        finishRequest();
        resolve();
      };

      // Set up handlers for possible responses
      let cleanupJoined = () => {};
      let cleanupDenied = () => {};
      let cleanupMismatch = () => {};
      let cleanupUpdate = () => {};

      cleanupJoined = onMessage('JoinedLobby', (message: any) => {
        if (settled) {
          return;
        }
        const { lobby_id } = message.data;
        handleSuccess(lobby_id);
      });

      cleanupDenied = onMessage('AccessDenied', (message: any) => {
        if (settled) {
          return;
        }
        const reason =
          typeof message?.data?.reason === 'string' ? message.data.reason : '';
        if (isLobbyMissingReason(reason)) {
          resetLobbyState();
        }
        settled = true;
        cleanupHandlers();
        finishRequest();
        reject(new Error(reason || 'Access denied'));
      });

      cleanupMismatch = onMessage('LobbyRegionMismatch', (message: any) => {
        const { target_region, ws_url, lobby_code: code } = message.data;
        console.log(`Lobby is in region ${target_region}, reconnecting to ${ws_url}`);

        if (settled) {
          return;
        }

        // Clean up handlers before reconnecting
        settled = true;
        cleanupHandlers();
        finishRequest();

        // Reconnect to the correct region
        connectToRegion(ws_url, { regionId: target_region });

        // After reconnecting, retry joining
        // This will be handled by the onConnect callback
        setTimeout(() => {
          joinLobby(code).then(resolve).catch(reject);
        }, 1000);
      });

      cleanupUpdate = onMessage('LobbyUpdate', (message: any) => {
        if (settled) {
          return;
        }
        const { lobby_id, host_user_id } = message.data;
        handleSuccess(lobby_id, host_user_id);
      });

      // Timeout after 5 seconds
      timeoutId = setTimeout(() => {
        if (settled) {
          return;
        }
        settled = true;
        cleanupHandlers();
        finishRequest();
        reject(new Error('Timeout waiting to join lobby'));
      }, 5000);

      try {
        sendMessage({
          JoinLobby: {
            lobby_code: normalizedCode,
            preferences: {
              selected_modes: joinPreferences.selectedModes,
              competitive: joinPreferences.competitive,
            },
          },
        });
      } catch (error) {
        settled = true;
        cleanupHandlers();
        finishRequest();
        reject(error);
      }
    });
  }, [
    beginResponseTrackedRequest,
    onMessage,
    sendMessage,
    connectToRegion,
    persistLobby,
    resetLobbyState,
    isLobbyMissingReason,
    buildInitialLobbyPreferences,
  ]);

  const leaveLobby = useCallback(async () => {
    return new Promise<void>((resolve, reject) => {
      if (!ws.current || ws.current.readyState !== WebSocket.OPEN) {
        reject(new Error('WebSocket not connected'));
        return;
      }

      const finishRequest = beginResponseTrackedRequest();
      let settled = false;
      let timeoutId: ReturnType<typeof setTimeout> | null = null;

      // Set up one-time handler for LeftLobby message
      const cleanup = onMessage('LeftLobby', () => {
        if (settled) {
          return;
        }
        resetLobbyState();
        cleanup();
        if (timeoutId) {
          clearTimeout(timeoutId);
        }
        settled = true;
        finishRequest();
        resolve();
      });

      // Send LeaveLobby message
      try {
        sendMessage('LeaveLobby');
      } catch (error) {
        settled = true;
        cleanup();
        finishRequest();
        reject(error);
        return;
      }

      // Timeout after 5 seconds
      timeoutId = setTimeout(() => {
        if (settled) {
          return;
        }
        settled = true;
        cleanup();
        finishRequest();
        reject(new Error('Timeout waiting to leave lobby'));
      }, 5000);
    });
  }, [beginResponseTrackedRequest, onMessage, sendMessage, resetLobbyState]);

  const updateLobbyPreferences = useCallback(
    (preferences: LobbyPreferences) => {
      // console.log('setLobbyPreferences', preferences);
      desiredLobbyPreferencesRef.current = preferences;
      setLobbyPreferences(preferences);

      if (!currentLobbyRef.current) {
        return;
      }

      sendMessage({
        UpdateLobbyPreferences: {
          selected_modes: preferences.selectedModes,
          competitive: preferences.competitive,
        },
      });
    },
    [sendMessage],
  );

  useEffect(() => {
    const cleanup = onMessage('LobbyChatMessage', (message: any) => {
      const payload = message?.data ?? message?.LobbyChatMessage ?? message;
      if (!payload || typeof payload.message !== 'string') {
        return;
      }

      const lobbyId =
        typeof payload.lobby_id === 'number'
          ? payload.lobby_id
          : typeof payload.lobby_id === 'string'
            ? parseInt(payload.lobby_id, 10)
            : undefined;
      const timestampMs =
        typeof payload.timestamp_ms === 'number'
          ? payload.timestamp_ms
          : Date.now();
      const rawUsername =
        typeof payload.username === 'string' && payload.username.trim()
          ? payload.username.trim()
          : null;
      const rawUserId =
        typeof payload.user_id === 'number'
          ? payload.user_id
          : typeof payload.user_id === 'string'
            ? parseInt(payload.user_id, 10)
            : null;
      const messageId =
        typeof payload.message_id === 'string'
          ? payload.message_id
          : `${lobbyId ?? 'lobby'}-${timestampMs}-${Math.random().toString(36).slice(2, 8)}`;

      const normalized: ChatMessage = {
        id: messageId,
        scope: 'lobby',
        lobbyId,
        gameId: undefined,
        userId: Number.isFinite(rawUserId as number) ? (rawUserId as number) : null,
        username: rawUsername,
        message: payload.message,
        type: rawUsername ? 'user' : 'system',
        timestamp: new Date(timestampMs),
      };

      setLobbyChatMessages((previous) => {
        const isNewLobby = typeof lobbyId === 'number' && lobbyChatLobbyIdRef.current !== lobbyId;
        if (isNewLobby) {
          lobbyChatLobbyIdRef.current = lobbyId;
        }
        const base = isNewLobby ? [] : previous;
        const next = [...base, normalized];
        if (next.length > MAX_CHAT_HISTORY) {
          return next.slice(next.length - MAX_CHAT_HISTORY);
        }
        return next;
      });
    });

    return cleanup;
  }, [onMessage]);

  useEffect(() => {
    return onMessage('AccessDenied', (message: any) => {
      const reason =
        typeof message?.data?.reason === 'string' ? message.data.reason : '';
      if (isLobbyMissingReason(reason)) {
        resetLobbyState();
      }
    });
  }, [onMessage, resetLobbyState, isLobbyMissingReason]);

  useEffect(() => {
    const cleanup = onMessage('LobbyChatHistory', (message: any) => {
      const payload = message?.data ?? message?.LobbyChatHistory ?? message;
      if (!payload) {
        return;
      }

      const lobbyIdRaw = payload.lobby_id;
      const lobbyId =
        typeof lobbyIdRaw === 'number'
          ? lobbyIdRaw
          : typeof lobbyIdRaw === 'string'
            ? parseInt(lobbyIdRaw, 10)
            : undefined;
      const messagesArray = Array.isArray(payload.messages) ? payload.messages : [];

      const normalized = messagesArray
        .map((entry: any) => {
          if (!entry || typeof entry.message !== 'string') {
            return null;
          }

          const timestampMs =
            typeof entry.timestamp_ms === 'number' ? entry.timestamp_ms : Date.now();
          const rawUsername =
            typeof entry.username === 'string' && entry.username.trim()
              ? entry.username.trim()
              : null;
          const rawUserId =
            typeof entry.user_id === 'number'
              ? entry.user_id
              : typeof entry.user_id === 'string'
                ? parseInt(entry.user_id, 10)
                : null;
          const messageId =
            typeof entry.message_id === 'string'
              ? entry.message_id
              : `${lobbyId ?? 'lobby'}-${timestampMs}-${Math.random().toString(36).slice(2, 8)}`;

          const chatMessage: ChatMessage = {
            id: messageId,
            scope: 'lobby' as const,
            lobbyId,
            gameId: undefined,
            userId: Number.isFinite(rawUserId as number) ? (rawUserId as number) : null,
            username: rawUsername,
            message: entry.message,
            type: rawUsername ? 'user' : 'system',
            timestamp: new Date(timestampMs),
          };
          return chatMessage;
        })
        .filter((entry: ChatMessage | null): entry is ChatMessage => entry !== null)
        .sort((a: ChatMessage, b: ChatMessage) => a.timestamp.getTime() - b.timestamp.getTime());

      if (typeof lobbyId === 'number' && Number.isFinite(lobbyId)) {
        lobbyChatLobbyIdRef.current = lobbyId;
      } else {
        lobbyChatLobbyIdRef.current = null;
      }

      setLobbyChatMessages(() => {
        if (normalized.length > MAX_CHAT_HISTORY) {
          return normalized.slice(normalized.length - MAX_CHAT_HISTORY);
        }
        return normalized;
      });
    });

    return cleanup;
  }, [onMessage]);

  // Handle lobby updates
  useEffect(() => {
    const cleanup = onMessage('LobbyUpdate', (message: any) => {
      const payload = message?.data ?? message?.LobbyUpdate ?? message;
      if (!payload || typeof payload !== 'object') {
        return;
      }

      const lobbyId =
        typeof payload.lobby_id === 'number'
          ? payload.lobby_id
          : typeof payload.lobby_id === 'string'
            ? parseInt(payload.lobby_id, 10)
            : NaN;
      const lobbyCode =
        typeof payload.lobby_code === 'string' && payload.lobby_code.trim()
          ? payload.lobby_code.trim().toUpperCase()
          : null;

      const currentLobbySnapshot = currentLobbyRef.current;
      if (!currentLobbySnapshot) {
        return;
      }

      const matchesById =
        Number.isFinite(lobbyId) && currentLobbySnapshot.id === lobbyId;
      const matchesByCode =
        lobbyCode !== null &&
        typeof currentLobbySnapshot.code === 'string' &&
        currentLobbySnapshot.code.toUpperCase() === lobbyCode;

      if (!matchesById && !matchesByCode) {
        return;
      }

      const members = Array.isArray(payload.members) ? payload.members : [];
      setLobbyMembers(members);

      const hostUserId =
        typeof payload.host_user_id === 'number' ? payload.host_user_id : 0;
      const rawState =
        typeof payload.state === 'string' ? payload.state.trim().toLowerCase() : '';
      const lobbyState: LobbyState = VALID_LOBBY_STATES.includes(rawState as LobbyState)
        ? (rawState as LobbyState)
        : 'waiting';

      const normalizedPreferences = normalizeLobbyPreferences(payload.preferences);
      // console.log('setLobbyPreferences', normalizedPreferences);
      setLobbyPreferences(normalizedPreferences);

      setCurrentLobby((previous) => {
        if (!previous) {
          return previous;
        }

        const previousMatchesById =
          Number.isFinite(lobbyId) && previous.id === lobbyId;
        const previousMatchesByCode =
          lobbyCode !== null &&
          typeof previous.code === 'string' &&
          previous.code.toUpperCase() === lobbyCode;

        if (!previousMatchesById && !previousMatchesByCode) {
          return previous;
        }

        const updatedLobby: Lobby = {
          ...previous,
          hostUserId,
          state: lobbyState,
        };
        currentLobbyRef.current = updatedLobby;
        return updatedLobby;
      });
    });

    return cleanup;
  }, [onMessage]);

  useEffect(() => {
    const lobbyId = currentLobby?.id ?? null;

    // if (lobbyId === null) {
    //   lobbyChatLobbyIdRef.current = null;
    //   setLobbyChatMessages([]);
    //   console.log('setLobbyPreferences', DEFAULT_LOBBY_PREFERENCES);
    //   setLobbyPreferences(DEFAULT_LOBBY_PREFERENCES);
    //   return;
    // }

    if (lobbyChatLobbyIdRef.current !== lobbyId) {
      lobbyChatLobbyIdRef.current = lobbyId;
      setLobbyChatMessages([]);
    }
  }, [currentLobby]);

  useEffect(() => {
    const cleanup = onMessage('GameChatHistory', (message: any) => {
      const payload = message?.data ?? message?.GameChatHistory ?? message;
      if (!payload) {
        return;
      }

      const gameIdRaw = payload.game_id;
      const gameId =
        typeof gameIdRaw === 'number'
          ? gameIdRaw
          : typeof gameIdRaw === 'string'
            ? parseInt(gameIdRaw, 10)
            : undefined;
      const messagesArray = Array.isArray(payload.messages) ? payload.messages : [];

      const normalized = messagesArray
        .map((entry: any) => {
          if (!entry || typeof entry.message !== 'string') {
            return null;
          }

          const timestampMs =
            typeof entry.timestamp_ms === 'number' ? entry.timestamp_ms : Date.now();
          const rawUsername =
            typeof entry.username === 'string' && entry.username.trim()
              ? entry.username.trim()
              : null;
          const rawUserId =
            typeof entry.user_id === 'number'
              ? entry.user_id
              : typeof entry.user_id === 'string'
                ? parseInt(entry.user_id, 10)
                : null;
          const messageId =
            typeof entry.message_id === 'string'
              ? entry.message_id
              : `${gameId ?? 'game'}-${timestampMs}-${Math.random().toString(36).slice(2, 8)}`;

          const chatMessage: ChatMessage = {
            id: messageId,
            scope: 'game' as const,
            lobbyId: undefined,
            gameId,
            userId: Number.isFinite(rawUserId as number) ? (rawUserId as number) : null,
            username: rawUsername,
            message: entry.message,
            type: rawUsername ? 'user' : 'system',
            timestamp: new Date(timestampMs),
          };
          return chatMessage;
        })
        .filter((entry: ChatMessage | null): entry is ChatMessage => entry !== null)
        .sort((a: ChatMessage, b: ChatMessage) => a.timestamp.getTime() - b.timestamp.getTime());

      if (typeof gameId === 'number' && Number.isFinite(gameId)) {
        gameChatIdRef.current = gameId;
      } else {
        gameChatIdRef.current = null;
      }

      setGameChatMessages(() => {
        if (normalized.length > MAX_CHAT_HISTORY) {
          return normalized.slice(normalized.length - MAX_CHAT_HISTORY);
        }
        return normalized;
      });
    });

    return cleanup;
  }, [onMessage]);

  useEffect(() => {
    const cleanup = onMessage('GameChatMessage', (message: any) => {
      const payload = message?.data ?? message?.GameChatMessage ?? message;
      if (!payload || typeof payload.message !== 'string') {
        return;
      }

      const gameId =
        typeof payload.game_id === 'number'
          ? payload.game_id
          : typeof payload.game_id === 'string'
            ? parseInt(payload.game_id, 10)
            : undefined;
      const timestampMs =
        typeof payload.timestamp_ms === 'number'
          ? payload.timestamp_ms
          : Date.now();
      const rawUsername =
        typeof payload.username === 'string' && payload.username.trim()
          ? payload.username.trim()
          : null;
      const rawUserId =
        typeof payload.user_id === 'number'
          ? payload.user_id
          : typeof payload.user_id === 'string'
            ? parseInt(payload.user_id, 10)
            : null;
      const messageId =
        typeof payload.message_id === 'string'
          ? payload.message_id
          : `${gameId ?? 'game'}-${timestampMs}-${Math.random().toString(36).slice(2, 8)}`;

      const normalized: ChatMessage = {
        id: messageId,
        scope: 'game',
        lobbyId: undefined,
        gameId,
        userId: Number.isFinite(rawUserId as number) ? (rawUserId as number) : null,
        username: rawUsername,
        message: payload.message,
        type: rawUsername ? 'user' : 'system',
        timestamp: new Date(timestampMs),
      };

      setGameChatMessages((previous) => {
        const isNewGame = typeof gameId === 'number' && gameChatIdRef.current !== gameId;
        if (isNewGame) {
          gameChatIdRef.current = gameId;
        }
        const base = isNewGame ? [] : previous;
        const next = [...base, normalized];
        if (next.length > MAX_CHAT_HISTORY) {
          return next.slice(next.length - MAX_CHAT_HISTORY);
        }
        return next;
      });
    });

    return cleanup;
  }, [onMessage]);

  useEffect(() => {
    const resetGameChat = (gameId: number | null) => {
      if (typeof gameId === 'number' && Number.isFinite(gameId)) {
        gameChatIdRef.current = gameId;
      } else {
        gameChatIdRef.current = null;
      }
      setGameChatMessages([]);
    };

    const extractGameId = (raw: any): number | null => {
      if (raw === null || raw === undefined) {
        return null;
      }
      if (typeof raw === 'number' && Number.isFinite(raw)) {
        return raw;
      }
      if (typeof raw === 'string' && raw.trim()) {
        const parsed = parseInt(raw, 10);
        return Number.isFinite(parsed) ? parsed : null;
      }
      if (typeof raw === 'object') {
        if (typeof raw.game_id === 'number') {
          return raw.game_id;
        }
        if (typeof raw.game_id === 'string') {
          const parsed = parseInt(raw.game_id, 10);
          return Number.isFinite(parsed) ? parsed : null;
        }
        if ('JoinGame' in raw) {
          return extractGameId((raw as any).JoinGame);
        }
        if ('data' in raw) {
          return extractGameId((raw as any).data);
        }
      }
      return null;
    };

    const cleanupJoin = onMessage('JoinGame', (message: any) => {
      const payload = message?.data ?? message?.JoinGame ?? message;
      resetGameChat(extractGameId(payload));
    });

    const cleanupCustomCreated = onMessage('CustomGameCreated', (message: any) => {
      const payload = message?.data ?? message?.CustomGameCreated ?? message;
      resetGameChat(extractGameId(payload));
    });

    const cleanupCustomJoined = onMessage('CustomGameJoined', (message: any) => {
      const payload = message?.data ?? message?.CustomGameJoined ?? message;
      resetGameChat(extractGameId(payload));
    });

    const cleanupSoloCreated = onMessage('SoloGameCreated', (message: any) => {
      const payload = message?.data ?? message?.SoloGameCreated ?? message;
      resetGameChat(extractGameId(payload));
    });

    const cleanupSpectator = onMessage('SpectatorJoined', () => {
      setGameChatMessages([]);
    });

    return () => {
      cleanupJoin();
      cleanupCustomCreated();
      cleanupCustomJoined();
      cleanupSoloCreated();
      cleanupSpectator();
    };
  }, [onMessage]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      disconnect();
    };
  }, [disconnect]);

  useEffect(() => {
    if (!hasLoadedStoredLobbyRef.current) {
      return;
    }

    if (!isConnected || !isSessionAuthenticated) {
      return;
    }

    if (currentLobby) {
      return;
    }

    if (!storedLobbyRef.current || !storedLobbyRef.current.code) {
      return;
    }

    if (restoreInProgressRef.current) {
      return;
    }

    let cancelled = false;
    restoreInProgressRef.current = true;

    const attemptRestore = async () => {
      const { code } = storedLobbyRef.current!;

      try {
        console.log(`Attempting to restore lobby ${code}`);
        await joinLobby(code);
      } catch (error) {
        if (cancelled) {
          return;
        }

        const message = error instanceof Error ? error.message : String(error ?? 'unknown error');
        const normalizedMessage = message.toLowerCase();

        if (normalizedMessage.includes('access denied') || normalizedMessage.includes('not found')) {
          console.warn('Stored lobby is no longer valid, clearing persisted lobby info');
          resetLobbyState();
          return;
        }

        console.warn('Failed to restore lobby from storage, not retrying automatically:', message);
      }
    };

    attemptRestore()
      .catch(error => {
        console.error('Failed to restore lobby from storage:', error);
      })
      .finally(() => {
        restoreInProgressRef.current = false;
      });

    return () => {
      cancelled = true;
    };
  }, [isConnected, isSessionAuthenticated, currentLobby, joinLobby, resetLobbyState]);

  const value: WebSocketContextType = {
    isConnected,
    isSessionAuthenticated,
    serverCapabilities,
    sendMessage,
    onMessage,
    connect,
    disconnect,
    connectToRegion,
    currentRegionUrl,
    latencyMs,
    currentLobby,
    lobbyMembers,
    lobbyChatMessages,
    gameChatMessages,
    lobbyPreferences,
    createLobby,
    joinLobby,
    leaveLobby,
    sendChatMessage,
    updateLobbyPreferences,
  };

  // Expose context for testing
  useEffect(() => {
    if (typeof window !== 'undefined') {
      window.__wsContext = value;
    }
  }, [value]);

  return (
    <WebSocketContext.Provider value={value}>
      {children}
    </WebSocketContext.Provider>
  );
};
