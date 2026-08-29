// Wire types are generated from the Rust source of truth via ts-rs
// (scripts/gen-types.sh -> client/web/types/generated/). They are re-exported
// here so the rest of the app keeps importing them from '../types'. Do not
// hand-write these — change the Rust definition and regenerate.
export type {
  Position,
  Snake,
  SnakeBoost,
  SnakeCombo,
  Arena,
  BoostConfig,
  BoostPad,
  ComboConfig,
  TeamZoneConfig,
  TeamId,
  Player,
  GameState,
  GameStatus,
  GameType,
  GameMode,
  QueueMode,
  GameProperties,
  DeathCause,
  HighlightClip,
  ScenarioAddons,
  ScenarioPlaybackSegment,
  ScenarioPresentation,
  ScenarioScript,
  CustomGameSettings,
  GameEvent,
  GameEventMessage,
  GameCommandMessage,
  CommandId,
  SyncStatus,
  AdBreakResolution,
  BannerAdsConfig,
  ClientAdsConfig,
  LobbyAdBreakView,
  VideoAdsConfig,
  LobbyMember,
  WSMessage,
  // HTTP DTOs (server/src/api/*)
  UserInfo,
  GuestUserInfo,
  AuthResponse,
  CreateGuestResponse,
  LeaderboardEntryResponse,
  HighScoreEntryResponse,
  LeaderboardEntry,
  LeaderboardResponse,
  SeasonsResponse,
  UserRankingResponse,
  RegionMetadata,
  HealthResponse,
  MatchHistoryPage,
  MatchHistoryPlayer,
  MatchHistorySummary,
  PublicRuntimeConfig,
  RuntimeAdsConfig,
  RuntimeAnnouncementConfig,
  RuntimeConfig,
  RuntimeConfigActor,
  RuntimeConfigAuditPage,
  RuntimeConfigRecord,
  RuntimeHistoryConfig,
  UpdateRuntimeConfigRequest,
} from './generated';

// Typed WebSocket protocol surface derived from the generated WSMessage union.
import type { OutboundMessage, WSMessageTag, TypedMessage } from './protocol';
import type {
  Challenge,
  ChallengeInbox,
  RematchState,
  ClientAdsConfig,
  Direction,
  LobbyAdBreakView,
  LobbyMember,
  OnlinePlayer,
  RegionRoster,
} from './generated';
export type { Challenge, ChallengeInbox, OnlinePlayer, RegionRoster, RematchState, RematchParticipant } from './generated';
export type { OutboundMessage, WSMessageTag, TypedMessage, PayloadOf } from './protocol';

// Leaderboard entry aliases: the components predate the generated names but the
// shapes are identical, so alias rather than rename call sites.
import type {
  LeaderboardEntryResponse as GenRankingEntry,
  HighScoreEntryResponse as GenHighScoreEntry,
  LeaderboardEntry as GenLeaderboardEntry,
} from './generated';
export type RankingEntry = GenRankingEntry;
export type HighScoreEntry = GenHighScoreEntry;

// User and Authentication Types
export interface User {
  id: number;
  username: string;
  mmr?: number;
  token?: string;
  isGuest?: boolean;
  isAdmin?: boolean;
  authSource?: 'crazygames' | string;
  avatarUrl?: string | null;
  /**
   * What this player is wearing, as the account has it. Mirrors the server's
   * `UserInfo` (see `types/generated/UserInfo.ts`), which sends both fields
   * with every authenticated user. Optional here because the guest-creation
   * response omits them — a brand-new guest is wearing nothing yet.
   *
   * This is the authority on the snake skin: it is what match preparation
   * reads and hands to every other player. Local storage only holds a choice
   * made before there was an account to write it to.
   */
  selectedSkin?: string | null;
  selectedBase?: string | null;
}

export type CrazyGamesSessionStatus =
  | 'not-applicable'
  | 'resolving'
  | 'linked'
  | 'guest'
  | 'error';

export interface AuthContextType {
  user: User | null;
  loading: boolean;
  login: (username: string, password: string) => Promise<void>;
  register: (username: string, password: string | null) => Promise<void>;
  createGuest: (nickname: string) => Promise<{ user: User; token: string }>;
  ensurePlayableSession: (nickname?: string) => Promise<{ user: User; token: string }>;
  updateGuestNickname: (nickname: string) => void;
  logout: () => void;
  getToken: () => string | null;
  crazyGamesSessionStatus: CrazyGamesSessionStatus;
  crazyGamesSessionError: string | null;
  retryCrazyGamesSession: () => Promise<void>;
  beginCrazyGamesAccountTransition: () => void;
  crazyGamesAccountTransitionSequence: number;
}

// Lobby Types
export interface Lobby {
  id: number | null;
  code: string;
  hostUserId: number;
  region: string;
  state: LobbyState;
  adBreak?: LobbyAdBreakView | null;
}

export type ChatScope = 'lobby' | 'game';

export interface ChatMessage {
  id: string;
  scope: ChatScope;
  lobbyId?: number;
  gameId?: number;
  userId: number | null;
  username: string | null;
  message: string;
  type: 'user' | 'system';
  timestamp: Date;
}

export type LobbyState = 'waiting' | 'ad_break' | 'queued' | 'matched';
export type LobbyGameMode = 'duel' | '2v2' | 'solo' | 'ffa';
export type MatchmakingStatus = 'idle' | 'queued' | 'joining';

// Client-side lobby preferences (camelCase UI shape). The wire equivalent is
// the generated LobbyPreferences (snake_case selected_modes); WebSocketContext
// translates between the two.
export interface LobbyPreferences {
  selectedModes: LobbyGameMode[];
  competitive: boolean;
}

// WebSocket Types
export interface WebSocketContextType {
  isConnected: boolean;
  isSessionAuthenticated: boolean;
  serverCapabilities: ReadonlySet<string>;
  sendMessage: (message: OutboundMessage) => boolean;
  waitForSessionReady: (timeoutMs?: number) => Promise<void>;
  onMessage: <K extends WSMessageTag>(
    type: K,
    handler: (message: TypedMessage<K>) => void,
  ) => () => void;
  connect: (url: string, onConnect?: () => void) => void;
  disconnect: () => void;
  connectToRegion: (
    wsUrl: string,
    options?: { regionId?: string; origin?: string; forceReconnect?: boolean }
  ) => void;
  currentRegionUrl: string | null;
  latencyMs: number;
  adConfiguration: ClientAdsConfig;

  // Lobby state
  lobbyRestorationComplete: boolean;
  currentLobby: Lobby | null;
  /**
   * Whether this player may change the game mode or start matchmaking.
   *
   * Both are lobby-wide actions, so the server admits them only from the
   * lobby's host. A player with no lobby is also `true`: they are about to
   * create one and become its host, and gating them would make the ordinary
   * solo case unplayable.
   */
  isLobbyLeader: boolean;
  lobbyMembers: LobbyMember[];
  lobbyChatMessages: ChatMessage[];
  gameChatMessages: ChatMessage[];
  lobbyPreferences: LobbyPreferences | null;
  matchmakingStatus: MatchmakingStatus;
  setMatchmakingStatus: (status: MatchmakingStatus) => void;

  // Lobby methods
  createLobby: () => Promise<void>;
  joinLobby: (lobbyCode: string) => Promise<void>;
  leaveLobby: () => Promise<void>;
  clearSessionForAccountChange: () => void;
  sendChatMessage: (scope: ChatScope, message: string) => void;
  updateLobbyPreferences: (preferences: LobbyPreferences) => void;

  // Social layer. `null` means the server has not sent a roster yet, which is
  // different from "nobody is online" and is why the panel can stay hidden
  // until there is something true to say.
  onlinePlayers: RegionRoster | null;
  challenges: ChallengeInbox;
  /** Most recent challenge failure, for display; cleared by the next action. */
  challengeError: string | null;
  challengePlayer: (userId: number) => void;
  /** Live rematch state for the game this socket is in, if any. */
  rematchState: RematchState | null;
  setRematchIntent: (gameId: number, optIn: boolean) => void;
  respondToChallenge: (challengeId: string, accept: boolean) => void;
  cancelChallenge: (challengeId: string) => void;
  dismissChallengeError: () => void;
}

// Latency Settings Types
export interface LatencySettings {
  enabled: boolean;
  sendDelayMs: number;
  receiveDelayMs: number;
}

// Client-side gameplay input. The snake_id and command envelope are filled in
// by the WASM engine, which returns the wire GameCommandMessage. Boost carries
// no client-selected speed, duration, or charge.
export type Command =
  | { Turn: { direction: Direction } }
  | 'ActivateBoost'
  | 'DeactivateBoost'
  | 'PlayerActivity'
  | 'Respawn';

// Command-protocol helper types for the v2 at-least-once command path. The
// wire types are generated (server/src/recovery.rs + common); these compose
// the client's view around them. `GameCommand` is the client's historical name
// for the command envelope, which is the generated wire GameCommandMessage.
export type { GameCommandMessage as GameCommand, ClientCommandIdentityV2, CommandOutcome, SessionCommandRejectionFence } from './generated';
import type {
  GameCommandMessage as GenGameCommand,
  ClientCommandIdentityV2 as GenClientCommandIdentityV2,
  CommandOutcome as GenCommandOutcome,
  SessionCommandRejectionFence as GenRejectionFence,
} from './generated';

export interface GameCommandV2 {
  command_id: GenClientCommandIdentityV2;
  command: GenGameCommand;
}

// Alias kept for existing call sites (wire name is SessionCommandRejectionFence).
export type CommandRejectionFence = GenRejectionFence;

export interface CommandOutcomesPayload {
  game_id: number;
  client_game_session_id: string;
  contiguous_through: number;
  outcomes: Record<string, GenCommandOutcome>;
  rejection_fence?: GenRejectionFence;
}

export interface CommandOutcomesCompleteMessage {
  CommandOutcomesComplete: {
    game_id: number;
    terminal_rejection_reason?: string;
  };
}

// Game load failure (client-side view of WSMessage::GameLoadFailed)
export interface GameLoadFailure {
  gameId: number | null;
  requestedGameId: string;
  reason: string;
}

// API Response Types
export interface ApiResponse<T> {
  data?: T;
  error?: string;
  message?: string;
}

// Client-side result of the username check. `requiresPassword` is NOT part of
// the server's wire response (generated CheckUsernameResponse is { available,
// errors }); it is a client field the UI reads to decide whether to prompt for
// a password. The server does not populate it today, so api.checkUsername
// always sets it false.
export interface CheckUsernameResult {
  available: boolean;
  requiresPassword: boolean;
  errors: string[];
}

// Component Props Types
export interface ProtectedRouteProps {
  children: React.ReactNode;
}

export interface JoinGameModalProps {
  isOpen: boolean;
  onClose: () => void;
}

export interface AuthModalProps {
  isOpen: boolean;
  onClose: () => void;
}

export interface AnimatedRoutesProps {
  children: React.ReactNode;
}

export interface UsernameAuthProps {
  onAuthenticated: (user: { username: string }) => void;
}

// Event Handler Types
export type FormEventHandler = React.FormEventHandler<HTMLFormElement>;
export type ChangeEventHandler = React.ChangeEventHandler<HTMLInputElement>;
export type KeyboardEventHandler = React.KeyboardEventHandler<HTMLDivElement>;

// Ref Types
export type InputRef = React.RefObject<HTMLInputElement>;
export type CanvasRef = React.RefObject<HTMLCanvasElement>;

// State Types
export type UsernameStatus = 'authenticated' | 'available' | 'exists' | null;
export type GameModeId = 'quick' | 'competitive' | 'solo' | 'custom' | 'duel' | 'freeforall';

// Index Signature Types
export interface SpeedMap {
  slow: number;
  normal: number;
  fast: number;
  extreme: number;
}

export interface FoodSpawnMap {
  low: number;
  medium: number;
  high: number;
  extreme: number;
}

// Game Lobby Types
export interface LobbyPlayer {
  id: number;
  name: string;
  isHost: boolean;
  isReady: boolean;
}

export interface LobbySettings {
  gameMode: string;
  maxPlayers: number;
  mapSize: string;
  gameSpeed: string;
  powerUps: boolean;
}

// Game Settings for UI
export interface UIGameSettings {
  gameMode: 'solo' | 'duel' | 'freeForAll';
  arenaWidth: number;
  arenaHeight: number;
  maxPlayers: number;
  foodSpawnRate: 'low' | 'medium' | 'high' | 'extreme';
  gameSpeed: 'slow' | 'normal' | 'fast' | 'extreme';
  allowJoin: boolean;
  allowSpectators: boolean;
  snakeStartLength: number;
}

// Arena rotation angles (in degrees)
export type ArenaRotation = 0 | 90 | 180 | 270;

// Region Types
export interface Region {
  id: string;
  name: string;
  origin: string;        // HTTP origin e.g., "https://use1.snaketron.io" or "http://localhost:8080"
  wsUrl: string;         // WebSocket URL e.g., "wss://use1.snaketron.io/ws"
  userCount: number;
  ping: number | null;
  isConnected: boolean;
}

// RegionMetadata is generated from the server (server/src/api/regions.rs) and
// re-exported at the top of this file.

// localStorage schema for region preference
export interface RegionPreference {
  regionId: string;
  wsUrl?: string;
  origin?: string;
  timestamp: number;
}

export interface RegionSelectorProps {
  regions: Region[];
  currentRegionId: string;
  onRegionChange: (regionId: string) => void;
}

// Leaderboard Types
export type RankTier = 'bronze' | 'silver' | 'gold' | 'platinum' | 'diamond' | 'master' | 'grandmaster';
export type RankDivision = 1 | 2 | 3;

export interface Rank {
  tier: RankTier;
  division: RankDivision;
  mmr: number;
}

// Type guards for discriminating the (untagged) leaderboard entry union.
export function isRankingEntry(entry: GenLeaderboardEntry): entry is GenRankingEntry {
  return 'mmr' in entry;
}

export function isHighScoreEntry(entry: GenLeaderboardEntry): entry is GenHighScoreEntry {
  return 'score' in entry;
}

export interface LeaderboardData {
  season: number;
  gameMode: LobbyGameMode;
  entries: GenLeaderboardEntry[];
}
