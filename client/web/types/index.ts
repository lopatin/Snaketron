// User and Authentication Types
export interface User {
  id: number;
  username: string;
  token?: string;
  isGuest?: boolean;
}

export interface AuthContextType {
  user: User | null;
  loading: boolean;
  login: (username: string, password: string) => Promise<void>;
  register: (username: string, password: string | null) => Promise<void>;
  createGuest: (nickname: string) => Promise<{ user: User; token: string }>;
  updateGuestNickname: (nickname: string) => void;
  logout: () => void;
  getToken: () => string | null;
}

// Lobby Types
export interface Lobby {
  id: number;
  code: string;
  hostUserId: number;
  region: string;
  state: LobbyState;
}

export interface LobbyMember {
  user_id: number;
  username: string;
  joined_at: number;
  is_host: boolean;
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

export type LobbyState = 'waiting' | 'queued' | 'matched';
export type LobbyGameMode = 'duel' | '2v2' | 'solo' | 'ffa';

export interface LobbyPreferences {
  selectedModes: LobbyGameMode[];
  competitive: boolean;
}

// WebSocket Types
export interface WebSocketContextType {
  isConnected: boolean;
  sendMessage: (message: any) => void;
  onMessage: (type: string, handler: (message: any) => void) => () => void;
  connect: (url: string, onConnect?: () => void) => void;
  disconnect: () => void;
  connectToRegion: (wsUrl: string, options?: { regionId?: string; origin?: string }) => void;
  currentRegionUrl: string | null;
  latencyMs: number;

  // Lobby state
  currentLobby: Lobby | null;
  lobbyMembers: LobbyMember[];
  lobbyChatMessages: ChatMessage[];
  gameChatMessages: ChatMessage[];
  lobbyPreferences: LobbyPreferences | null;

  // Lobby methods
  createLobby: () => Promise<void>;
  joinLobby: (lobbyCode: string) => Promise<void>;
  leaveLobby: () => Promise<void>;
  sendChatMessage: (scope: ChatScope, message: string) => void;
  updateLobbyPreferences: (preferences: LobbyPreferences) => void;
}

// Latency Settings Types
export interface LatencySettings {
  enabled: boolean;
  sendDelayMs: number;
  receiveDelayMs: number;
}

// Game Types
export interface Position {
  x: number;
  y: number;
}

export interface Snake {
  body: Position[];
  direction: 'Up' | 'Down' | 'Left' | 'Right';
  is_alive: boolean;
  food: number;
  team_id?: number | null;  // Team ID: 0 or 1 for team games
}

export interface Arena {
  width: number;
  height: number;
  snakes: Snake[];
  food: Position[];
}

export interface GameState {
  tick: number;
  status: GameStatus;
  arena: Arena;
  game_type: GameType;
  properties: GameProperties;
  players: Record<number, Player>;
  usernames: Record<number, string>;  // Username mappings by user_id
  spectators: number[];               // User IDs that are spectating (no snakes)
  game_id: string;
  game_ended?: boolean;
  final_score?: number;
  duration?: number;
  start_ms: number;
  event_sequence: number;
  scores: Record<number, number>;  // Snake ID to score mapping
  team_scores?: Record<number, number>;  // Team ID to team score mapping (for team games)

  // XP tracking (only present after game completion)
  player_xp?: Record<number, number>;       // user_id -> xp_gained
}

export type GameStatus = 
  | 'Stopped'
  | { Started: { server_id: number } }
  | { Complete: { winning_snake_id: number | null } };

export type GameType =
  | 'Solo'
  | { TeamMatch: { per_team: number } }
  | { FreeForAll: { max_players: number } }
  | { Custom: { settings: CustomGameSettings } };

export type QueueMode = 'Quickmatch' | 'Competitive';

export interface CustomGameSettings {
  arena_width: number;
  arena_height: number;
  tick_duration_ms: number;
  food_spawn_rate: number;
  max_players: number;
  game_mode: GameMode;
  is_private: boolean;
  allow_spectators: boolean;
  snake_start_length: number;
}

export type GameMode = 
  | 'Solo'
  | 'Duel'
  | { FreeForAll: { max_players: number } };

export interface GameProperties {
  available_food_target: number;
  tick_duration_ms: number;
  time_limit_ms?: number | null;
}

export interface Player {
  user_id: number;
  snake_id: number;
}

// Game Command Types
export interface GameCommand {
  command_id_client: {
    tick: number;
    user_id: number;
    sequence_number: number;
  };
  command_id_server: null;
  command: Command;
}

export type Command = 
  | { Turn: { direction: 'Up' | 'Down' | 'Left' | 'Right' } }
  | 'Respawn';

// WebSocket Message Types
export interface CreateCustomGameMessage {
  CreateCustomGame: {
    settings: Partial<CustomGameSettings>;
  };
}

export interface JoinCustomGameMessage {
  JoinCustomGame: {
    game_code: string;
  };
}

export interface JoinGameMessage {
  JoinGame: number;
}

export interface QueueForMatchMessage {
  QueueForMatch: {
    game_type: GameType;
    queue_mode: QueueMode;
  };
}

export interface QueueForMatchMultiMessage {
  QueueForMatchMulti: {
    game_types: GameType[];
    queue_mode: QueueMode;
  };
}

export type CreateSoloGameMessage = 'CreateSoloGame';

export interface GameCommandMessage {
  GameCommand: GameCommand;
}

export interface TokenMessage {
  Token: string;
}

export type WebSocketMessage =
  | CreateCustomGameMessage
  | JoinCustomGameMessage
  | JoinGameMessage
  | QueueForMatchMessage
  | QueueForMatchMultiMessage
  | CreateSoloGameMessage
  | GameCommandMessage
  | TokenMessage
  | string;

// API Response Types
export interface ApiResponse<T> {
  data?: T;
  error?: string;
  message?: string;
}

export interface LoginResponse {
  token: string;
  user: User;
}

export interface CreateGuestResponse {
  token: string;
  user: User;
}

export interface CheckUsernameResponse {
  available: boolean;
  requiresPassword?: boolean;
  errors?: string[];
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

// Region metadata from backend API
export interface RegionMetadata {
  id: string;
  name: string;
  origin: string;
  ws_url: string;  // Backend uses snake_case
}

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
