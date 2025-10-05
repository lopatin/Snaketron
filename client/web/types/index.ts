// User and Authentication Types
export interface User {
  id: number;
  username: string;
  token?: string;
}

export interface AuthContextType {
  user: User | null;
  loading: boolean;
  login: (username: string, password: string) => Promise<void>;
  register: (username: string, password: string | null) => Promise<void>;
  logout: () => void;
  getToken: () => string | null;
}

// WebSocket Types
export interface WebSocketContextType {
  isConnected: boolean;
  sendMessage: (message: any) => void;
  onMessage: (type: string, handler: (message: any) => void) => () => void;
  connect: (url: string, onConnect?: () => void) => void;
  connectToRegion: (wsUrl: string) => void;
  currentRegionUrl: string | null;
  latencyMs: number;
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
  game_id: string;
  game_ended?: boolean;
  final_score?: number;
  duration?: number;
  start_ms: number;
  event_sequence: number;
  scores: Record<number, number>;  // Snake ID to score mapping
  team_scores?: Record<number, number>;  // Team ID to team score mapping (for team games)

  // Round-based scoring fields
  current_round: number;                     // Current round number (1, 2, 3...)
  round_wins: Record<number, number>;        // TeamId to rounds won mapping
  rounds_to_win: number;                     // 1 for quick match, 2 for competitive
  round_start_times: number[];              // Start time of each round (ms timestamps)
  is_transitioning: boolean;                // True during round transitions

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
  timestamp: number;
}

export interface RegionSelectorProps {
  regions: Region[];
  currentRegionId: string;
  onRegionChange: (regionId: string) => void;
}