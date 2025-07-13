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
  game_id: string;
  game_ended?: boolean;
  final_score?: number;
  duration?: number;
  start_ms: number;
}

export type GameStatus = 
  | 'Stopped'
  | { Started: { server_id: number } }
  | { Complete: { winning_snake_id: number | null } };

export type GameType = 
  | { Custom: { settings: CustomGameSettings } }
  | 'QuickPlay'
  | 'Competitive';

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
  tactical_mode: boolean;
}

export type GameMode = 
  | 'Solo'
  | 'Duel'
  | { FreeForAll: { max_players: number } };

export interface GameProperties {
  available_food_target: number;
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

export interface CreateSoloGameMessage {
  CreateSoloGame: {
    mode: 'Classic' | 'Tactical';
  };
}

export interface GameCommandMessage {
  GameCommand: GameCommand;
}

export interface TokenMessage {
  Token: string;
}

export type WebSocketMessage = 
  | CreateCustomGameMessage
  | JoinCustomGameMessage
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
export type GameModeId = 'quick' | 'competitive' | 'solo-classic' | 'solo-tactical' | 'custom' | 'duel' | 'freeforall';

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
  tacticalMode: boolean;
  allowJoin: boolean;
  allowSpectators: boolean;
  snakeStartLength: number;
}