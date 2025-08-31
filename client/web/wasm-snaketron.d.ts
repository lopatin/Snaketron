declare module 'wasm-snaketron' {
  export interface Position {
    x: number;
    y: number;
  }

  export type GameStatus = 
    | 'Stopped'
    | { Started: { server_id: number } }
    | { Complete: { winning_snake_id: number | null } };

  export type GameType = 
    | { Custom: { settings: any } }
    | 'QuickPlay'
    | 'Competitive'
    | { TeamMatch: { per_team: number } }
    | 'Solo';

  export interface GameState {
    tick: number;
    status: GameStatus;
    arena: {
      width: number;
      height: number;
      snakes: Snake[];
      food: Position[];
    };
    game_type: GameType;
    properties: any;
    players: Record<number, { user_id: number; snake_id: number }>;
    game_id: string;
  }

  export interface Snake {
    body: Position[];
    direction: 'Up' | 'Down' | 'Left' | 'Right';
    is_alive: boolean;
    food: number;
  }

  export class GameClient {
    constructor(gameId: number, startMs: bigint);
    static newFromState(gameId: number, stateJson: string): GameClient;
    setLocalPlayerId(playerId: number): void;
    runUntil(timestampMs: bigint): string;
    rebuildPredictedState(timestampMs: bigint): void;
    processTurn(snakeId: number, direction: string): string;
    processServerEvent(eventMessageJson: string): void;
    initializeFromSnapshot(stateJson: string, currentTs: bigint): void;
    getGameStateJson(): string;
    getCommittedStateJson(): string;
    getEventLogJson(): string;
    getCurrentTick(): number;
    getCommittedTick(): number;
    getPredictedTick(): number;
    getGameId(): number;
    getSnakeIdForUser(userId: number): number | undefined;
  }

  export class Game {
    constructor();
    // Add any methods that Game class has
  }

  export function render(game: GameClient, canvas: HTMLCanvasElement): void;
  export function render_game(gameStateJson: string, canvas: HTMLCanvasElement, cellSize: number, localUserId: number | null): void;

  const init: () => Promise<void>;
  export default init;
}