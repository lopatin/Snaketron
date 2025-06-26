declare module 'wasm-snaketron' {
  export interface Position {
    x: number;
    y: number;
  }

  export interface GameState {
    tick: number;
    status: any;
    arena: {
      width: number;
      height: number;
      snakes: Snake[];
      food: Position[];
    };
    game_type: any;
    properties: any;
    players: any;
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
    static newFromState(gameId: number, startMs: bigint, stateJson: string): GameClient;
    setLocalPlayerId(playerId: number): void;
    runUntil(timestampMs: bigint): string;
    rebuildPredictedState(timestampMs: bigint): void;
    processTurn(snakeId: number, direction: string): string;
    processServerEvent(eventMessageJson: string, currentTs: bigint): void;
    initializeFromSnapshot(stateJson: string, currentTs: bigint): void;
    getGameStateJson(): string;
    getCommittedStateJson(): string;
    getEventLogJson(): string;
    getCurrentTick(): number;
    getCommittedTick(): number;
    getPredictedTick(): number;
    getGameId(): number;
  }

  export class Game {
    constructor();
    // Add any methods that Game class has
  }

  export function render(game: GameClient, canvas: HTMLCanvasElement): void;
  export function render_game(gameStateJson: string, canvas: HTMLCanvasElement, cellSize: number): void;

  const init: () => Promise<void>;
  export default init;
}