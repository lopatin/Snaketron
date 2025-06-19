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
    constructor(userId: number, timestamp: bigint);
    step(): void;
    get_state(): GameState;
  }

  export function render(game: GameClient, canvas: HTMLCanvasElement): void;

  const init: () => Promise<void>;
  export default init;
}