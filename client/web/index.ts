import './index.css';
import * as wasm from "wasm-snaketron";

console.log('hi from index.js');

// Game instantiation would go here if Game class existed
// let game = new wasm.Game();

console.log('got game');


const canvas = window.document.getElementById("gameCanvas") as HTMLCanvasElement;
if (canvas) {
  // wasm.render(game, canvas);
}
