import './index.css';
import * as wasm from "wasm-snaketron";

console.log('hi from index.js');

let game = wasm.Game.new(60, 40);

console.log('got game');


wasm.render(game, window.document.getElementById("gameCanvas"));
