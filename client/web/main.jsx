import './index.css';
import * as wasm from "wasm-snaketron";

// import react and react-dom
import React from 'react';
import ReactDOM from 'react-dom/client';

let game = wasm.Game.new(60, 40);

function GameCanvas() {
  return (
    <canvas width="900" height="500" />
  );
}

console.log('hi from main.jsx');

// mount
const container = document.getElementById('gameCanvas');
const root = ReactDOM.createRoot(container);

root.render(
    <React.StrictMode>
      <GameCanvas />
    </React.StrictMode>
);

// wasm.render(game, window.document.getElementById("gameCanvas"));
