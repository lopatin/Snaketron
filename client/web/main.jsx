import './index.css';
import * as wasm from "wasm-snaketron";

// import react and react-dom
import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App.jsx';

// Initialize game client after WASM is loaded
let game = null;
wasm.default().then(() => {
  game = new wasm.GameClient(1, BigInt(Date.now()));
  // Expose wasm to window for components to use
  window.wasm = wasm;
  console.log('GameClient initialized');
});

console.log('hi from main.jsx');

// mount
const container = document.getElementById('root');
const root = ReactDOM.createRoot(container);

root.render(
    <React.StrictMode>
      <App />
    </React.StrictMode>
);

// wasm.render(game, window.document.getElementById("gameCanvas"));