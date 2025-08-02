import './index.css';
import * as wasm from "wasm-snaketron";

// import react and react-dom
import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';

// Extend window interface to include wasm
declare global {
  interface Window {
    wasm: typeof wasm;
  }
}

// Initialize game client after WASM is loaded
let game: wasm.GameClient | null = null;
wasm.default().then(() => {
  game = new wasm.GameClient(1, BigInt(Date.now()));
  // Expose wasm to window for components to use
  window.wasm = wasm;
  console.log('GameClient initialized');
});

console.log('hi from main.tsx');

// mount
const container = document.getElementById('root');
if (container) {
  const root = ReactDOM.createRoot(container);
  
  root.render(
    <App />
  );
}

// wasm.render(game, window.document.getElementById("gameCanvas"));