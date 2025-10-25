import './index.css';
import * as wasm from 'wasm-snaketron';

// import react and react-dom
import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';

// Extend window interface to include wasm helpers
declare global {
  interface Window {
    wasm?: typeof wasm;
    wasmReady?: Promise<void>;
  }
}

// Initialize game client after WASM is loaded and expose readiness
let game: wasm.GameClient | null = null;
const wasmInitPromise = wasm
  .default()
  .then(() => {
    window.wasm = wasm;
    game = new wasm.GameClient(1, BigInt(Date.now()));
    console.log('GameClient initialized');
  })
  .catch(error => {
    console.error('Failed to initialize WASM module', error);
    throw error;
  });

window.wasmReady = wasmInitPromise;

console.log('hi from main.tsx');

// mount
const container = document.getElementById('root');
if (container) {
  const root = ReactDOM.createRoot(container);

  root.render(<App />);
}

// wasm.render(game, window.document.getElementById("gameCanvas"));
