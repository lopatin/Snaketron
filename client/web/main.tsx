import './index.css';
import { initWasm } from './wasm';

// import react and react-dom
import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';
import { crazyGames } from './services/crazyGames';
import { installScenarioCaptureNetworkStubs } from './utils/scenarioCaptureMode';

// Capture pages must remain deterministic and must never wait on app services.
// This runs before WASM initialization and, crucially, before React can mount
// any provider that might otherwise start a fetch or WebSocket connection.
installScenarioCaptureNetworkStubs();

// Kick off WASM initialization; consumers await initWasm()/read getWasm().
initWasm()
  .catch(error => {
    console.error('Failed to initialize WASM module', error);
  })
  .finally(() => {
    crazyGames.loadingStop();
  });

// mount
const container = document.getElementById('root');
if (container) {
  const root = ReactDOM.createRoot(container);

  root.render(<App />);
}
