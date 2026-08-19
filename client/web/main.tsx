import './index.css';
import { initWasm } from './wasm';

// import react and react-dom
import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';
import { crazyGames } from './services/crazyGames';
import { analytics } from './services/analytics';
import { api } from './services/api';
import { installScenarioCaptureNetworkStubs, isScenarioCaptureMode } from './utils/scenarioCaptureMode';

// Capture pages must remain deterministic and must never wait on app services.
// This runs before WASM initialization and, crucially, before React can mount
// any provider that might otherwise start a fetch or WebSocket connection.
installScenarioCaptureNetworkStubs();

// Resolve the analytics exclusion gate and, if this session counts, start the
// GameAnalytics session. Nothing here is awaited: the SDK loads in its own
// chunk and buffers whatever the game reports meanwhile, so a slow or blocked
// analytics host can never delay the game starting.
//
// Capture pages are a recording harness, not a play session, and are held to
// the same "no app services" rule as the rest of this file.
if (!isScenarioCaptureMode()) {
  void analytics.start({
    consent: () => api.getAnalyticsConsent(),
    resolveUserId: () => api.getAuthenticatedUserId(),
  });
}

// Kick off WASM initialization; consumers await initWasm()/read getWasm().
initWasm()
  .catch(error => {
    console.error('Failed to initialize WASM module', error);
    // The game is unplayable from here, which is the one client failure worth
    // an error event: it is invisible in gameplay funnels but shows up as a
    // player who opened the page and never started a match.
    analytics.trackError(
      'critical',
      `wasm-init: ${error instanceof Error ? error.message : String(error)}`,
    );
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
