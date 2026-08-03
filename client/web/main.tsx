import './index.css';
import { initWasm } from './wasm';

// import react and react-dom
import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';

// Kick off WASM initialization; consumers await initWasm()/read getWasm().
initWasm().catch(error => {
  console.error('Failed to initialize WASM module', error);
});

// mount
const container = document.getElementById('root');
if (container) {
  const root = ReactDOM.createRoot(container);

  root.render(<App />);
}
