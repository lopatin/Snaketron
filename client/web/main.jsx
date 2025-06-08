import './index.css';
import * as wasm from "wasm-snaketron";

// import react and react-dom
import React from 'react';
import ReactDOM from 'react-dom/client';

// let game = wasm.Game.new(60, 40);

function Header() {
  return (
    <header className="site-header">
      <img src="/SnaketronLogo.png" alt="Snaketron" className="logo" />
    </header>
  );
}

function GameCanvas() {
  return (
    <canvas width="900" height="500" />
  );
}

function App() {
  return (
    <div className="app">
      <Header />
      <main className="game-container">
        <GameCanvas />
      </main>
    </div>
  );
}

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
