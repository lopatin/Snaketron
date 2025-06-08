import './index.css';
import * as wasm from "wasm-snaketron";

// import react and react-dom
import React from 'react';
import ReactDOM from 'react-dom/client';

// let game = wasm.Game.new(60, 40);

function Header() {
  return (
    <header className="bg-white border-t-3 border-b-3 border-white py-5 pb-[18px] flex justify-center items-center relative site-header">
      <img src="/SnaketronLogo.png" alt="Snaketron" className="h-6 w-auto opacity-80" />
    </header>
  );
}

function GameCanvas() {
  return (
    <canvas width="900" height="500" className="block max-w-full h-auto border border-gray-100" />
  );
}

function App() {
  return (
    <div className="min-h-screen flex flex-col">
      <Header />

    <div className="flex flex-col gap-30 items-center mt-10">
        <div className="relative inline-block border border-black-70 border-r-2 border-b-2 rounded-lg overflow-hidden -skew-x-[10deg] cursor-pointer button-outer">
            <div className="bg-white p-[3px] m-0 cursor-pointer button-wrapper">
                <button className="relative w-[280px] py-18 px-0 bg-white text-black-70 text-18 font-black italic uppercase tracking-1 cursor-pointer text-center border border-black-70 rounded-[5px] skewed-button">
                    <span className="inline-block skew-x-[10deg]">SINGLE PLAYER</span>
                </button>
            </div>
        </div>
    </div>
        <main className="flex-1 flex justify-center items-center text-center p-5">
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