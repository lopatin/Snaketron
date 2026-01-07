// Central export for all page objects
const { HomePage } = require('./home-page.js');
const { CustomGamePage } = require('./custom-game-page.js');
const { GameLobbyPage } = require('./game-lobby-page.js');
const { GameArenaPage } = require('./game-arena-page.js');

module.exports = {
  HomePage,
  CustomGamePage,
  GameLobbyPage,
  GameArenaPage
};