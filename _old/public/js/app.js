(function() {
  define(['underscore', 'backbone', 'api', 'models/game'], function(_, Backbone, Api, GameModel) {
    var App;

    App = (function() {
      function App() {
        _.bindAll(this);
      }

      App.prototype.init = function(view, router) {
        this.view = view;
        this.router = router;
        this._startHeartbeat();
        Api.call('getUser', this._fetchedUser);
        Api.listen('playerCount', function(count) {
          return console.log(count);
        });
        Api.listen('startGame', this._startGame);
        return Api.listen('updatedRoster', this._updateRoster);
      };

      App.prototype.getUser = function() {
        return this.user;
      };

      App.prototype.requestQuickMatch = function() {
        var game;

        game = this._createGame();
        return Api.call('requestQuickMatch', (function(err, matchId) {
          game.set('matchId', matchId);
          return this.router.navigate('play', {
            trigger: true
          });
        }).bind(this));
      };

      App.prototype._createGame = function() {
        var game, self;

        self = this;
        game = this.game = new GameModel({
          client: this.getUser
        });
        return game;
      };

      App.prototype._startGame = function(matchId) {
        if (this.game && this.game.get('matchId') === matchId) {
          return this.game.start();
        }
      };

      App.prototype._fetchedUser = function(err, user) {
        if (err) {
          return console.log(err);
        } else {
          this.user = user;
          this.view.showIndexView();
          return console.log("Logged in user: " + user.name);
        }
      };

      App.prototype._startHeartbeat = function() {
        Api.call('hartbeet');
        return setInterval(function() {
          return Api.call('hartbeet');
        }, 1000);
      };

      return App;

    })();
    window.stapp = new App();
    return window.stapp;
  });

}).call(this);
