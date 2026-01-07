(function() {
  define(['underscore', 'redisclient', 'player', 'match', 'playermanager'], function(_, redis, Player, Match, PlayerManager) {
    var Snaketron;

    Snaketron = (function() {
      function Snaketron() {
        this._bindApiMethods();
        this.players = new PlayerManager();
        this.matches = {};
        this.queuedMatches = [];
        setInterval(this._emitPlayerCounts.bind(this), 2000);
      }

      Snaketron.prototype.socketConnected = function(socket, user) {
        if (user) {
          this.players.userConnected(user, socket);
        }
        return this._emitPlayerCounts();
      };

      Snaketron.prototype.socketDisconnected = function(socket, user) {
        if (user) {
          this.players.userDisconnected(user, socket);
        }
        return this._emitPlayerCounts();
      };

      Snaketron.prototype._emitPlayerCounts = function() {
        var count;

        count = this.players.getCount();
        if (this._lastCount !== count) {
          this.players.emit('playerCount', this.players.getCount());
        }
        return this._lastCount = count;
      };

      Snaketron.prototype._bindApiMethods = function() {
        var self;

        self = this;
        return _.each(this._api, function(method, key) {
          return self._api[key] = method.bind(self);
        });
      };

      Snaketron.prototype.apiCall = function(method, args, callback) {
        var error, result;

        if (!((method != null) && (this._api[method] != null))) {
          error = "Invalid API method";
        } else if (args['user'] == null) {
          error = "API calls require a user";
        } else {
          result = this._api[method](args);
        }
        if (callback != null) {
          if (error != null) {
            return callback(error);
          } else {
            return callback(null, result);
          }
        }
      };

      Snaketron.prototype._api = {
        testApiCall: function(args) {
          return args.user.name;
        },
        hartbeet: function(args) {
          return this.players.heartbeat(args.user);
        },
        getPlayerCount: function(args) {
          return this.players.getCount();
        },
        getUser: function(args) {
          return args.user;
        },
        requestQuickMatch: function(args) {
          var match, player;

          player = this.players.get(args.user.id);
          if (this.queuedMatches.length) {
            match = this.queuedMatches[0];
            match.addPlayer(player);
            if (match.isReadyToStart()) {
              this.queuedMatches.shift();
              match.start();
            }
          } else {
            match = new Match(2);
            match.addPlayer(player);
            this.queuedMatches.push(match);
            this.matches[match.getId()] = match;
          }
          return match.getId();
        }
      };

      return Snaketron;

    })();
    return new Snaketron();
  });

}).call(this);
