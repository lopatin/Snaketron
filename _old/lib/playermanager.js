(function() {
  define(['underscore', 'redisclient', 'player'], function(_, redis, Player) {
    var PlayerManager;

    return PlayerManager = (function() {
      function PlayerManager() {
        this.players = {};
        this.count = 0;
        redis.del('connected_players');
        setInterval(this._updateCount.bind(this), 1000);
      }

      PlayerManager.prototype.add = function(player) {
        return this.players[player.getId()] = player;
      };

      PlayerManager.prototype.remove = function(player) {
        if (player == null) {
          return;
        }
        redis.srem('connected_players', player.socket.id);
        return delete this.players[player.getId()];
      };

      PlayerManager.prototype.get = function(id) {
        return this.players[id];
      };

      PlayerManager.prototype.emit = function(message, data) {
        return _.each(this.players, function(player) {
          return player.emit(message, data);
        });
      };

      PlayerManager.prototype.userConnected = function(user, socket) {
        var player;

        player = new Player(user, socket);
        return this.add(player);
      };

      PlayerManager.prototype.userDisconnected = function(user, socket) {
        return this.remove(this.players[user.id]);
      };

      PlayerManager.prototype.heartbeat = function(user) {
        var player;

        player = this.players[user.id];
        if (player == null) {
          return;
        }
        if (player.hb_timeout) {
          clearTimeout(player.hb_timeout);
        } else {
          redis.sadd('connected_players', player.socket.id);
        }
        return player.hb_timeout = setTimeout((function() {
          return this.remove.bind(this, player);
        }).bind(this), 4100);
      };

      PlayerManager.prototype.getCount = function() {
        return this.count;
      };

      PlayerManager.prototype._updateCount = function() {
        var self;

        self = this;
        return redis.scard('connected_players', function(err, val) {
          return self.count = val;
        });
      };

      return PlayerManager;

    })();
  });

}).call(this);
