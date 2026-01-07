(function() {
  define(['redisclient'], function(redis) {
    var PlayerCounter;

    return PlayerCounter = (function() {
      function PlayerCounter(timeoutCallback) {
        redis.del('connected_players');
        this.count = 0;
        setInterval(this.updateCount.bind(this), 1000);
        if (typeof timeoutCallback === "function") {
          this.timeout = timeoutCallback;
        }
      }

      PlayerCounter.prototype.add = function(player) {
        var self;

        self = this;
        player.socket.on('hartbeet', this.heartbeat.bind(this, player));
        return player.socket.on('disconnect', this.remove.bind(this, player));
      };

      PlayerCounter.prototype.remove = function(player) {
        return redis.srem('connected_players', player.socket.id);
      };

      PlayerCounter.prototype.heartbeat = function(player) {
        if (player.hb_timeout) {
          clearTimeout(player.hb_timeout);
        } else {
          redis.sadd('connected_players', player.socket.id);
        }
        return player.hb_timeout = setTimeout(this.remove.bind(this, player), 4100);
      };

      PlayerCounter.prototype.updateCount = function() {
        var self;

        self = this;
        return redis.scard('connected_players', function(err, val) {
          return self.count = val;
        });
      };

      return PlayerCounter;

    })();
  });

}).call(this);
