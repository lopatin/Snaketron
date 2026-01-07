(function() {
  define(['underscore', 'node-uuid', 'common/models/game'], function(_, uuid, GameModel) {
    var Match;

    Match = (function() {
      function Match(capacity) {
        this.capacity = capacity;
        this.id = uuid.v4();
        this.players = {};
        this.status = 'idle';
      }

      Match.prototype.addPlayer = function(player) {
        return this.players[player.getId()] = player;
      };

      Match.prototype.isReadyToStart = function() {
        return _.size(this.players) === this.capacity && this.status === 'idle';
      };

      Match.prototype.start = function() {
        var self;

        if (!this.isReadyToStart()) {
          return;
        }
        this.status = 'started';
        this.masterGame = new GameModel();
        self = this;
        return _.each(this.players, function(player) {
          return player.emit('start', {
            matchId: self.getId()
          });
        });
      };

      Match.prototype.getId = function() {
        return this.id;
      };

      return Match;

    })();
    return Match;
  });

}).call(this);
