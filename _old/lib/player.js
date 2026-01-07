(function() {
  define(['underscore', 'redisclient'], function(_, redis) {
    var Player;

    return Player = (function() {
      function Player(user, socket) {
        this.user = user;
        this.socket = socket;
      }

      Player.prototype.emit = function(message, data, callback) {
        if (typeof data === "undefined") {
          data = callback = void 0;
        }
        if (typeof data === "function") {
          callback = data;
          data = {};
        }
        if (this.socket) {
          return this.socket.emit(message, data, callback);
        }
      };

      Player.prototype.getId = function() {
        return this.user.id;
      };

      return Player;

    })();
  });

}).call(this);
