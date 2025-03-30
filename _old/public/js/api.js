(function() {
  define(['underscore', 'socketio'], function(_, io) {
    var Api, l, socket;

    Api = {};
    l = window.location;
    socket = io.connect(l.protocol + l.host);
    Api.call = function(method, args, callback) {
      var options;

      if (method == null) {
        return;
      }
      if (args == null) {
        args = {};
      }
      options = {
        method: method,
        args: args
      };
      if (typeof args === "function") {
        callback = args;
        delete options.args;
      }
      return socket.emit('api', options, callback);
    };
    Api.listen = function(message, action) {
      if (!((message != null) && (action != null))) {
        return;
      }
      return socket.on(message, action);
    };
    return Api;
  });

}).call(this);
