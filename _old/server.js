(function() {
  var requirejs;

  requirejs = require('requirejs');

  requirejs.config({
    baseUrl: __dirname + "/lib",
    paths: {
      common: __dirname + "/public/js"
    },
    nodeRequire: require
  });

  requirejs(['snaketron', 'express', 'socket.io', 'connect-redis', 'session.socket.io', 'http', 'routes', 'auth', 'redisclient', 'private/settings'], function(snaketron, express, socketio, connectredis, SessionSockets, http, routes, auth, redis, settings) {
    var RedisStore, app, cookieParser, io, server, sessionSockets, sessionStore;

    app = express();
    RedisStore = connectredis(express);
    cookieParser = express.cookieParser(settings.SESSION_SECRET);
    sessionStore = new RedisStore({
      client: redis
    });
    app.configure(function() {
      app.use(cookieParser);
      app.use(express.bodyParser());
      app.use(express.methodOverride());
      app.use(express.session({
        secret: settings.SESSION_SECRET,
        store: sessionStore
      }));
      app.use(auth.passport.initialize());
      app.use(auth.passport.session());
      app.use(app.router);
      return app.use(express["static"](__dirname + '/public'));
    });
    server = http.createServer(app);
    server.listen(8083);
    io = socketio.listen(server);
    sessionSockets = new SessionSockets(io, sessionStore, cookieParser);
    sessionSockets.on('connection', function(err, socket, session) {
      return auth.getUser(session.passport.user, function(err, user) {
        snaketron.socketConnected(socket, user);
        socket.on('api', function(data, callback) {
          var args;

          args = data.args || {};
          args.user = user;
          args.socket = socket;
          return snaketron.apiCall(data.method, args, callback);
        });
        return socket.on('disconnect', function() {
          return snaketron.socketDisconnected(socket, user);
        });
      });
    });
    return routes(app);
  });

}).call(this);
