requirejs = require 'requirejs'

# Require.js configuration
requirejs.config
  baseUrl: __dirname+"/lib",
  paths:
    common: __dirname+"/public/js"
  nodeRequire: require

# Main server code
requirejs [
  'snaketron'
  'express'
  'socket.io'
  'connect-redis'
  'session.socket.io'
  'http'
  'routes'
  'auth'
  'redisclient'
  'private/settings'
  ], (snaketron, express, socketio, connectredis, SessionSockets, http, routes, auth, redis, settings) ->
    # Express web server
    app = express()

    # Configure express web server
    RedisStore = connectredis(express)
    cookieParser = express.cookieParser settings.SESSION_SECRET
    sessionStore = new RedisStore {client: redis}
    app.configure () ->
      app.use cookieParser
      app.use express.bodyParser()
      app.use express.methodOverride()
      app.use express.session
        secret: settings.SESSION_SECRET
        store: sessionStore
      app.use auth.passport.initialize()
      app.use auth.passport.session()
      app.use app.router
      app.use express.static __dirname+'/public'

    server = http.createServer app
    server.listen 8083

    # Socket.io websocket server
    io = socketio.listen server
    sessionSockets = new SessionSockets io, sessionStore, cookieParser
    sessionSockets.on 'connection', (err, socket, session) ->
      auth.getUser session.passport.user, (err, user) ->
        snaketron.socketConnected socket, user
        socket.on 'api', (data, callback) ->
          args = data.args or {}
          args.user = user
          args.socket = socket
          snaketron.apiCall data.method, args, callback
        socket.on 'disconnect', () ->
          snaketron.socketDisconnected socket, user

    # Setup routes
    routes(app)
