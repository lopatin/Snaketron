#
# Keep track of online users
#

define [
  'underscore'
  'redisclient'
  'player'
], (_, redis, Player) ->
  class PlayerManager
    constructor: () ->
      @players = {}
      @count = 0
      redis.del 'connected_players'
      setInterval @_updateCount.bind(this), 1000

    # Add a player to Snaketron
    add: (player) ->
      @players[player.getId()] = player

    # Remove a player from Snaketron
    remove: (player) ->
      return if not player?
      redis.srem 'connected_players', player.socket.id
      delete @players[player.getId()]

    # Return a player of the requested id
    get: (id) ->
      @players[id]

    # Emit a message to all players
    emit: (message, data) ->
      _.each @players, (player) ->
        player.emit message, data

    # A user disconnected from the app
    userConnected: (user, socket) ->
      player = new Player user, socket
      @add player

    # A user connected to the app
    userDisconnected: (user, socket) ->
      @remove @players[user.id]

    # Keep players alive
    heartbeat: (user) ->
      player = @players[user.id]
      return if not player?
      if player.hb_timeout then clearTimeout player.hb_timeout
      else redis.sadd 'connected_players', player.socket.id
      player.hb_timeout = setTimeout (() -> 
          @remove.bind(this, player)
        ).bind(this), 4100

    # Return the number of online players
    getCount: () ->
      @count

    _updateCount: () ->
      self = this
      redis.scard 'connected_players', (err, val) ->
        self.count = val