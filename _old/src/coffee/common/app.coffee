#
# Main client application
#

define [
  'underscore'
  'backbone'
  'api'
  'models/game'
], (_, Backbone, Api, GameModel) ->
  class App
    constructor: () ->
      _.bindAll this

    init: (view, router) ->
      @view = view
      @router = router
      @_startHeartbeat()
      Api.call 'getUser', @_fetchedUser

      Api.listen 'playerCount', (count) ->
        console.log count

      Api.listen 'startGame', @_startGame

      Api.listen 'updatedRoster', @_updateRoster

    # Return the current user
    getUser: () ->
      @user

    # Start a game against a random player ASAP
    requestQuickMatch: () ->
      game = @_createGame()
      Api.call 'requestQuickMatch', ((err, matchId) ->
        game.set 'matchId', matchId
        @router.navigate 'play', { trigger: true }
        ).bind(this)


    # Create the local game model
    _createGame: () ->
      self = this
      game = @game = new GameModel 
        client: @getUser
      # game.on 'change:matchId', () ->
      #   self.startGame()

      return game

    # Match is ready, start the game
    _startGame: (matchId) ->
      if @game and @game.get('matchId') == matchId
        @game.start()

    # User returned from server by api call
    _fetchedUser: (err, user) ->
      if err
        console.log err
      else
        @user = user
        @view.showIndexView()
        console.log "Logged in user: #{user.name}"

    # Emit a regular heartbeat as a hack to keep count of online users
    _startHeartbeat: () ->
      Api.call 'hartbeet'
      setInterval ->
        Api.call 'hartbeet'
      , 1000

  window.stapp = new App()
  return window.stapp
