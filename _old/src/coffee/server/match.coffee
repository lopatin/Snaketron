define [
  'underscore'
  'node-uuid'
  'common/models/game'
], (_, uuid, GameModel) ->
	class Match
		constructor: (@capacity) ->
      @id = uuid.v4()
      @players = {}
      @status = 'idle'

    addPlayer: (player) ->
      @players[player.getId()] = player

    isReadyToStart: ->
      _.size(@players) == @capacity and @status == 'idle'

    start: ->
      return if not @isReadyToStart()
      @status = 'started'
      @masterGame = new GameModel()
      self = this
      _.each @players, (player) ->
        player.emit 'start', { matchId: self.getId() }

    getId: ->
      @id

	return Match