define [
	'underscore'
	'redisclient'
], (_, redis) ->
  class Player
    constructor: (@user, @socket) -> 


    # Emit a message to the player
    emit: (message, data, callback) ->
      if typeof data == "undefined"
        data = callback = undefined
      if typeof data == "function"
        callback = data
        data = {}
      if @socket
        @socket.emit message, data, callback

    getId: () ->
      @user.id
