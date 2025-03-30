#
# Api to communicate with server side
#

define [
  'underscore'
  'socketio'
], (_, io) ->
  Api = {}

  # Connect the websocket
  l = window.location
  socket = io.connect l.protocol+l.host

  # Make an API call
  Api.call = (method, args, callback) ->
    return if not method?
    if not args?
      args = {}
    options = { method: method, args: args }
    if typeof args == "function"
      callback = args 
      delete options.args
    socket.emit 'api', options, callback

  # Respond to a message from the server
  Api.listen = (message, action) ->
    return if not (message? and action?)
    socket.on message, action

  return Api
