#
# Game class that controls a game's logic. Used on client and server.
#

define [
  'underscore'
  'backbone'
], (_, Backbone) ->
  GameModel = Backbone.Model.extend
    initialize: () ->
