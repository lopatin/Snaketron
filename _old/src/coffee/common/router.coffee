define [
	'underscore'
	'backbone'
], (_, Backbone) ->
	Router = Backbone.Router.extend
    routes:
      'instructions': 'instructions'
      'leaderboards': 'leaderboards'
      'logout': 'logout'
      'play': 'play'
      '*splat': 'index'
