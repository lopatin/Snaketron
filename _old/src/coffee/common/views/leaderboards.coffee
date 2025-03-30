define [
	'jquery'
	'underscore'
	'backbone'
	'hb!/templates/leaderboards.hb'
], ($, _, Backbone, leaderboardsTemplate) ->
	LeaderboardsView = Backbone.View.extend
		initialize: ->

		render: ->
			@$el.html leaderboardsTemplate()