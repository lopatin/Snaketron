define [
	'jquery'
	'underscore'
	'backbone'
	'views/authentication'
	'hb!/templates/marketing.hb'
], ($, _, Backbone, AuthenticationView, marketingTemplate) ->
	MarketingView = Backbone.View.extend
		initialize: ->
			@children = 
				authentication: new AuthenticationView()
				
		render: ->
			@$el.html marketingTemplate()
			@children.authentication.setElement @$el.find('.authentication')
			@children.authentication.render()
