define [
  'jquery'
  'underscore'
  'backbone'
  'app'
  'hb!/templates/home.hb'
], ($, _, Backbone, app, homeTemplate) ->
  HomeView = Backbone.View.extend
    initialize: ->

    events: 
      'click .btn.quick-play': 'quickPlayClicked'

    render: ->
      @$el.html homeTemplate({
        name: app.getUser().name
        })

    quickPlayClicked: ->
      app.requestQuickMatch()
