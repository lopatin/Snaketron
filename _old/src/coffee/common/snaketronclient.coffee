# Require.js configuration
require.config
  paths:
    'underscore': 'libs/underscore'
    'backbone': 'libs/backbone'
    'jquery': 'libs/jquery'
    'text': 'libs/text'
    'handlebars' : 'libs/hb'
    'hb' : 'libs/hbtemplate'
    'socketio': '/socket.io/socket.io.js'
  shim:
    'underscore':
      exports: '_'
    'backbone':
      deps: ['underscore', 'jquery']
      exports: 'Backbone'
    'socketio':
      exports: 'io'
    'handlebars':
      exports: 'Handlebars'

define [
  'underscore'
  'router'
  'views/application'
  'app'
], (_, Router, ApplicationView, app) ->
  r = new Router
  $(document).delegate 'a', 'click', (e) ->
    e.preventDefault()
    href = $(e.target).attr 'href'
    r.navigate(href, trigger: true) if href
    return false

  # Run when page loads
  $ ->
    view = new ApplicationView
      el: $("body")
      router: r
    Backbone.history.start pushState: true
    app.init(view, r)
