define [
  'jquery'
  'underscore'
  'backbone'
], ($, _, Backbone) ->
  FooterView = Backbone.View.extend
    initialize: ->

    render: ->

    hide: ->
      @$el.addClass '_hidden'

    show: ->
      @$el.removeClass '_hidden'
