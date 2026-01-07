define [
  'jquery'
  'underscore'
  'backbone'
  'hb!/templates/header.hb'
], ($, _, Backbone, headerTemplate) ->
  HeaderView = Backbone.View.extend({
    initialize: ->

    render: ->
      @$el.html headerTemplate

    select: (name) ->
      @$el.find('.navigation li a').removeClass 'selected'
      @$el.find('.navigation li a.'+name).addClass 'selected' 

    hide: ->
      @$el.addClass '_hidden'

    show: ->
      @$el.removeClass '_hidden'

  })

