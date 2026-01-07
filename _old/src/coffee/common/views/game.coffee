#
# Game view
#

define [
  'jquery'
  'underscore'
  'backbone'
  'hb!/templates/game.hb'
  'hb!/templates/scoreboard.hb'
], ($, _, Backbone, gameTemplate, scoreboardTemplate) ->
  GameView = Backbone.View.extend
    render: (game) ->
      @game = game
      @$el.html gameTemplate()
      @$wrapper = @$el.find '.game-wrapper'
      @$waiting = @$el.find '.waiting'
      @$stage = @$el.find '.stage'
      @$results = @$el.find '.results'
      @$scoreboard = @$el.find '.scoreboard-wrapper'
      @renderScoreboard()
      @blinkIndicator()

    renderScoreboard: ->
      @$scoreboard.html scoreboardTemplate() if @$scoreboard

    showWaiting: ->
      @showElement @$waiting

    showStage: ->
      @showElement @$stage

    showResults: ->
      @showElement @$results

    showElement: (element) ->
      return if @selectedElement == element
      _.each [@$waiting, @$stage, @$results], (el) ->
        el.hide()
      element.show()
      @selectedElement = element

    show: ->
      @$el.show()
      self = this
      setTimeout ->
        self.$wrapper.removeClass '_hidden'
      , 500
      setTimeout ->
        self.$scoreboard.removeClass '_hidden'
      , 700

    hide: ->
      @$wrapper.addClass '_hidden' if @$wrapper
      @$scoreboard.addClass '_hidden' if @$scoreboard
      setTimeout (() ->
        @$el.hide()
      ).bind(this), 500

    blinkIndicator: ->
      self = this
      if self.blinkTimer
        clearInterval(self.blinkTimer) 
        delete self.blinkTimer
      self.blinkTimer = setInterval ->
        self.$waiting.removeClass 'slow' if self.$waiting
        self.$waiting.addClass '_hidden' if self.$waiting
        setTimeout ->
          self.$waiting.addClass 'slow' if self.$waiting
          self.$waiting.removeClass '_hidden' if self.$waiting
        , 200
      , 1500