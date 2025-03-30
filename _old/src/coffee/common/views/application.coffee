define [
  'jquery'
  'underscore'
  'backbone'
  'app'
  'hb!/templates/application.hb'
  'views/header'
  'views/footer'
  'views/home'
  'views/game'
  'views/marketing'
  'views/leaderboards'
], ($, _, Backbone, app, appTemplate, HeaderView, FooterView, HomeView, GameView, MarketingView, LeaderboardsView) ->
  ApplicationView = Backbone.View.extend
    initialize: ->
      @children =
        header: new HeaderView()
        footer: new FooterView()
        game: new GameView()
        home: new HomeView()
        marketing: new MarketingView()
        leaderboards: new LeaderboardsView()
      @render()
      @routes()

    render: ->
      @$el.html appTemplate()
      @children.header.setElement @$el.find('.header')
      @children.header.render()
      @children.footer.setElement @$el.find('.footer')
      @children.footer.render()
      @children.game.setElement @$el.find('.game')

    routes: ->
      _.bindAll @
      router = @options.router
      router.on 'route:index', @index
      router.on 'route:play', @play
      router.on 'route:leaderboards', @leaderboards
      router.on 'route:instructions', @instructions
      router.on 'route:logout', @logout

    play: ->
      if app.game
        @children.game.render(app.game)
        @children.game.showWaiting()
        @showGameView()
      else
        @options.router.navigate '/', { trigger: true }

    showGameView: ->
      @children.header.hide()
      @children.footer.hide()
      @$el.find('.content').addClass '_hidden'
      @children.game.show()

    hideGameView: ->
      @children.game.hide()
      setTimeout ( () ->
        @children.header.show()
        @children.footer.show()
        @$el.find('.content').removeClass '_hidden'
      ).bind(this), 500

    index: ->
      @children.header.select 'home'
      @showIndexView()

    showIndexView: ->
      if app.getUser()
        @showContentView @children.home
      else
        @showContentView @children.marketing

    leaderboards: ->
      @children.header.select 'leaderboards'
      @showContentView @children.leaderboards

    instructions: ->
      @children.header.select 'instructions'

    showContentView: (view) ->
      view.setElement @$el.find('.content')
      view.render()
      @hideGameView()
      # this is unused
      # @contentView = view

    logout: ->
      window.location.reload()

