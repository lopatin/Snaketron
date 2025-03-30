(function() {
  define(['jquery', 'underscore', 'backbone', 'app', 'hb!/templates/application.hb', 'views/header', 'views/footer', 'views/home', 'views/game', 'views/marketing', 'views/leaderboards'], function($, _, Backbone, app, appTemplate, HeaderView, FooterView, HomeView, GameView, MarketingView, LeaderboardsView) {
    var ApplicationView;

    return ApplicationView = Backbone.View.extend({
      initialize: function() {
        this.children = {
          header: new HeaderView(),
          footer: new FooterView(),
          game: new GameView(),
          home: new HomeView(),
          marketing: new MarketingView(),
          leaderboards: new LeaderboardsView()
        };
        this.render();
        return this.routes();
      },
      render: function() {
        this.$el.html(appTemplate());
        this.children.header.setElement(this.$el.find('.header'));
        this.children.header.render();
        this.children.footer.setElement(this.$el.find('.footer'));
        this.children.footer.render();
        return this.children.game.setElement(this.$el.find('.game'));
      },
      routes: function() {
        var router;

        _.bindAll(this);
        router = this.options.router;
        router.on('route:index', this.index);
        router.on('route:play', this.play);
        router.on('route:leaderboards', this.leaderboards);
        router.on('route:instructions', this.instructions);
        return router.on('route:logout', this.logout);
      },
      play: function() {
        if (app.game) {
          this.children.game.render(app.game);
          this.children.game.showWaiting();
          return this.showGameView();
        } else {
          return this.options.router.navigate('/', {
            trigger: true
          });
        }
      },
      showGameView: function() {
        this.children.header.hide();
        this.children.footer.hide();
        this.$el.find('.content').addClass('_hidden');
        return this.children.game.show();
      },
      hideGameView: function() {
        this.children.game.hide();
        return setTimeout((function() {
          this.children.header.show();
          this.children.footer.show();
          return this.$el.find('.content').removeClass('_hidden');
        }).bind(this), 500);
      },
      index: function() {
        this.children.header.select('home');
        return this.showIndexView();
      },
      showIndexView: function() {
        if (app.getUser()) {
          return this.showContentView(this.children.home);
        } else {
          return this.showContentView(this.children.marketing);
        }
      },
      leaderboards: function() {
        this.children.header.select('leaderboards');
        return this.showContentView(this.children.leaderboards);
      },
      instructions: function() {
        return this.children.header.select('instructions');
      },
      showContentView: function(view) {
        view.setElement(this.$el.find('.content'));
        view.render();
        return this.hideGameView();
      },
      logout: function() {
        return window.location.reload();
      }
    });
  });

}).call(this);
