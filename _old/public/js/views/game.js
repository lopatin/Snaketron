(function() {
  define(['jquery', 'underscore', 'backbone', 'hb!/templates/game.hb', 'hb!/templates/scoreboard.hb'], function($, _, Backbone, gameTemplate, scoreboardTemplate) {
    var GameView;

    return GameView = Backbone.View.extend({
      render: function(game) {
        this.game = game;
        this.$el.html(gameTemplate());
        this.$wrapper = this.$el.find('.game-wrapper');
        this.$waiting = this.$el.find('.waiting');
        this.$stage = this.$el.find('.stage');
        this.$results = this.$el.find('.results');
        this.$scoreboard = this.$el.find('.scoreboard-wrapper');
        this.renderScoreboard();
        return this.blinkIndicator();
      },
      renderScoreboard: function() {
        if (this.$scoreboard) {
          return this.$scoreboard.html(scoreboardTemplate());
        }
      },
      showWaiting: function() {
        return this.showElement(this.$waiting);
      },
      showStage: function() {
        return this.showElement(this.$stage);
      },
      showResults: function() {
        return this.showElement(this.$results);
      },
      showElement: function(element) {
        if (this.selectedElement === element) {
          return;
        }
        _.each([this.$waiting, this.$stage, this.$results], function(el) {
          return el.hide();
        });
        element.show();
        return this.selectedElement = element;
      },
      show: function() {
        var self;

        this.$el.show();
        self = this;
        setTimeout(function() {
          return self.$wrapper.removeClass('_hidden');
        }, 500);
        return setTimeout(function() {
          return self.$scoreboard.removeClass('_hidden');
        }, 700);
      },
      hide: function() {
        if (this.$wrapper) {
          this.$wrapper.addClass('_hidden');
        }
        if (this.$scoreboard) {
          this.$scoreboard.addClass('_hidden');
        }
        return setTimeout((function() {
          return this.$el.hide();
        }).bind(this), 500);
      },
      blinkIndicator: function() {
        var self;

        self = this;
        if (self.blinkTimer) {
          clearInterval(self.blinkTimer);
          delete self.blinkTimer;
        }
        return self.blinkTimer = setInterval(function() {
          if (self.$waiting) {
            self.$waiting.removeClass('slow');
          }
          if (self.$waiting) {
            self.$waiting.addClass('_hidden');
          }
          return setTimeout(function() {
            if (self.$waiting) {
              self.$waiting.addClass('slow');
            }
            if (self.$waiting) {
              return self.$waiting.removeClass('_hidden');
            }
          }, 200);
        }, 1500);
      }
    });
  });

}).call(this);
