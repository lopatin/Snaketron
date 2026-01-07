(function() {
  define(['jquery', 'underscore', 'backbone', 'hb!/templates/leaderboards.hb'], function($, _, Backbone, leaderboardsTemplate) {
    var LeaderboardsView;

    return LeaderboardsView = Backbone.View.extend({
      initialize: function() {},
      render: function() {
        return this.$el.html(leaderboardsTemplate());
      }
    });
  });

}).call(this);
