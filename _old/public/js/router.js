(function() {
  define(['underscore', 'backbone'], function(_, Backbone) {
    var Router;

    return Router = Backbone.Router.extend({
      routes: {
        'instructions': 'instructions',
        'leaderboards': 'leaderboards',
        'logout': 'logout',
        'play': 'play',
        '*splat': 'index'
      }
    });
  });

}).call(this);
