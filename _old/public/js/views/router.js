(function() {
  define(['underscore', 'backbone'], function(_, Backbone) {
    var Router;

    return Router = Backbone.Router.extend({
      routes: {
        '*splat': 'index'
      }
    });
  });

}).call(this);
