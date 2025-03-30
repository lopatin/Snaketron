(function() {
  require.config({
    paths: {
      'underscore': 'libs/underscore',
      'backbone': 'libs/backbone',
      'jquery': 'libs/jquery',
      'text': 'libs/text',
      'handlebars': 'libs/hb',
      'hb': 'libs/hbtemplate',
      'socketio': '/socket.io/socket.io.js'
    },
    shim: {
      'underscore': {
        exports: '_'
      },
      'backbone': {
        deps: ['underscore', 'jquery'],
        exports: 'Backbone'
      },
      'socketio': {
        exports: 'io'
      },
      'handlebars': {
        exports: 'Handlebars'
      }
    }
  });

  define(['underscore', 'router', 'views/application', 'app'], function(_, Router, ApplicationView, app) {
    var r;

    r = new Router;
    $(document).delegate('a', 'click', function(e) {
      var href;

      e.preventDefault();
      href = $(e.target).attr('href');
      if (href) {
        r.navigate(href, {
          trigger: true
        });
      }
      return false;
    });
    return $(function() {
      var view;

      view = new ApplicationView({
        el: $("body"),
        router: r
      });
      Backbone.history.start({
        pushState: true
      });
      return app.init(view, r);
    });
  });

}).call(this);
