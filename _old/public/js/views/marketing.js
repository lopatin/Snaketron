(function() {
  define(['jquery', 'underscore', 'backbone', 'views/authentication', 'hb!/templates/marketing.hb'], function($, _, Backbone, AuthenticationView, marketingTemplate) {
    var MarketingView;

    return MarketingView = Backbone.View.extend({
      initialize: function() {
        return this.children = {
          authentication: new AuthenticationView()
        };
      },
      render: function() {
        this.$el.html(marketingTemplate());
        this.children.authentication.setElement(this.$el.find('.authentication'));
        return this.children.authentication.render();
      }
    });
  });

}).call(this);
