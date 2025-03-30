(function() {
  define(['jquery', 'underscore', 'backbone'], function($, _, Backbone) {
    var FooterView;

    return FooterView = Backbone.View.extend({
      initialize: function() {},
      render: function() {},
      hide: function() {
        return this.$el.addClass('_hidden');
      },
      show: function() {
        return this.$el.removeClass('_hidden');
      }
    });
  });

}).call(this);
