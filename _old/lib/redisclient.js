(function() {
  define(['redis'], function(redis) {
    return redis.createClient();
  });

}).call(this);
