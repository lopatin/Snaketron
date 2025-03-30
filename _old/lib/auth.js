(function() {
  define(['passport', 'passport-local', 'passport-facebook', 'private/settings', 'redisclient', 'bcrypt'], function(passport, passport_local, passport_facebook, settings, redis, bcrypt) {
    var createUser, getUser, hashPassword, isValidUser;

    passport.serializeUser(function(user, done) {
      if (user) {
        return done(null, user.id);
      } else {
        return done("Invalid user");
      }
    });
    passport.deserializeUser(function(id, done) {
      return redis.hgetall("user:" + id, function(err, user) {
        return done(err, user);
      });
    });
    passport.use(new passport_facebook.Strategy({
      clientID: settings.FB_APP_ID,
      clientSecret: settings.FB_APP_SECRET,
      callbackURL: "http://snaketron.io:8080/auth/facebook/redirect/"
    }, function(accessToken, refreshToken, profile, done) {
      if (!(profile && profile.id)) {
        return done(null, false);
      } else {
        return getUser(profile.id, function(err, user) {
          if (err) {
            return done(err);
          }
          if (isValidUser(user)) {
            return done(null, user);
          } else {
            user = {
              id: profile.id,
              name: profile.displayName,
              fb_profile: profile._raw
            };
            return createUser(user, {
              force: true
            }, function(err, user) {
              return done(err, user);
            });
          }
        });
      }
    }));
    passport.use(new passport_local.Strategy(function(username, password, done) {
      return getUser(username, function(err, user) {
        if (err) {
          return done(err);
        }
        if (!user) {
          return done(null, false, {
            message: 'Incorrect username'
          });
        }
        if (!bcrypt.compareSync(password, user.password)) {
          return done(null, false, {
            message: 'Incorrect password'
          });
        }
        return done(null, user);
      });
    }));
    hashPassword = function(pw) {
      var salt;

      salt = bcrypt.genSaltSync(10);
      return bcrypt.hashSync(pw, salt);
    };
    isValidUser = function(user) {
      var field, fields, _i, _len;

      fields = ['id', 'name'];
      if (!user) {
        return false;
      }
      for (_i = 0, _len = fields.length; _i < _len; _i++) {
        field = fields[_i];
        if (user[field] == null) {
          return false;
        }
      }
      return true;
    };
    getUser = function(id, callback) {
      return redis.hgetall("user:" + id, function(err, user) {
        return callback(err, user);
      });
    };
    createUser = function(user, options, callback) {
      if (typeof options === "function") {
        callback = options;
      }
      if (!options) {
        options = {};
      }
      if (!user) {
        return callback({
          message: "Invalid user parameters"
        });
      }
      if (!user.id) {
        return callback({
          message: "Invalid username"
        });
      }
      return getUser(user.id, function(err, existingUser) {
        if (err) {
          return callback(err);
        }
        if (existingUser) {
          return callback({
            message: "User already exists"
          });
        }
        if (user.password) {
          user.password = hashPassword(user.password);
        }
        redis.hmset("user:" + user.id, user);
        return callback(null, user);
      });
    };
    return {
      passport: passport,
      getUser: getUser,
      createUser: createUser
    };
  });

}).call(this);
