(function() {
  define(['auth'], function(auth) {
    var routes;

    return routes = function(app) {
      app.get('/auth/facebook/', auth.passport.authenticate('facebook'));
      app.get('/auth/facebook/redirect/', auth.passport.authenticate('facebook', {
        successRedirect: '/',
        failureRedirect: '/'
      }));
      app.post('/login', function(req, res, next) {
        return auth.passport.authenticate('local', function(err, user, info) {
          if (!err && user) {
            return req.login(user, function(err) {
              return res.json({
                error: err,
                user: user
              });
            });
          } else {
            return res.json({
              error: info
            });
          }
        })(req, res, next);
      });
      app.post('/register', function(req, res) {
        var err, password, passwordConfirmation, username;

        username = req.body.username;
        password = req.body.password;
        passwordConfirmation = req.body.passwordConfirmation;
        if (password !== passwordConfirmation) {
          err = {
            message: "Password confirmation does not match"
          };
        }
        if (!password) {
          err = {
            message: "Invalid password"
          };
        }
        if (!username) {
          err = {
            message: "Invalid username"
          };
        }
        if (err) {
          return res.json({
            error: err
          });
        } else {
          return auth.createUser({
            id: username,
            name: username,
            password: password
          }, function(err, user) {
            if (!err && user) {
              return req.login(user, function(err) {
                return res.json({
                  error: err,
                  user: user
                });
              });
            } else {
              return res.json({
                error: err,
                user: user
              });
            }
          });
        }
      });
      return app.get('/logout', function(req, res) {
        req.logout();
        return res.redirect('/');
      });
    };
  });

}).call(this);
