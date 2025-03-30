define [
  'auth'
], (auth) ->
  routes = (app) ->
    # Facebook Auth
    app.get '/auth/facebook/', auth.passport.authenticate 'facebook'
    app.get '/auth/facebook/redirect/', auth.passport.authenticate('facebook', {
      successRedirect: '/'
      failureRedirect: '/'
      })

    # Local Auth
    app.post '/login', (req, res, next) ->
      auth.passport.authenticate('local', (err, user, info) ->
        if not err and user
          req.login user, (err) ->
            res.json { error: err, user: user }
        else
          res.json { error: info }
      )(req, res, next)

    app.post '/register', (req, res) ->
      username = req.body.username
      password = req.body.password
      passwordConfirmation = req.body.passwordConfirmation
      err = { message: "Password confirmation does not match" } if password != passwordConfirmation
      err = { message: "Invalid password" } if not password
      err = { message: "Invalid username" } if not username
      if err
        res.json { error: err }
      else
        auth.createUser {
          id: username 
          name: username 
          password: password
        }, (err, user) ->
          if not err and user
            req.login user, (err) ->
              res.json { error: err, user: user }
          else
            res.json { error: err, user: user }

    # Logout
    app.get '/logout', (req, res) ->
      req.logout()
      res.redirect '/'


