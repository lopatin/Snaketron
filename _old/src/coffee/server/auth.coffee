define [
  'passport'
  'passport-local'
  'passport-facebook'
  'private/settings'
  'redisclient'
  'bcrypt'
], (passport, passport_local, passport_facebook, settings, redis, bcrypt) ->
  # Serialization/deserialization for sessions
  passport.serializeUser (user, done) ->
    if user
      done(null, user.id)
    else
      done("Invalid user")
  passport.deserializeUser (id, done) ->
    redis.hgetall "user:"+id, (err, user) ->
      done(err, user)

  # Verify FB user
  passport.use(new passport_facebook.Strategy {
    clientID: settings.FB_APP_ID
    clientSecret: settings.FB_APP_SECRET
    callbackURL: "http://snaketron.io:8080/auth/facebook/redirect/"
    }, (accessToken, refreshToken, profile, done) ->
      if not (profile and profile.id)
        done(null, false)
      else
        getUser profile.id, (err, user) ->
          if err
            return done(err)
          if isValidUser(user)
            done(null, user)
          else
            user = 
              id: profile.id
              name: profile.displayName
              fb_profile: profile._raw
            createUser user, { force: true }, (err, user) ->
              done(err, user)
  )

  # Verify Password login
  passport.use(new passport_local.Strategy (username, password, done) ->
    getUser username, (err, user) ->
      return done err if err
      if not user
        return done null, false, message: 'Incorrect username'
      if !bcrypt.compareSync password, user.password
        return done null, false, message: 'Incorrect password'
      done null, user
  )

  hashPassword = (pw) ->
    salt = bcrypt.genSaltSync(10)
    bcrypt.hashSync pw, salt

  isValidUser = (user) ->
    fields = ['id', 'name']
    if not user
      return false
    for field in fields
      return false unless user[field]?
    return true

  getUser = (id, callback) ->
    redis.hgetall "user:#{id}", (err, user) ->
      callback err, user

  createUser = (user, options, callback) ->
    callback = options if typeof options == "function"
    options = {} if not options
    return callback({ message: "Invalid user parameters" }) if not user
    return callback({ message: "Invalid username" }) if not user.id
    getUser user.id, (err, existingUser) ->
      return callback(err) if err
      return callback({message: "User already exists"}) if existingUser
      user.password = hashPassword(user.password) if user.password
      redis.hmset("user:#{user.id}", user)
      callback(null, user)

  { 
    passport: passport 
    getUser: getUser
    createUser: createUser
  }
