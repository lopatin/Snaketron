define [
  'jquery'
  'underscore'
  'backbone'
  'hb!/templates/authentication.hb'
], ($, _, Backbone, authenticationTemplate) ->
  AuthViewModel = Backbone.Model.extend({})
  AuthenticationView = Backbone.View.extend({
    initialize: ->
      _.bindAll this
      @model = new AuthViewModel()
      @model.on 'change:authType', @updateManualAuthView

    events:
      'click .manual-login a': 'selectAuthType'
      'keyup input': 'setAuthField'
      'submit form': 'submitForm'

    render: ->
      @$el.html authenticationTemplate(@model.attributes)

    updateManualAuthView: (model, authType) ->
      @$el.find('.manual-login a').each () ->
        if authType == $(this).data 'authtype'
          $(this).addClass 'active'
        else
          $(this).removeClass 'active'
      @$el.find('form').each () ->
        if $(this).hasClass authType
          $(this).show()
        else
          $(this).hide()
      @$el.find('form input.username').val(@model.get 'username')
      @$el.find('form input.password').val(@model.get 'password')
      @$el.find('form input.confirm-password').val(@model.get 'passwordConfirmation')
      @$el.find('form:visible input:first').focus()
      @updateAutocompleteFields()

    updateAutocompleteFields: () ->
      if @autofillCheckCount?
        return
      else @autofillCheckCount = 0
      usernameField = @$el.find('form:visible input.username')
      passwordField = @$el.find('form:visible input.password')
      interval = setInterval (() ->
        username = usernameField.val()
        password = passwordField.val()
        @model.set 'username', username if username?
        @model.set 'password', password if password? 
        @updateSubmitButtons()
        @autofillCheckCount++
        clearInterval(interval) if @autofillCheckCount > 200
      ).bind(this), 10 

    selectAuthType: (e) ->
      selectedType = $(e.target).data 'authtype'
      if @model.get('authType') == selectedType
        selectedType = null
      @model.set('authType', selectedType)

    setAuthField: (e) ->
      usernameField = @$el.find('input.username:visible')
      passwordField = @$el.find('input.password:visible')
      passwordConfirmField = @$el.find('input.confirm-password:visible')
      if usernameField.length
        @model.set 'username', usernameField.val()
      if passwordField.length
        @model.set 'password', passwordField.val()
      if passwordConfirmField.length
        @model.set 'passwordConfirmation', passwordConfirmField.val()
      @updateSubmitButtons()

    updateSubmitButtons: () ->
      if @model.get 'username'
        @$el.find('form.anonymous button').removeAttr 'disabled'
      else
        @$el.find('form.anonymous button').attr 'disabled', ''

      if @model.get('username') and @model.get('password')
        @$el.find('form.login button').removeAttr 'disabled'
      else
        @$el.find('form.login button').attr 'disabled', ''

      if @model.get('username') and @model.get('password') and @model.get('passwordConfirmation')
        @$el.find('form.register button').removeAttr 'disabled'
      else
        @$el.find('form.register button').attr 'disabled', ''

    submitForm: (e) ->
      t = $(e.target)
      callback = @loginCallback if t.hasClass 'login'
      callback = @registerCallback if t.hasClass 'register'
      callback = @anonymousCallback if t.hasClass 'anonymous'
      $.ajax {
        method: "POST"
        url: t.attr 'action'
        data: t.serialize()
        success: (data) ->
          if data.user
            window.location.reload()
          else if data.error
            console.log data.error
          else
            console.log "error"

      }
      e.preventDefault()
      false

    loginCallback: () ->
      window.location.reload()

    })