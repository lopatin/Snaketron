(function() {
  define(['jquery', 'underscore', 'backbone', 'hb!/templates/authentication.hb'], function($, _, Backbone, authenticationTemplate) {
    var AuthViewModel, AuthenticationView;

    AuthViewModel = Backbone.Model.extend({});
    return AuthenticationView = Backbone.View.extend({
      initialize: function() {
        _.bindAll(this);
        this.model = new AuthViewModel();
        return this.model.on('change:authType', this.updateManualAuthView);
      },
      events: {
        'click .manual-login a': 'selectAuthType',
        'keyup input': 'setAuthField',
        'submit form': 'submitForm'
      },
      render: function() {
        return this.$el.html(authenticationTemplate(this.model.attributes));
      },
      updateManualAuthView: function(model, authType) {
        this.$el.find('.manual-login a').each(function() {
          if (authType === $(this).data('authtype')) {
            return $(this).addClass('active');
          } else {
            return $(this).removeClass('active');
          }
        });
        this.$el.find('form').each(function() {
          if ($(this).hasClass(authType)) {
            return $(this).show();
          } else {
            return $(this).hide();
          }
        });
        this.$el.find('form input.username').val(this.model.get('username'));
        this.$el.find('form input.password').val(this.model.get('password'));
        this.$el.find('form input.confirm-password').val(this.model.get('passwordConfirmation'));
        this.$el.find('form:visible input:first').focus();
        return this.updateAutocompleteFields();
      },
      updateAutocompleteFields: function() {
        var interval, passwordField, usernameField;

        if (this.autofillCheckCount != null) {
          return;
        } else {
          this.autofillCheckCount = 0;
        }
        usernameField = this.$el.find('form:visible input.username');
        passwordField = this.$el.find('form:visible input.password');
        return interval = setInterval((function() {
          var password, username;

          username = usernameField.val();
          password = passwordField.val();
          if (username != null) {
            this.model.set('username', username);
          }
          if (password != null) {
            this.model.set('password', password);
          }
          this.updateSubmitButtons();
          this.autofillCheckCount++;
          if (this.autofillCheckCount > 200) {
            return clearInterval(interval);
          }
        }).bind(this), 10);
      },
      selectAuthType: function(e) {
        var selectedType;

        selectedType = $(e.target).data('authtype');
        if (this.model.get('authType') === selectedType) {
          selectedType = null;
        }
        return this.model.set('authType', selectedType);
      },
      setAuthField: function(e) {
        var passwordConfirmField, passwordField, usernameField;

        usernameField = this.$el.find('input.username:visible');
        passwordField = this.$el.find('input.password:visible');
        passwordConfirmField = this.$el.find('input.confirm-password:visible');
        if (usernameField.length) {
          this.model.set('username', usernameField.val());
        }
        if (passwordField.length) {
          this.model.set('password', passwordField.val());
        }
        if (passwordConfirmField.length) {
          this.model.set('passwordConfirmation', passwordConfirmField.val());
        }
        return this.updateSubmitButtons();
      },
      updateSubmitButtons: function() {
        if (this.model.get('username')) {
          this.$el.find('form.anonymous button').removeAttr('disabled');
        } else {
          this.$el.find('form.anonymous button').attr('disabled', '');
        }
        if (this.model.get('username') && this.model.get('password')) {
          this.$el.find('form.login button').removeAttr('disabled');
        } else {
          this.$el.find('form.login button').attr('disabled', '');
        }
        if (this.model.get('username') && this.model.get('password') && this.model.get('passwordConfirmation')) {
          return this.$el.find('form.register button').removeAttr('disabled');
        } else {
          return this.$el.find('form.register button').attr('disabled', '');
        }
      },
      submitForm: function(e) {
        var callback, t;

        t = $(e.target);
        if (t.hasClass('login')) {
          callback = this.loginCallback;
        }
        if (t.hasClass('register')) {
          callback = this.registerCallback;
        }
        if (t.hasClass('anonymous')) {
          callback = this.anonymousCallback;
        }
        $.ajax({
          method: "POST",
          url: t.attr('action'),
          data: t.serialize(),
          success: function(data) {
            if (data.user) {
              return window.location.reload();
            } else if (data.error) {
              return console.log(data.error);
            } else {
              return console.log("error");
            }
          }
        });
        e.preventDefault();
        return false;
      },
      loginCallback: function() {
        return window.location.reload();
      }
    });
  });

}).call(this);
