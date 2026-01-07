<div class='panel panel-default'>
  <div class='panel-heading'>
    <h3 class='panel-title lead'>Enter Snaketron</h3>
  </div>
  <div class='panel-body'>
    <a class='fb-button' href='/auth/facebook/'>
      <img src='/images/fbButton.png'>
    </a>
    <div class='hr'>
      <div class='label'>OR</div>
    </div>
    <div class='list-group manual-login'>
      <a class='list-group-item small' data-authtype='login'>
        <span class='glyphicon glyphicon-ok'></span>
        Login
      </a>
      <a class='list-group-item small' data-authtype='register'>
        <span class='glyphicon glyphicon-ok'></span>
        Register
      </a>
      <a class='list-group-item small' data-authtype='anonymous'>
        <span class='glyphicon glyphicon-ok'></span>
        Play anonymously
      </a>
    </div>
    <form action='/login' class='login' method='POST' role='form'>
      <input class='form-control username' name='username' placeholder='Username' type='text'>
      <input class='form-control password' name='password' placeholder='Password' type='password'>
      <button class='btn btn-primary btn-block' disabled='' type='submit'>Login</button>
    </form>
    <form action='/register' class='register' method='POST' role='form'>
      <input class='form-control username' name='username' placeholder='Username' type='text'>
      <input class='form-control password' name='password' placeholder='Password' type='password'>
      <input class='form-control confirm-password' name='passwordConfirmation' placeholder='Confirm password' type='password'>
      <button class='btn btn-primary btn-block' disabled='' type='submit'>Register</button>
    </form>
    <form class='anonymous' role='form'>
      <input class='form-control username' name='name' placeholder='Pick a name' type='text'>
      <button class='btn btn-primary btn-block' disabled='' type='submit'>Start playing</button>
    </form>
  </div>
</div>
