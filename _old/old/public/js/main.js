(function() {
window.addEvent('domready', function(){
	var game = new Snaketron({
		containerId: "snaketron",
		size: {width: 80, height: 40},
		speed: 100
	});

	var socket = io.connect("http://snaketron.com");
	game.socket = socket;
	io.sockets = {}; // remove global reference to socket

	socket.on('start-game', function(data){
		game.snacks = [];
		game.gameId = data.gameId;
		game.partnerId = data.partnerId;
		game.partnerUsername = data.partnerUsername;
		game.startGame(data.position);
	});

	socket.on('waiting', function(){
		game.setState('waiting');
	});

	socket.on('menu', function(data){
		if(data){
			if(data.username) game.username = data.username;
			if(data.score != undefined) game.setScore(data.score);
			if(data.wins != undefined) game.setWins(data.wins);
			if(data.losses != undefined) game.setLosses(data.losses);
		}
		if(data && data.score)
			game.setScore(data.score);
		if(data)
			game.setState('menu');
	});

	socket.on('snack', function(snack){
		game.addSnack(snack);
	});

	socket.on('remove-snack', function(data){
		game.removeSnack(data.id);
		if(data.pid === game.partnerId)
			game.mainSnake.addScore(data.weight);
		else game.partnerSnake.addScore(data.weight);
		game.refreshScore();
	});

	socket.on('add-direction', function(data){
		game.addDirection(data.d, data.p);
		game.sendPoints();
	});

	socket.on('send-points', function(data){
		game.partnerSnake.points = data;
		game.draw();
	});

	socket.on('i', function(){
		game.iterate();
	});

	socket.on('gameover', function(data){
		game.winner = data.winner;
		var message = data.message || "";
		game.setState('gameover', {message: message});

		game.activeGame = false;
		if(game.iterateTimer)
			clearInterval(game.iterateTimer);
	});

	socket.on('draw', function(){
		game.setState('gameover', {draw: true});
		if(game.iterateTimer)
			clearInterval(game.iterateTimer);
	});

	$(document.body).addEvent('keydown', function(e){
		if(e.key === 'space')
			e.stop();
		if(e.key === 'space' && game.state === 'menu')
			game.socket.emit('random-opponent', game.username);
	});
	//game.addSnack({x: 30, y: 40, weight: 40});


	socket.on('add-loss', function(){
		game.setLosses(game.losses+1);
	});

	socket.on('add-win', function(){
		game.setWins(game.wins+1);
	});

	socket.on('player-count', function(count){
		game.onlineCount = count;
		game.refreshNav();
	});

	socket.on('reset-score', function(){
		game.setScore(0);
	});	

	socket.on('leaderboards', function(data){
		game.leaderboards = data;
		game.refreshLeaderboards(data);
	});

	socket.on('set-speed', function(speed){
		game.setSpeed = speed;
	});

	socket.on('login-error', function(err){
		game.setLoginError(err.error);
	});

	socket.on('register-error', function(err){
		game.setLoginError(err.error);
	});
});

var Snaketron = new Class({
	initialize: function(options){
		this.options = options;

		// Initialize container/overlay
		this.container = $(options.containerId);
		this.overlay = new Element('div', {id: 'overlay'});
		this.overlay.inject(this.container);

		this.canvas = document.getElementById("gameCanvas");
		this.ctx = this.canvas.getContext("2d");

		// Create snakes
		this.mainSnake = new Snake({
			color: "#333333",
			points: [],
			direction: "left"
		});

		this.partnerSnake = new Snake({
			color: "#999999",
			points: [],
			direction: "right"
		});

		this.setSpeed = 0;

		// Create snacks
		this.snacks = [];

		// Attach key events for movement
		var closure = function (that){
			window.addEvent('keydown', function(e){
				var lastDir = that.mainSnake.directions.getLast();
				if(e.key !== lastDir)
					if((e.key === 'left' && that.mainSnake.directions.getLast() !== 'right')
						|| (e.key === 'right' && that.mainSnake.directions.getLast() !== 'left')
						|| (e.key === 'up' && that.mainSnake.directions.getLast() !== 'down')
						|| (e.key === 'down' && that.mainSnake.directions.getLast() !== 'up')){
						e.stop();
						that.newDirection(e.key);
					}
			});
		}(this);

		this.activeGame = false;

		this.setState('login');
		this.draw();

		// Start game
		//this.iterate();
	},
	iterate: function(){
		this.mainSnake.step();
		this.checkDeath();
		this.partnerSnake.step();
		this.checkDeath();
		this.checkSwallows();
		
		this.draw();

		if(this.iterateTimer && this.setSpeed){
			clearInterval(this.iterateTimer);
			this.iterateTimer = this.iterate.periodical(this.setSpeed, this);
			this.setSpeed = 0;
		}
	},
	draw: function(clear){
		// Draw board
		this.ctx.fillStyle = "#f6f6f6";
		for(var x = 0; x < 80; x++){
			for(var y = 0; y < 40; y++){
				this.ctx.fillRect(1+x*11, 1+y*11, 10, 10);
			}
		}

		if(clear) return;

		// Draw snakes
		var that = this;
		this.snakesArray().each(function(snake, index){
			snake.points.each(function(point, index){
				that.ctx.fillStyle = snake.options.color;
				that.ctx.fillRect(1+point.x*11, 1+point.y*11, 10, 10);
			});
		});

		// Draw snacks
		this.snacks.each(function(snack){
			that.ctx.fillStyle = "green";
			that.ctx.fillRect(1+snack.x*11, 1+snack.y*11, 10, 10);
		});
	},
	clearTails: function(){
		var that = this;
		this.snakesArray().each(function(snake){
			var popped = snake.popped;
			if(popped){
				that.colorCell(popped.x, popped.y, "#f6f6f6");
				snake.popped = null;
			}
		});
	},
	clearTable: function(){
		//this.table.getElements('td').setStyle('background-color', '#f6f6f6');
	},
	colorCell: function(x, y, color){
		var cell = this.table.getElements("tr:nth-child("+y+") td:nth-child("+x+")")[0];
		if(cell)
			cell.setStyle('background-color', color);
	},
	addSnack: function(snack){
		var newSnack = new Snack(snack);
		this.snacks.push(newSnack);
	},
	checkSwallows: function(){
		var that = this;
		this.snacks.each(function(snack){
			if(snack.x === that.mainSnake.points[0].x && snack.y === that.mainSnake.points[0].y){
				that.snacks.erase(snack);
				that.socket.emit('eaten-snack', {
					gameId: that.gameId,
					partnerId: that.partnerId,
					snackId: snack.id,
					snackWeight: snack.weight
				});
			}
		});
	},
	removeSnack: function(id){
		var that = this;
		this.snacks.each(function(snack){
			if(id === snack.id)
				that.snacks.erase(snack);
		});
	},
	setState: function(state, options){
		var that = this;

		this.clearTable();
		if(state === 'waiting'){
			this.state = 'waiting';
			this.overlay.set('html', '<h3 style="margin-top: 100px;">Waiting for players</h3><br /><img style="margin-top: 0px;" src="/images/loader.gif" alt="Loading" />');
			this.draw(true);
		}
		else if(state === 'login'){
			this.overlay.set('html', loginTemplate({}));
			this.state = 'login';
			// Set login form events
			this.overlay.getElements('.registerPassword').addEvent('keydown', function(e){
				if(e.key === 'enter')
					that.socket.emit('register', {
						username: that.overlay.getElements('.registerUsername').get('value'),
						password: that.overlay.getElements('.registerPassword').get('value')
					});
			});
			this.overlay.getElements('.loginPassword').addEvent('keydown', function(e){
				if(e.key === 'enter')
					that.socket.emit('login', {
						username: that.overlay.getElements('.loginUsername').get('value'),
						password: that.overlay.getElements('.loginPassword').get('value')
					});
			});
			$("playAnon").addEvent('click', function(){
				that.socket.emit('login', {
					anon: true
				});
			});

			this.overlay.getElements('.registerPassword').addEvent('focus', function(e){
				that.overlay.getElements('.registerHelper').show();
			});
			this.overlay.getElements('.registerPassword').addEvent('blur', function(e){
				that.overlay.getElements('.registerHelper').hide();
			});
			this.overlay.getElements('.loginPassword').addEvent('focus', function(e){
				that.overlay.getElements('.loginHelper').show();
			});
			this.overlay.getElements('.loginPassword').addEvent('blur', function(e){
				that.overlay.getElements('.loginHelper').hide();
			});

			this.draw(true);
			this.refreshScore(true);
		}
		else if(state === 'menu'){
			var that = this;
			this.state = 'menu';
			this.overlay.set('html', '<div style="text-align: center; width: 83%; margin: 0 auto 46px auto;">\
			<h3>Welcome, '+this.username+'</h3>\
			<br /><br />\
			<span class="btn btn-success btn-large randomOpponent">Play Random Opponent<br><span style="font-size: 11px;">(space)</span></span>\
			<!--<span style="margin-left: 15px;"> or play against <input style="margin-top: 5px;" type="text" placeholder="Type Username" /></span>-->\
			</div>');

			this.overlay.getElements('.randomOpponent').addEvent('click', function(){
				that.socket.emit('random-opponent', that.username);
			});
			this.refreshScore();
			this.draw(true);
			this.refreshNav();
		}
		else if(state === 'gameover'){
			var that = this;
			this.state = 'gameover';

			if(options && options.draw)
				this.overlay.set('html', gameOverTemplate({
					winner: "It's a draw!",
					message: options.message
				}));
			else
				this.overlay.set('html', gameOverTemplate({
					winner: that.username === that.winner ? "You won!" : that.winner+" won :(",
					message: options.message
				}));

			$("continueButton").addEvent('click', function(){
				that.setState('menu');
			})

			this.refreshScore();
		}
		else if(state === 'countdown'){
			var that = this;
			this.overlay.set('html', '<h3>The game is starting in ...</h3><h1>'+options.seconds+'</h1>')
		}

		if(this.leaderboards)
			this.refreshLeaderboards(this.leaderboards);
	},
	startGame: function(position){
		this.activeGame = true;
		this.overlay.set('html', '');
		if(position === 'left'){
			this.mainSnake.points = [{x:25, y:20},{x:24, y:20},{x:23, y:20},{x:22, y:20},{x:21, y:20},{x:20, y:20}];
			this.mainSnake.directions = ["right"];
			this.partnerSnake.points = [{x:60, y:20},{x:61, y:20},{x:62, y:20},{x:63, y:20},{x:64, y:20},{x:65, y:20}];
			this.partnerSnake.directions = ["left"];
		}
		else if(position === 'right'){
			this.mainSnake.points = [{x:60, y:20},{x:61, y:20},{x:62, y:20},{x:63, y:20},{x:64, y:20},{x:65, y:20}];
			this.mainSnake.directions = ["left"];
			this.partnerSnake.points = [{x:25, y:20},{x:24, y:20},{x:23, y:20},{x:22, y:20},{x:21, y:20},{x:20, y:20}];
			this.partnerSnake.directions = ["right"];
		}
		this.refreshScore();
		this.draw();

		this.startCountdown(3);
		//this.iterate();
	},
	addDirection: function(directions, points){
		// Remove all points that happened after this key press (sync issues)
		/*while(this.partnerSnake.points[0].x !== point.x && this.partnerSnake.points[0].y !== point.y)
			this.partnerSnake.points.shift();*/
		this.partnerSnake.points = points;

		// Sync directions
		this.partnerSnake.directions = directions;
		this.draw();						// TODO: is this needed?
	},
	newDirection: function(key){
		this.mainSnake.directions.push(key);

		this.socket.emit('send-direction', {
			d: this.mainSnake.directions, 	// directions
			p: this.mainSnake.points,		// Send snake points. Syncronization assurance
			g: this.gameId,					// gameId
			id: this.partnerId				// partnerId
		});
		this.draw();						// TODO: Is this needed?
	},
	checkDeath: function(){
		if(!this.activeGame) return;

		var that = this;
		var dead = false;
		var draw = false;
		var doub = false;
		var x = this.mainSnake.points[0].x,
			y = this.mainSnake.points[0].y;
		
		this.partnerSnake.points.each(function(point, index){
			if(point.x === x && point.y === y){
				dead = true;
				if(index === 0)
					draw = true;
				else doub = true;
			}
		});
		
		this.mainSnake.points.each(function(point, index){
			if(index !== 0 && point.x === that.mainSnake.points[0].x && point.y === that.mainSnake.points[0].y)
				dead = true;
		});

		if(draw)
			this.socket.emit('draw', {gameId: this.gameId, partnerId: this.partnerId, myScore: that.mainSnake.score, oScore: that.partnerSnake.score, doub: doub, points: this.mainSnake.points});
		else if(dead)
			this.socket.emit('dead', {gameId: this.gameId, partnerId: this.partnerId, myScore: that.mainSnake.score, oScore: that.partnerSnake.score, doub: doub, points: this.mainSnake.points});

		if(draw || dead)
			this.activeGame = false;
	},

	/*
	 * Refresh stats logic
	 */

	refreshScore: function(clear){
		var that = this;

		if(clear && clear === true)
			$('tableHolder').set('html','');
		else
			$('tableHolder').set('html', '<table class="scoreTable">\
			<tr>\
				<td>Running Score</td>\
				<td>Win / Loss</td>\
			</tr>\
			<tr>\
				<td class="score" style="color: green;">'+that.mainSnake.score+'</td>\
				<td class="score">'+that.wins+"/"+that.losses+'</td>\
			</tr>\
			</table>');
	},
	refreshNav: function(){
		if(!this.onlineCount) return;

		var str = "<strong>"+this.onlineCount+"</strong> "+ ((this.onlineCount>1) ? "people" : "person") +" online";
		if(this.username)
			str += " | <strong>"+this.username+"</strong> - <a href='/'>Logout</a>";
		//$('navBar').set('html', str);
	},

	/* 
	 * Refresh the leaderboard
	 */
	refreshLeaderboards: function(leaders){
		if(leaders && leaders.scores && leaders.ratios){
			for(var i = 0; i < leaders.ratios.length; i++){
				leaders.ratios[i].ratio = roundNumber(leaders.ratios[i].ratio, 2);
				if(leaders.ratios[i].username == this.username)
					leaders.ratios[i].rowClass = "highlight";
				else leaders.ratios[i].rowClass = "";
			}
			for(var i = 0; i < leaders.scores.length; i++){
				if(leaders.scores[i].username == this.username)
					leaders.scores[i].rowClass = "highlight";
				else leaders.scores[i].rowClass = "";
			}

			$('leaderboards').set('html', leaderboardsTemplate(leaders));
		}
	},

	snakesArray: function(){
		return [this.mainSnake, this.partnerSnake];
	},

	// Send back points, to be extra sure snakes are in sync
	sendPoints: function(){
		this.socket.emit('send-points', {
			id: this.partnerId,
			g: this.gameId,
			p: this.mainSnake.points
		});
	},

	setScore: function(score){
		this.mainSnake.score = score;
		this.refreshScore();
	},
	setWins: function(wins){
		this.wins = wins;
		this.refreshScore();
	},
	setLosses: function(losses){
		this.losses = losses;
		this.refreshScore();
	},

	startCountdown: function(seconds){
		var that = this;
		if(this.activeGame){
			if(seconds === 0){
				that.overlay.set('html', '');
				this.iterateTimer = this.iterate.periodical(this.options.speed, this);
			}
			else {
				that.setState('countdown', {seconds: seconds});
				setTimeout(function(){
					that.startCountdown(seconds-1);
				}, 1000);
			}	
		}
	},

	setLoginError: function(err){
		$('notificationHolder').set('html', '<div class="alert alert-error">'+err+'</div>');
	}
});

var Snake = new Class({
	initialize: function(options){
		this.options = options;		
		this.points = options.points;
		this.directions = [options.direction];
		this.score = 0;
		this.halt = 0;
	},
	getNewCoords: function(){
		var dir = this.directions.length > 1 ? this.directions.shift() : this.directions[0];
		var tx, ty;
		if(dir === 'left')
			return {
				x: (tx = this.points[0].x - 1) < 0 ? 79 : tx, 
				y: this.points[0].y
			};
		if(dir === 'down')
			return {
				x: this.points[0].x, 
				y: (ty = this.points[0].y + 1) >= 40 ? 0 : ty
			};
		if(dir === 'right')
			return {
				x: (tx = this.points[0].x + 1) >= 80 ? 0 : tx, 
				y: this.points[0].y
			};
		if(dir === 'up')
			return {
				x: this.points[0].x , 
				y: (ty = this.points[0].y - 1) < 0 ? 39 : ty
			};
	},
	step: function(){
		this.points.unshift(this.getNewCoords());
		if(this.halt === 0)
			this.popped = this.points.pop();
		else {
			this.popped = null;
			this.halt--;
		}
	},
	addScore: function(score){
		this.score += score;
		this.halt += score;
	}
});

var Snack = new Class({
	initialize: function(options){
		this.options = options;
		this.x = options.x;
		this.y = options.y;
		this.weight = options.weight;
		this.id = options.id;
	}
});


var FocusTracker = {
    startFocusTracking: function() {
       this.store('hasFocus', false);
       this.addEvent('focus', function() { this.store('hasFocus', true); });
       this.addEvent('blur', function() { this.store('hasFocus', false); });
    },

    hasFocus: function() {
       return this.retrieve('hasFocus');
    }
}

function roundNumber(num, dec) {
	var result = Math.round(num*Math.pow(10,dec))/Math.pow(10,dec);
	return result;
}

Element.implement(FocusTracker);
})();
