var express = require('express');
var app = express.createServer();
var io = require('socket.io').listen(app);  // Socket.io
var async = require('async');               // Flow control
var mysqlConfig = require('./mysqlConfig');
var _ = require('underscore');
var passHash = require('./passHash');

app.listen(8081);


// Public files config
app.configure(function(){
    app.use(express.static(__dirname + '/public'));
});

// Database
var mysql = require('mysql');
var client = mysql.createClient({
  user: mysqlConfig.username,
  password: mysqlConfig.password
});
client.query("USE snaketron");

var playerQueue = [];
var games = {};

var playerCount = 0;

var leaderboards;
refreshLeaderboards();

io.sockets.on('connection', function(socket){

    refreshPlayerCount(socket, true);    

    /*
     * Register user
     */
    socket.on('register', function(data){
        console.log(data.username[0].length);
        if(!data.password[0])
            socket.emit('register-error', {error: "Please use a password"});
        else if(!data.username[0])
            socket.emit('register-error', {error: "Username is empty"});
        else if(data.username[0].length > 20)
            socket.emit('register-error', {error: "Username must be under 20 characters"});
        else
        client.query('SELECT * FROM users WHERE username=?', [data.username], function(err, results){
            if(results.length > 0)
                socket.emit('register-error', {error: "Username in use"});
            else {
                client.query('INSERT INTO users VALUES ("", ?, ?, 0, 0, 0)', [data.username, passHash.hash(data.password)], function(err, results){
                    socket.emit('menu', {username: data.username, score: 0, wins: 0, losses: 0, playerCount: playerCount});

                    socket.anon = false;
                    socket.playerId = results.insertId;
                    
                });
            }
        });
    });

    /*
     * Login as user or anonymous
     * Identify yourself to server and view the menu
     */
    socket.on('login', function(data){
        if(data.anon)
            getAnonName(function(newName, id){
                socket.emit('menu', {username: newName, score: 0, wins: 0, losses: 0, playerCount: playerCount});
                socket.anon = true;
                socket.playerId = id;
            });
        else
            client.query('SELECT * FROM users WHERE username=?', [data.username], function(err, results){
                if(!err && results.length > 0 && passHash.validate(results[0].password, data.password)){
                    socket.emit('menu', {username: results[0].username, score: results[0].score, wins: results[0].wins, losses: results[0].losses, playerCount: playerCount});
                    socket.anon = false;
                    socket.playerId = results[0].id;
                }
                else 
                    socket.emit('login-error', {error: "Invalid username or password"});
            });
    });

    socket.on('random-opponent', function(username){
        playerQueue.push({
            uid: getUID(),
            socket: socket,
            username: username
        });
        connectPlayers();
    });

    socket.on('eaten-snack', function(data){
        if(games[data.gameId] && games[data.gameId].snacks[data.snackId]){
            emitToAll(data.gameId, 'remove-snack', {id: data.snackId, weight: data.snackWeight, pid: data.partnerId});
            sendSnacks(data.gameId);
            delete games[data.gameId].snacks[data.snackId];
            addScore(socket, data.snackWeight);
        }



    });

    socket.on('disconnect', function(){
        refreshPlayerCount(socket, false);

        for(var i = 0; i < playerQueue.length; i++){
            if(playerQueue[i].socket == socket)
                playerQueue.splice(i,1);
        }
        
        // Delete the game and notify the other player if a game is in progress 
        for(key in games)
            if(games.hasOwnProperty(key))
                for(pid in games[key].players)
                    if(games[key] && games[key].players.hasOwnProperty(pid) && socket == games[key].players[pid]){
                        for(pid2 in games[key].players)
                            if(games[key].players.hasOwnProperty(pid2))
                                if(pid2 != pid){
                                    games[key].players[pid2].emit('gameover', {message: "Lucky you! Your opponent disconnected.", winner: games[key].players[pid2].username});
                                    lostGame(socket);
                                    wonGame(games[key].players[pid2]);
                                }
                        delete games[key];
                    }
    });

    socket.on('send-direction', function(data){
        if(games[data.g])
            games[data.g].players[data.id].emit('add-direction', data);
    });

    socket.on('dead', function(data){
        if(games[data.gameId]){
            emitToAll(data.gameId, 'gameover', {winner: games[data.gameId].players[data.partnerId].username});
            games[data.gameId].players[data.partnerId].emit('send-points', data.points);
            //saveGame(data);

            lostGame(socket);
            if(games[data.gameId] && games[data.gameId].players[data.partnerId])
                wonGame(games[data.gameId].players[data.partnerId]);

            delete games[data.gameId];
        }
    });

    socket.on('draw', function(data){
        if(games[data.gameId]){
            emitToAll(data.gameId, 'draw', data);
            games[data.gameId].players[data.partnerId].emit('send-points', data.points);
            //saveGame(data);

            delete games[data.gameId];
        }
    });

    socket.on('send-points', function(data){
        if(games[data.g]){
            games[data.g].players[data.id].emit('send-points', data.p);
        }
    })
});

function saveGame(data){
            var me, them;
            for(key in games[data.gameId].players)
                if(games[data.gameId].players.hasOwnProperty(key)){
                    if(key === data.partnerId)
                        them = {
                            name: games[data.gameId].players[key].username,
                            score: data.doub ? 2*data.oScore : data.oScore};
                    else
                        me = {
                            name: games[data.gameId].players[key].username,
                            score: data.myScore};
                }
            client.query("INSERT INTO games VALUES('', ?, ?, ?, ?)", [me.name, them.name, me.score, them.score]);
} 

function getAnonName(cb){
    var anonName;
    async.series([
        function(callback){
            client.query('INSERT INTO anons VALUES("", 0, 0, 0)', function(err, info){
                callback(null, info.insertId);
            });
        }
    ], function(err, results){
            anonName = "Anon #"+results[0];
            cb(anonName, results[0]);
        });
}

function connectPlayers(){
    if(playerQueue.length >= 2){
        var player1 = playerQueue.shift(),
            player2 = playerQueue.shift(),
            gameId = getUID();

        games[gameId] = { players: { }, snacks: {} };
        games[gameId].players[player1.uid] = player1.socket;
        games[gameId].players[player1.uid].username = player1.username;
        games[gameId].players[player2.uid] = player2.socket;
        games[gameId].players[player2.uid].username = player2.username;
        
        player2.socket.emit('start-game', {
            gameId: gameId,
            partnerId: player1.uid,
            partnerUsername: player1.username,
            position: 'right'
        });
        player1.socket.emit('start-game', {
            gameId: gameId,
            partnerId: player2.uid,
            partnerUsername: player2.username,
            position: 'left'
        });

        sendSnacks(gameId);
        sendSnacks(gameId);


        setTimeout(function(){setSpeed(gameId, 90)}, 30000);
        setTimeout(function(){setSpeed(gameId, 80)}, 60000);
    }
    else{
        var player = playerQueue[0];
        player.socket.emit('waiting', {});
    }
}

/*
 * Snack and Score functions
 */

// Add score to database
function addScore(socket, score){
    var table = socket.anon ? 'anons' : 'users';
    client.query("UPDATE "+table+" SET score=score+? WHERE id=?", [score, socket.playerId]);
}

function resetScore(socket){
    var table = socket.anon ? 'anons' : 'users';
    client.query("UPDATE "+table+" SET score=0 WHERE id=?", [socket.playerId]);
    socket.emit('reset-score');
}

/*
 * Won / Lost game logic
 */

function wonGame(socket){
    var table = socket.anon ? 'anons' : 'users';
    console.log("UPDATE "+table+" SET wins=wins+1 WHERE id=?", [socket.playerId]);
    client.query("UPDATE "+table+" SET wins=wins+1 WHERE id=?", [socket.playerId]);
    socket.emit('add-win');

    refreshLeaderboards(socket);
}

function lostGame(socket){
    var table = socket.anon ? 'anons' : 'users';
    client.query("UPDATE "+table+" SET losses=losses+1 WHERE id=?", [socket.playerId]);    
    resetScore(socket);
    socket.emit('add-loss');
}

/*
 * Refresh leaderboards
 */

function refreshLeaderboards(socket){
    async.parallel({
        anonRatio: function (callback){
            client.query("SELECT id, CONCAT('Anon #', id) as username, wins, losses, (wins/losses) AS ratio FROM anons ORDER BY ratio DESC LIMIT 10", function(err, results){
                callback(null, results);
            });
        },
        anonScore: function (callback){
            client.query("SELECT id, CONCAT('Anon #', id) as username, score FROM anons ORDER BY score DESC LIMIT 10", function(err, results){
                callback(null, results);
            });
        },
        usersRatio: function (callback){
            client.query("SELECT username, wins, losses, (wins/losses) AS ratio FROM users ORDER BY ratio DESC LIMIT 10", function(err, results){
                callback(null, results);
            });
        },
        usersScore: function (callback){
            client.query("SELECT username, score FROM users ORDER BY score DESC LIMIT 10", function(err, results){
                callback(null, results);
            });
        }
    }, function(err, results){
        var leaders = {};
        leaders.ratios = _.first(
            _.sortBy(
                _.union(
                    results.anonRatio,
                    results.usersRatio), 
                function(e){return parseFloat(e.ratio);}).reverse(), 
            10);
        leaders.scores = _.first(
            _.sortBy(
                _.union(
                    results.anonScore,
                    results.usersScore), 
                function(e){return parseFloat(e.score);}).reverse(), 
            10);

        for(var i = 1; i <= leaders.ratios.length; i++)
            leaders.ratios[i-1].position = i;
        for(var i = 1; i <= leaders.scores.length; i++)
            leaders.scores[i-1].position = i;

        if(!_.isEqual(leaderboards, leaders)){
            leaderboards = leaders;
            if(socket){
                socket.broadcast.emit('leaderboards', leaders);
                socket.emit('leaderboards', leaders);
            }
        }
    });
}

/*

needed?

function sendRecord(socket){
    var table = socket.anon ? 'anons' : 'users';
    client.query("SELECT wins, losses FROM "+table+" WHERE id=?", [socket.playerId], function(err, results){
        if(!err && results.length){
            socket.emit('record', {wins: results[0].wins, losses: results[0].losses});
        }
    });
}
*/

/*
 * Let all clients know the current number of players online
 */
function refreshPlayerCount(socket, active){
    playerCount = _.filter(io.sockets.clients(), function(c){return c != null;}).length;
    socket.broadcast.emit('player-count', playerCount);
    if(active){
        socket.emit('player-count', playerCount);
        socket.emit('leaderboards', leaderboards);
    }
}

// send snacks
function sendSnacks(gameId){
    var game = games[gameId];
    var x = Math.floor(Math.random()*80);
    var y = Math.floor(Math.random()*40);
    var sid = getUID();
    game.snacks[sid] = {id: sid, x: x, y: y, weight: 5};

    emitToAll(gameId, 'snack', game.snacks[sid]);
}

// Increase speed
function setSpeed(gameId, speed){
    if(games[gameId]){
        emitToAll(gameId, 'set-speed', speed);
    }
}

function emitToAll(gameId, messageName, data){
    var game = games[gameId];
    for (var pid in game.players){
        if(game.players.hasOwnProperty(pid)){
            game.players[pid].emit(messageName, data);
        }
    }
}

function getUID(){
    return (Date.now()) + 'x' + Math.round(Math.random() *1E18);
}
