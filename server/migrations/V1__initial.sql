CREATE TABLE servers (
    id SERIAL PRIMARY KEY,
    ip_address VARCHAR(45),
    grpc_port INT NOT NULL DEFAULT 50051,
    raft_port INT NOT NULL DEFAULT 50052,
    grpc_address VARCHAR(255) NOT NULL,
    last_heartbeat TIMESTAMP,
    region VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    current_game_count INT NOT NULL DEFAULT 0,
    max_game_capacity INT NOT NULL DEFAULT 100
);

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    mmr INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE games (
    id SERIAL PRIMARY KEY,
    server_id INT,
    game_type JSONB NOT NULL,
    game_state JSONB,
    status VARCHAR(20) NOT NULL DEFAULT 'waiting',
    ended_at TIMESTAMPTZ,
    last_activity TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    game_mode VARCHAR(20) NOT NULL DEFAULT 'matchmaking',
    is_private BOOLEAN NOT NULL DEFAULT FALSE,
    game_code VARCHAR(8),
    FOREIGN KEY (server_id) REFERENCES servers(id),
    CONSTRAINT games_status_check CHECK (status IN ('waiting', 'active', 'finished', 'abandoned')),
    CONSTRAINT games_mode_check CHECK (game_mode IN ('matchmaking', 'custom', 'solo'))
);

CREATE INDEX idx_games_server_id ON games(server_id);
CREATE INDEX idx_games_status_last_activity ON games(status, last_activity);

CREATE TABLE game_requests (
    id SERIAL PRIMARY KEY,
    server_id INT NOT NULL,
    user_id INT NOT NULL UNIQUE,
    game_type JSONB NOT NULL,
    game_id INT DEFAULT NULL,
    request_time TIMESTAMP NOT NULL DEFAULT NOW(),
    FOREIGN KEY (server_id) REFERENCES servers(id),
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE SET NULL
);

CREATE TABLE game_players (
    id SERIAL PRIMARY KEY,
    game_id INT NOT NULL,
    user_id INT NOT NULL,
    team_id INT NOT NULL,
    joined_at TIMESTAMP NOT NULL DEFAULT NOW(),
    FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id),
    UNIQUE(game_id, user_id)
);

-- New table for custom game lobbies
CREATE TABLE custom_game_lobbies (
    id SERIAL PRIMARY KEY,
    game_code VARCHAR(8) UNIQUE NOT NULL,
    host_user_id INTEGER NOT NULL REFERENCES users(id),
    settings JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP NOT NULL,
    game_id INTEGER REFERENCES games(id),
    state VARCHAR(20) NOT NULL DEFAULT 'waiting'
);

CREATE INDEX idx_custom_game_lobbies_game_code ON custom_game_lobbies(game_code);
CREATE INDEX idx_custom_game_lobbies_expires_at ON custom_game_lobbies(expires_at);

-- Add spectators table
CREATE TABLE game_spectators (
    game_id INTEGER NOT NULL REFERENCES games(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES users(id),
    joined_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (game_id, user_id)
);
