CREATE TABLE servers (
    id SERIAL PRIMARY KEY,
    grpc_address VARCHAR(255) NOT NULL,
    last_heartbeat TIMESTAMP,
    region VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL
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
    server_id UUID,
    game_type JSONB NOT NULL,
    game_state JSONB,
    status VARCHAR(20) NOT NULL DEFAULT 'waiting',
    ended_at TIMESTAMPTZ,
    last_activity TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    FOREIGN KEY (server_id) REFERENCES servers(id),
    CONSTRAINT games_status_check CHECK (status IN ('waiting', 'active', 'finished', 'abandoned'))
);

CREATE INDEX idx_games_server_id ON games(server_id);
CREATE INDEX idx_games_status_last_activity ON games(status, last_activity);

CREATE TABLE game_requests (
    id SERIAL PRIMARY KEY,
    server_id UUID NOT NULL,
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
