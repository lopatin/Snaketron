CREATE TABLE servers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    address VARCHAR(255) NOT NULL,
    grpc_address VARCHAR(255),
    last_heartbeat TIMESTAMP NOT NULL DEFAULT NOW(),
    current_game_count INT NOT NULL DEFAULT 0,
    max_game_capacity INT NOT NULL DEFAULT 100,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
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
