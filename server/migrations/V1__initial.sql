CREATE TABLE servers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hostname VARCHAR(255) NOT NULL,
    host VARCHAR(255),
    ws_port INT,
    grpc_port INT,
    region VARCHAR(255) NOT NULL,
    capacity INT DEFAULT 100,
    current_load INT DEFAULT 0,
    registered_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_heartbeat TIMESTAMP,
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
    game_type INT,
    game_state INT,
    status VARCHAR(50) DEFAULT 'waiting',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    FOREIGN KEY (server_id) REFERENCES servers(id)
);

CREATE INDEX idx_games_server_id ON games(server_id);

CREATE TABLE game_requests (
    id SERIAL PRIMARY KEY,
    server_id UUID NOT NULL,
    user_id INT NOT NULL UNIQUE,
    game_type INT NOT NULL,
    request_time TIMESTAMP NOT NULL DEFAULT NOW(),
    FOREIGN KEY (server_id) REFERENCES servers(id),
    FOREIGN KEY (user_id) REFERENCES users(id)
);
