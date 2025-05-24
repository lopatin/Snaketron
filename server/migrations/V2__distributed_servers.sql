-- Add support for distributed game servers

-- Add gRPC port to servers table
ALTER TABLE servers ADD COLUMN grpc_port INT;

-- Add capacity tracking for load balancing
ALTER TABLE servers ADD COLUMN capacity INT DEFAULT 100;
ALTER TABLE servers ADD COLUMN current_load INT DEFAULT 0;

-- Add server assignment to games
ALTER TABLE games ADD COLUMN server_id UUID REFERENCES servers(id);
CREATE INDEX idx_games_server_id ON games(server_id);

-- Update existing games to have NULL server_id (will be assigned when started)
UPDATE games SET server_id = NULL;