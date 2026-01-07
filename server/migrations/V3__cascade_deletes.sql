-- Add CASCADE DELETE to foreign key constraints
ALTER TABLE games DROP CONSTRAINT games_server_id_fkey;
ALTER TABLE games ADD CONSTRAINT games_server_id_fkey 
    FOREIGN KEY (server_id) REFERENCES servers(id) ON DELETE CASCADE;

ALTER TABLE game_requests DROP CONSTRAINT game_requests_server_id_fkey;
ALTER TABLE game_requests ADD CONSTRAINT game_requests_server_id_fkey 
    FOREIGN KEY (server_id) REFERENCES servers(id) ON DELETE CASCADE;