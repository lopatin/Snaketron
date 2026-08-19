import React from 'react';
import { useParams } from 'react-router-dom';
import GameArena from './GameArena';
import ProtectedRoute from './ProtectedRoute';
import PlayerInvitePage from './PlayerInvitePage';
import { parseU32GameId } from '../utils/gameId';

/**
 * `/play/:gameId` serves two things that cannot be told apart by the router.
 *
 * A numeric segment is a live game (`/play/4213`); anything else is a player
 * invite link (`/play/lopatron`). React Router ranks `/play/:gameId` and a
 * hypothetical `/play/:username` identically, so the choice has to be made
 * inside one element rather than by registering two routes.
 *
 * Numeric wins, always. Game ids are `SERIAL` and reaching a live match is
 * load-bearing, so the ambiguous case resolves in favour of the arena. The
 * cost is that a wholly numeric username — `validate_username` permits one —
 * is not reachable through this link, which is a better trade than making
 * every game URL depend on a database lookup.
 *
 * The two branches also differ in authentication: the arena requires a session
 * (`ProtectedRoute`), while an invite must work for a first-time visitor and
 * mints a guest itself.
 */
const PlayRoute: React.FC = () => {
  const { gameId } = useParams<{ gameId: string }>();

  if (parseU32GameId(gameId) === null) {
    return <PlayerInvitePage username={gameId ?? ''} />;
  }

  return (
    <ProtectedRoute>
      <GameArena />
    </ProtectedRoute>
  );
};

export default PlayRoute;
