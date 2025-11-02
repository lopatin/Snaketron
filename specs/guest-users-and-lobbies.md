# Guest Users and Lobby System Design

## Overview

This document describes the design and implementation of guest users and a lobby system for SnakeTron. The system allows anonymous users to play without creating accounts and enables players to form lobbies before matchmaking.

## Requirements

1. **Anonymous Guest Users**: Support guest users who can play without creating a permanent account
2. **Lobby-Based Matchmaking**: Refactor matchmaking to work with lobbies instead of individual users
3. **Redis-Based Lobby Presence**: Use Redis heartbeats (with TTL) to track lobby membership
4. **Cross-Region Lobby Joining**: Automatically reconnect users to the correct region when joining a lobby
5. **UI Simplification**: Remove separate lobby page, show lobby in sidebar

## User Scenarios

### Scenario 1: Anonymous Guest User (Solo Play)
1. User visits snaketron.io
2. Sees GameStartForm with empty nickname field and disabled "Start Game" button
3. Enters nickname (3+ characters)
4. Selects game mode and optional competitive checkbox
5. Clicks "Start Game"
6. System automatically:
   - Creates guest user in DynamoDB (with `is_guest=true`)
   - Issues JWT token
   - Creates single-member lobby
   - Queues lobby for matchmaking
7. Sidebar shows "Searching for match..."
8. When match found, user joins game

### Scenario 2: Anonymous Guest User (Invite Friends)
1. User visits snaketron.io
2. Enters nickname
3. Clicks "Invite Friends" button
4. System automatically:
   - Creates guest user
   - Creates lobby with room code
5. Modal appears with:
   - Room code (e.g., "ABCD1234")
   - Copyable link (e.g., "snaketron.io/join/ABCD1234")
6. Sidebar shows lobby members (just the user initially)
7. Friends join via link or room code
8. When ready, host clicks "Start Game" in sidebar
9. Lobby queues for matchmaking

### Scenario 3: Authenticated User
1. User visits snaketron.io (already logged in)
2. Sees GameStartForm with username pre-filled and non-editable
3. Selects game mode
4. Clicks "Start Game"
5. System creates lobby and queues for matchmaking immediately
6. No guest user creation needed

## Architecture

### Database Schema (DynamoDB)

#### User Table
```
PK: USER#{user_id}
SK: META

Attributes:
- id: i32
- username: string
- passwordHash: string (empty for guests)
- mmr: i32
- xp: i32
- isGuest: boolean
- guestToken: string (optional, for guest identification)
- createdAt: timestamp
```

**Guest Users**:
- Stored in main users table with `isGuest=true`
- NOT stored in usernames table (no uniqueness constraint)
- Can have duplicate nicknames
- No password hash

#### Lobby Table
```
PK: LOBBY#{lobby_id}
SK: META

Attributes:
- id: i32 (auto-incrementing)
- lobbyCode: string (8-character code like "ABCD1234")
- hostUserId: i32
- region: string
- createdAt: timestamp
- expiresAt: timestamp
- state: string (waiting | queued | matched)
- ttl: i64
```

#### Lobby Code Index Table
```
Table: snaketron-lobby-codes
PK: lobbyCode

Attributes:
- lobbyCode: string
- lobbyId: i32
- region: string
```

### Redis Data Structures

#### Lobby Member Presence
```
Key: lobby:{lobby_id}:member:{user_id}:{websocket_id}
Value: JSON { username, joinedAt, region }
TTL: 30 seconds
```

Each WebSocket connection maintains a heartbeat loop that refreshes this key every 10 seconds. When a WebSocket disconnects or leaves the lobby, the key is immediately deleted. If a WebSocket crashes, the TTL expires after 30 seconds and Redis automatically removes the member.

#### Lobby Metadata
```
Key: lobby:{lobby_id}:meta
Value: JSON { hostUserId, region, createdAt, lobbyCode }
No TTL (persistent)
```

#### Matchmaking Queue (Modified)
```
Key: matchmaking:queue:{mode}:{game_type_hash}
Type: Sorted Set
Score: timestamp (when lobby joined queue)
Member: lobby_id (changed from user_id)
```

Each lobby is queued as a unit. The matchmaking algorithm now:
1. Fetches lobbies from queue (not users)
2. For each lobby, gets members from Redis
3. Calculates average MMR for the lobby
4. Creates matches grouping compatible lobbies

### Server Components

#### LobbyManager Service

New service that manages lobby membership and presence:

```rust
pub struct LobbyManager {
    redis_client: MultiplexedConnection,
    db: Arc<dyn Database>,
    // Tracks active heartbeat tasks for this server's websockets
    active_lobbies: Arc<RwLock<HashMap<(u32, String), JoinHandle<()>>>>,
}

impl LobbyManager {
    /// Create a new lobby for a user
    pub async fn create_lobby(&mut self, host_user_id: u32, region: &str)
        -> Result<Lobby>

    /// Start heartbeat loop for user in lobby
    /// Returns a handle that automatically cancels on drop
    pub async fn join_lobby(
        &mut self,
        lobby_id: u32,
        user_id: u32,
        username: String,
        websocket_id: String,
        region: &str
    ) -> Result<LobbyJoinHandle>

    /// Stop heartbeat and remove from Redis
    pub async fn leave_lobby(
        &mut self,
        lobby_id: u32,
        user_id: u32,
        websocket_id: &str
    ) -> Result<()>

    /// Get all active members of a lobby from Redis
    pub async fn get_lobby_members(&self, lobby_id: u32)
        -> Result<Vec<LobbyMember>>

    /// Get lobby by ID from DynamoDB
    pub async fn get_lobby(&self, lobby_id: u32) -> Result<Option<Lobby>>

    /// Get lobby by code from DynamoDB
    pub async fn get_lobby(&self, lobby_code: &str)
        -> Result<Option<Lobby>>

    /// Check if lobby is in a different region
    /// Returns Some(redirect_info) if user needs to reconnect
    pub async fn check_lobby_region(
        &self,
        lobby_id: u32,
        current_region: &str
    ) -> Result<Option<RegionRedirect>>
}

pub struct LobbyJoinHandle {
    task: JoinHandle<()>,
    lobby_id: u32,
    user_id: u32,
}

impl Drop for LobbyJoinHandle {
    fn drop(&mut self) {
        // Automatically cancels heartbeat when handle is dropped
        self.task.abort();
    }
}

pub struct LobbyMember {
    pub user_id: u32,
    pub username: String,
    pub joined_at: i64,
    pub is_host: bool,
}

pub struct RegionRedirect {
    pub target_region: String,
    pub ws_url: String,
}
```

**Heartbeat Loop Implementation**:
```rust
async fn heartbeat_loop(
    mut redis: MultiplexedConnection,
    lobby_id: u32,
    user_id: u32,
    username: String,
    websocket_id: String,
    region: String,
) {
    let key = format!("lobby:{}:member:{}:{}", lobby_id, user_id, websocket_id);
    let value = json!({
        "username": username,
        "joinedAt": Utc::now().timestamp_millis(),
        "region": region,
    });

    let mut interval = tokio::time::interval(Duration::from_secs(10));

    loop {
        interval.tick().await;

        // Set key with 30-second TTL
        match redis.set_ex(&key, value.to_string(), 30).await {
            Ok(_) => {},
            Err(e) => {
                error!("Failed to refresh lobby presence: {}", e);
                break;
            }
        }
    }
}
```

#### WebSocket Message Changes

```rust
#[derive(Debug, Serialize, Deserialize)]
pub enum WSMessage {
    // Existing messages...
    Token(String),
    JoinGame(u32),
    LeaveGame,
    // ... etc

    // New lobby messages
    CreateLobby,
    LobbyCreated {
        lobby_id: u32,
        lobby_code: String
    },
    JoinLobby {
        lobby_code: String
    },
    JoinedLobby {
        lobby_id: u32
    },
    LeaveLobby,
    LeftLobby,
    LobbyUpdate {
        lobby_id: u32,
        members: Vec<LobbyMember>,
        host_user_id: u32,
    },
    LobbyRegionMismatch {
        target_region: String,
        ws_url: String,
        lobby_code: String,
    },

    // Modified messages
    QueueForMatch {
        game_type: GameType,
        queue_mode: QueueMode,
        // lobby_id is now implicit from connection state
    },
    // ... rest unchanged
}
```

#### Connection State Changes

```rust
enum ConnectionState {
    Unauthenticated,

    Authenticated {
        metadata: PlayerMetadata,
    },

    // New state: In lobby but not in game
    InLobby {
        metadata: PlayerMetadata,
        lobby_id: u32,
        lobby_join_handle: LobbyJoinHandle, // Auto-cleanup on drop
    },

    InGame {
        metadata: PlayerMetadata,
        game_id: u32,
        // Also track lobby_id if game was created from lobby
        lobby_id: Option<u32>,
    },

    ShuttingDown {
        timeout: Pin<Box<Sleep>>,
    },
}
```

### Client Components

#### Auth API Changes (`client/web/services/api.ts`)

```typescript
interface CreateGuestRequest {
  nickname: string;
}

interface CreateGuestResponse {
  token: string;
  user: User & { isGuest: boolean };
}

class API {
  async createGuest(nickname: string): Promise<CreateGuestResponse> {
    const data = await this.request<CreateGuestResponse>('/auth/guest', {
      method: 'POST',
      body: JSON.stringify({ nickname }),
    });
    this.setAuthToken(data.token);
    return data;
  }

  // ... existing methods
}
```

#### WebSocket Context Changes (`client/web/contexts/WebSocketContext.tsx`)

```typescript
interface WebSocketContextType {
  // ... existing fields

  // New lobby fields
  currentLobby: Lobby | null;
  lobbyMembers: LobbyMember[];

  // New lobby methods
  createLobby: () => Promise<void>;
  joinLobby: (lobbyCode: string) => Promise<void>;
  leaveLobby: () => Promise<void>;

  // ... existing methods
}

interface Lobby {
  id: number;
  code: string;
  hostUserId: number;
  region: string;
}

interface LobbyMember {
  userId: number;
  username: string;
  isHost: boolean;
}
```

#### Updated Sidebar (`client/web/components/Sidebar.tsx`)

The sidebar now shows real-time lobby members:

```tsx
export const Sidebar: React.FC<SidebarProps> = ({
  regions,
  currentRegionId,
  onRegionChange,
  lobbyMembers, // Real-time from WebSocket
  currentUsername,
  onInvite,
  onStartGame, // NEW: Callback to queue for match
  lobbyCode, // NEW: Show lobby code if in lobby
}) => {
  return (
    <aside className="sidebar">
      {/* ... existing navigation ... */}

      {/* Lobby Section */}
      {lobbyMembers.length > 0 && (
        <div className="lobby-section">
          {lobbyCode && (
            <div className="lobby-code">
              <span>Code: {lobbyCode}</span>
              <button onClick={onCopyCode}>Copy</button>
            </div>
          )}

          <h3>Lobby</h3>
          <div className="lobby-members">
            {lobbyMembers.map(member => (
              <div key={member.userId} className="lobby-member">
                <span>{member.username}</span>
                {member.isHost && <span>(Host)</span>}
              </div>
            ))}
          </div>

          {isHost && (
            <button onClick={onStartGame}>Start Game</button>
          )}
          <button onClick={onLeaveLobby}>Leave Lobby</button>
        </div>
      )}

      {lobbyMembers.length === 0 && (
        <button onClick={onInvite}>Invite Friends</button>
      )}

      {/* ... social icons ... */}
    </aside>
  );
};
```

#### Updated GameStartForm (`client/web/components/GameStartForm.tsx`)

```tsx
export const GameStartForm: React.FC<GameStartFormProps> = ({
  onStartGame,
  currentUsername,
  isLoading = false
}) => {
  const { user, createGuest } = useAuth();
  const { createLobby, queueForMatch } = useWebSocket();
  const [nickname, setNickname] = useState(currentUsername || '');

  const handleStartGame = async (e: React.FormEvent) => {
    e.preventDefault();

    // If guest, create guest user first
    if (!user) {
      await createGuest(nickname);
    }

    // Create lobby
    await createLobby();

    // Immediately queue for matchmaking (single-player lobby)
    await queueForMatch(selectedMode, isCompetitive);
  };

  const handleInviteFriends = async () => {
    // If guest, create guest user first
    if (!user) {
      await createGuest(nickname);
    }

    // Create lobby (don't queue yet)
    await createLobby();

    // Show invite modal (with code + link)
    showInviteModal();
  };

  // ... rest of form UI
};
```

### Cross-Region Lobby Support

When a user tries to join a lobby in a different region:

1. **Server Detection** (`server/src/lobby_manager.rs`):
```rust
pub async fn check_lobby_region(
    &self,
    lobby_id: u32,
    current_region: &str
) -> Result<Option<RegionRedirect>> {
    let lobby = self.get_lobby(lobby_id).await?
        .ok_or_else(|| anyhow!("Lobby not found"))?;

    if lobby.region != current_region {
        // Look up WebSocket URL for target region
        let ws_url = self.db.get_region_ws_url(&lobby.region).await?;

        Ok(Some(RegionRedirect {
            target_region: lobby.region,
            ws_url,
        }))
    } else {
        Ok(None)
    }
}
```

2. **WebSocket Handler** (`server/src/ws_server.rs`):
```rust
WSMessage::JoinLobby { lobby_code } => {
    // Check region
    if let Some(redirect) = lobby_manager.check_lobby_region(...).await? {
        // Send redirect message
        ws_tx.send(WSMessage::LobbyRegionMismatch {
            target_region: redirect.target_region,
            ws_url: redirect.ws_url,
            lobby_code,
        }).await?;

        return Ok(state); // Don't transition state
    }

    // Same region, proceed with join
    // ...
}
```

3. **Client Reconnection** (`client/web/contexts/WebSocketContext.tsx`):
```typescript
useEffect(() => {
  const handleMessage = (msg: WSMessage) => {
    if (msg.type === 'LobbyRegionMismatch') {
      // Close current connection
      ws.close();

      // Connect to new region
      const newWs = new WebSocket(msg.wsUrl);
      setWs(newWs);

      // After connection, retry join
      newWs.addEventListener('open', () => {
        newWs.send(JSON.stringify({
          type: 'JoinLobby',
          lobby_code: msg.lobbyCode
        }));
      });
    }
  };

  // ... rest of handler
}, [ws]);
```

## Implementation Plan

### Phase 1: Guest Users Backend ✅ COMPLETED
**Files to modify**:
- `server/src/db/models.rs` - Add `is_guest`, `guest_token` to User ✅
- `server/src/db/dynamodb.rs` - Modify user creation logic ✅
- `server/src/api/auth.rs` - Add `create_guest` endpoint ✅
- `server/src/api/jwt.rs` - Include `is_guest` in JWT claims ✅

**Testing**: Create guest users via API, verify JWT tokens ✅

### Phase 2: Guest Users Frontend ✅ COMPLETED
**Files to modify**:
- `client/web/services/api.ts` - Add createGuest method ✅
- `client/web/contexts/AuthContext.tsx` - Handle guest users ✅
- `client/web/components/GameStartForm.tsx` - Auto-create guests ✅
- `client/web/types/index.ts` - Update User type ✅

**Testing**: Full guest user flow from UI ✅

### Phase 3: Lobby Manager Backend ✅ COMPLETED
**Files to create**:
- `server/src/lobby_manager.rs` - New LobbyManager service ✅

**Files to modify**:
- `server/src/redis_keys.rs` - Add lobby key functions ✅
- `server/src/db/mod.rs` - Add lobby trait methods ✅
- `server/src/db/dynamodb.rs` - Implement lobby operations ✅
- `server/src/ws_server.rs` - Integrate LobbyManager ✅
  - Added InLobby connection state ✅
  - Implemented CreateLobby handler ✅
  - Implemented JoinLobby handler ✅
  - Implemented LeaveLobby handler ✅
  - Added lobby update subscriptions via Redis pub/sub ✅
  - Periodic LobbyUpdate broadcasts (every 10s) ✅
- `server/src/lib.rs` - Export lobby_manager module ✅
- `server/src/http_server.rs` - Wire up LobbyManager to WebSocket handler ✅

**Testing**: Create lobbies, join/leave, verify Redis heartbeats ⏳ PENDING

### Phase 4: Lobby UI ✅ COMPLETED
**Files to modify**:
- `client/web/types/index.ts` - Add Lobby and LobbyMember types ✅
- `client/web/contexts/WebSocketContext.tsx` - Add lobby state/methods ✅
  - Added currentLobby and lobbyMembers state ✅
  - Implemented createLobby(), joinLobby(), leaveLobby() methods ✅
  - Added LobbyUpdate message handler with automatic state updates ✅
  - Added LobbyRegionMismatch handler with automatic reconnection ✅
- `client/web/components/Sidebar.tsx` - Show lobby members ✅
  - Updated props to accept lobbyMembers, lobbyCode, currentUserId, isHost ✅
  - Added lobby code display with copy button ✅
  - Show real-time lobby member list with host indicator ✅
  - Added Start Game button (host only) and Leave Lobby button ✅
- `client/web/components/NewHome.tsx` - Wire up lobby actions ✅
  - Integrated WebSocketContext lobby state and methods ✅
  - Implemented handleInvite to create lobby ✅
  - Implemented handleLeaveLobby and handleStartGameFromLobby ✅
  - Updated Sidebar props to pass lobby state ✅

**Files to delete**:
- `client/web/components/GameLobby.tsx` - Remove full-page lobby (not needed - lobby shown in sidebar)

**Testing**: UI shows real-time lobby updates ⏳ PENDING

### Phase 5: Matchmaking Integration ✅ COMPLETED
**Files modified**:
- `server/src/ws_server.rs` - Complete QueueForMatch handler in InLobby state ✅
  - Added host-only permission check for queuing ✅
  - Fetches all lobby members from Redis ✅
  - Calculates average MMR from all member MMRs ✅
  - Adds lobby to matchmaking queue via MatchmakingManager ✅
  - Subscribes to lobby match notifications for all members ✅
  - Sends JoinGame message when match found ✅
- `server/src/matchmaking_manager.rs` - Update queue structures ✅
  - Added QueuedLobby data structure ✅
  - Implemented add_lobby_to_queue() method ✅
  - Implemented get_queued_lobbies() method ✅
  - Implemented remove_lobby_from_queue() method ✅
- `server/src/redis_keys.rs` - Add lobby queue key methods ✅
  - matchmaking_lobby_queue() ✅
  - matchmaking_lobby_mmr_index() ✅
  - matchmaking_lobby_notification_channel() ✅
- `server/src/matchmaking.rs` - Add lobby matchmaking algorithm ✅
  - Implemented create_lobby_matches() function ✅
  - Integrated into main matchmaking loop for both Quickmatch and Competitive ✅
  - Publishes match notifications to lobby channel ✅
  - Removes matched lobbies from queue ✅

**Implementation Details**:
- ✅ QueueForMatch handler verifies host permission before queuing
- ✅ Lobby members fetched from Redis using lobby_manager.get_lobby_members()
- ✅ Average MMR calculated: sum of all members' MMR / member count
- ✅ Lobby added to Redis sorted set: `matchmaking:lobby:queue:{mode}:{game_type_hash}`
- ✅ Each lobby queues as a unit with lobby_id as identifier
- ✅ Matchmaking algorithm processes lobbies separately from individual players
- ✅ Match notification published to `matchmaking:lobby:notification:{lobby_id}`
- ✅ All lobby members receive JoinGame message via subscription
- ✅ WebSocket clients automatically wait for game to be available in replication manager

**Testing**: Lobbies can queue and match correctly ⏳ PENDING

### Phase 6: Cross-Region Support
**Files to modify**:
- `server/src/lobby_manager.rs` - Add region checking
- `server/src/ws_server.rs` - Handle region mismatch
- `client/web/contexts/WebSocketContext.tsx` - Reconnection logic

**Testing**: Join lobbies across regions

## Testing Strategy

### Unit Tests
- LobbyManager: Create, join, leave, heartbeat
- Redis key generation
- Guest user creation
- JWT with guest flag

### Integration Tests
- Full guest user flow (API → WebSocket → Lobby)
- Lobby heartbeat and TTL expiry
- Cross-region lobby join with reconnection
- Matchmaking with lobbies

### Manual Testing
- Guest user can play immediately
- Invite friends flow works
- Sidebar shows real-time updates
- Region switching works
- Lobby members disappear when they disconnect

## Deployment Considerations

### Database Migration
- DynamoDB schema changes are backward compatible (adding fields)
- Deploy server first, then client
- No downtime required

### Redis
- New key patterns don't conflict with existing
- Clean rollback: just stop using new keys

### Backward Compatibility
- Authenticated users continue to work normally
- Matchmaking for solo players: single-member lobbies
- No breaking changes to existing game flow

## Future Enhancements

### Guest to Registered Migration
- Allow guests to "claim" their account
- Preserve MMR and stats
- Link guest token to new account

### Lobby Features
- Kick player (host only)
- Transfer host
- Lobby chat
- Ready/not ready status
- Team selection for team games

### Matchmaking Improvements
- Party MMR adjustment (groups tend to coordinate better)
- Lobby size preferences
- Find more lobbies vs. find solos

## Open Questions

1. **Guest username collisions**: Should we append numbers if duplicate? (e.g., "Guest#1234")
2. **Lobby expiry**: Should DynamoDB lobbies have TTL too, or just Redis presence?
3. **Matchmaking fairness**: Should lobbies of different sizes be matched against each other?
4. **Region switching**: Should we support changing regions while in a lobby?

## References

- Current matchmaking: `server/src/matchmaking.rs`
- Current user auth: `server/src/api/auth.rs`
- Redis patterns: `server/src/redis_keys.rs`
- WebSocket handling: `server/src/ws_server.rs`
