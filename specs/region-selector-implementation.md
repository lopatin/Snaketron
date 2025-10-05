# Region Selector Implementation Specification

**Status:** Design Approved
**Created:** 2025-10-05
**Last Updated:** 2025-10-05

## Overview

This specification defines the implementation of a dynamic region selector system that allows users to view and select game server regions with real-time user counts and client-measured latency.

## Requirements

1. **Region Discovery:** Browser fetches available regions from backend API
2. **User Count Display:** Real-time user counts aggregated from Redis (updated every 5s)
3. **Latency Measurement:** Client-side HTTP ping to each region endpoint
4. **Auto-Selection:** Automatically select best region based on latency
5. **Manual Override:** Allow users to manually select a different region
6. **Persistence:** Remember user's region preference in localStorage
7. **Reconnection:** WebSocket connects to selected region's endpoint
8. **Clean Architecture:** Logic in custom hooks, UI components remain presentational

## Architecture

### Data Flow

```
1. App Mount
   ↓
2. useRegions() hook initializes
   ↓
3. Fetch /api/regions → Get region metadata (id, name, origin, wsUrl)
   ↓
4. Parallel ping measurement → HTTP GET /api/health for each region
   ↓
5. Fetch /api/regions/user-counts → Get aggregated user counts from Redis
   ↓
6. Auto-select region (localStorage preference OR best ping)
   ↓
7. Connect WebSocket to selected region's wsUrl
   ↓
8. Every 5s: Refresh user counts from /api/regions/user-counts
```

### Component Hierarchy

```
App.tsx
  └─ Header
       └─ RegionSelector (presentational)
            ├─ uses: useRegions() hook
            └─ triggers: WebSocket reconnection on region change
```

## Type Definitions

**File:** `client/web/types.ts`

```typescript
// Enhanced Region type with backend URL
export interface Region {
  id: string;              // e.g., "us-east", "eu-west"
  name: string;            // e.g., "US East", "Europe"
  origin: string;          // e.g., "https://use1.snaketron.io" or "http://localhost:8080"
  wsUrl: string;           // e.g., "wss://use1.snaketron.io/ws"
  userCount: number;       // Aggregated from Redis
  ping: number | null;     // Client-measured latency in ms
  isConnected: boolean;    // Whether this is the active region
}

// API response from GET /api/regions
export interface RegionMetadata {
  id: string;
  name: string;
  origin: string;
  wsUrl: string;
}

// localStorage schema
export interface RegionPreference {
  regionId: string;
  timestamp: number;
}
```

## Custom Hook: useRegions

**File:** `client/web/hooks/useRegions.ts` (NEW)

### API

```typescript
interface UseRegionsReturn {
  regions: Region[];                    // All available regions with live data
  selectedRegion: Region | null;        // Currently selected region
  selectedRegionId: string | null;      // ID of selected region
  selectRegion: (regionId: string) => void;  // Manually select a region
  isLoading: boolean;                   // Initial load state
  error: string | null;                 // Error message if any
  refreshRegions: () => Promise<void>;  // Force re-fetch all data
}

export function useRegions(): UseRegionsReturn
```

### Key Responsibilities

1. **Initialization:**
   - Fetch region metadata from `/api/regions`
   - Measure ping to each region in parallel
   - Fetch user counts from `/api/regions/user-counts`
   - Combine data into `Region[]` array

2. **Auto-Selection Logic:**
   - Check localStorage for saved preference
   - If preference exists and region is available, use it
   - Otherwise, select region with lowest ping
   - Save selection to localStorage

3. **Periodic Updates:**
   - Every 5 seconds, refresh user counts
   - Update `regions` state with new counts
   - Maintain existing ping values (no re-ping on updates)

4. **Manual Selection:**
   - `selectRegion(regionId)` updates selected region
   - Saves preference to localStorage
   - Triggers WebSocket reconnection (via useEffect in App.tsx)

### Implementation Details

```typescript
// localStorage keys
const REGION_PREFERENCE_KEY = 'snaketron_selected_region';

// Auto-selection algorithm
function autoSelectRegion(regions: Region[], savedPreference?: string): string {
  // 1. Try saved preference first
  if (savedPreference && regions.some(r => r.id === savedPreference)) {
    return savedPreference;
  }

  // 2. Otherwise, select region with best ping
  const bestRegion = regions
    .filter(r => r.ping !== null)
    .sort((a, b) => (a.ping ?? Infinity) - (b.ping ?? Infinity))[0];

  return bestRegion?.id ?? regions[0]?.id;
}
```

## Backend API Endpoints

### 1. GET `/api/regions`

**Purpose:** Return list of available regions with connection metadata

**Response:**
```json
[
  {
    "id": "us-east",
    "name": "US East",
    "origin": "https://use1.snaketron.io",
    "wsUrl": "wss://use1.snaketron.io/ws"
  },
  {
    "id": "eu-west",
    "name": "Europe",
    "origin": "https://euw1.snaketron.io",
    "wsUrl": "wss://euw1.snaketron.io/ws"
  }
]
```

**Development Mode:**
- Return localhost URLs with different ports
- Example: `http://localhost:8080`, `http://localhost:8081`

**Production Mode:**
- Return regional subdomains
- Read from environment variables or config file

**Implementation:** `server/src/api/regions.rs`

### 2. GET `/api/regions/user-counts`

**Purpose:** Return aggregated user counts per region from Redis

**Response:**
```json
{
  "us-east": 1247,
  "eu-west": 2341
}
```

**Redis Schema:**
```
Key: server:{server_id}:user_count
Value: <number of active WebSocket connections>
TTL: 10 seconds

Key: server:{server_id}:region
Value: <region_id>
TTL: none (persistent)
```

**Algorithm:**
1. Query all keys matching `server:*:user_count`
2. For each server, get its count and region
3. Aggregate counts by region
4. Return map of region_id → total_count

**Update Frequency:**
- Each WebSocket server updates its count every 5 seconds
- Keys have 10s TTL as safety mechanism (auto-cleanup dead servers)

**Implementation:** `server/src/api/regions.rs`

### 3. GET `/api/health`

**Purpose:** Simple health check endpoint for client-side ping measurement

**Response:**
```json
{
  "status": "ok"
}
```

**Characteristics:**
- Must be fast (< 10ms processing time)
- No database queries
- No authentication required
- CORS enabled for cross-origin ping tests

**Implementation:** `server/src/api/regions.rs`

## WebSocket Context Updates

**File:** `client/web/contexts/WebSocketContext.tsx`

### New API

```typescript
export interface WebSocketContextType {
  // ... existing fields
  connectToRegion: (wsUrl: string) => void;
  currentRegionUrl: string | null;
}
```

### Behavior

- `connectToRegion(wsUrl)` closes existing connection and opens new one
- Called by App.tsx when selected region changes
- Handles authentication token on reconnection

## UI Component Updates

### App.tsx Changes

**Remove:**
- Hard-coded region test data (lines 32-36)
- Manual region state management

**Add:**
```typescript
function Header() {
  const { regions, selectedRegion, selectRegion, isLoading } = useRegions();
  const { connectToRegion } = useWebSocket();

  // Auto-connect when selected region changes
  useEffect(() => {
    if (selectedRegion) {
      connectToRegion(selectedRegion.wsUrl);
    }
  }, [selectedRegion?.id, connectToRegion]);

  const handleRegionChange = (regionId: string) => {
    selectRegion(regionId);
  };

  return (
    <header>
      {/* ... */}
      {!isLoading && selectedRegion && (
        <RegionSelector
          regions={regions}
          currentRegionId={selectedRegion.id}
          onRegionChange={handleRegionChange}
        />
      )}
    </header>
  );
}
```

### RegionSelector.tsx

**No changes required** - component remains presentational

## Server-Side Implementation

### Redis User Count Tracking

**File:** `server/src/ws_server.rs` (or new `server/src/metrics.rs`)

```rust
// Background task that runs every 5 seconds
pub fn spawn_metrics_updater(
    redis_pool: RedisPool,
    server_id: String,
    region: String,
    connection_count: Arc<AtomicUsize>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            let count = connection_count.load(Ordering::Relaxed);

            // Update Redis with current connection count
            if let Err(e) = update_redis_metrics(&redis_pool, &server_id, &region, count).await {
                error!("Failed to update Redis metrics: {}", e);
            }
        }
    });
}

async fn update_redis_metrics(
    pool: &RedisPool,
    server_id: &str,
    region: &str,
    count: usize,
) -> Result<()> {
    let mut conn = pool.get().await?;

    // Set user count with TTL
    redis::cmd("SETEX")
        .arg(format!("server:{}:user_count", server_id))
        .arg(10)  // 10 second TTL
        .arg(count)
        .query_async(&mut conn)
        .await?;

    // Set region (no TTL)
    redis::cmd("SET")
        .arg(format!("server:{}:region", server_id))
        .arg(region)
        .query_async(&mut conn)
        .await?;

    Ok(())
}
```

### Server Configuration

Each server instance needs to know its region:

**Environment Variable:**
```bash
SNAKETRON_REGION=us-east
```

**Config File (optional):**
```toml
[server]
region = "us-east"
origin = "https://use1.snaketron.io"
ws_url = "wss://use1.snaketron.io/ws"
```

## Implementation Roadmap

### Phase 1: Backend API Foundation (2-3 hours)
- [ ] Create `server/src/api/regions.rs`
- [ ] Implement `/api/health` endpoint
- [ ] Implement `/api/regions` endpoint with hard-coded test data
- [ ] Add region configuration via environment variable
- [ ] Test endpoints with curl

**Success Criteria:**
- `curl http://localhost:8080/api/health` returns `{"status":"ok"}`
- `curl http://localhost:8080/api/regions` returns region list

### Phase 2: Redis User Count Tracking (2-3 hours)
- [ ] Add `spawn_metrics_updater()` to WebSocket server startup
- [ ] Track active connection count with `Arc<AtomicUsize>`
- [ ] Implement Redis update logic (every 5s)
- [ ] Implement `/api/regions/user-counts` endpoint
- [ ] Test with multiple server instances

**Success Criteria:**
- Redis contains `server:*:user_count` keys
- Counts update every 5 seconds
- API endpoint aggregates counts correctly

### Phase 3: Custom Hook Implementation (3-4 hours)
- [ ] Create `client/web/hooks/useRegions.ts`
- [ ] Implement region fetching
- [ ] Implement ping measurement (parallel)
- [ ] Implement user count fetching
- [ ] Add auto-selection logic
- [ ] Add localStorage persistence
- [ ] Add periodic user count refresh (5s interval)
- [ ] Add error handling and loading states

**Success Criteria:**
- Hook initializes with real data
- Ping measurement completes in < 2s
- Auto-selection works correctly
- User counts refresh every 5 seconds

### Phase 4: WebSocket & UI Integration (2 hours)
- [ ] Update `WebSocketContext` with `connectToRegion()`
- [ ] Update `App.tsx` to use `useRegions()` hook
- [ ] Remove hard-coded region data
- [ ] Add loading state UI
- [ ] Test region switching flow
- [ ] Test WebSocket reconnection

**Success Criteria:**
- Region selector shows live data
- Selecting a region reconnects WebSocket
- User counts update in real-time
- localStorage persists selection

### Phase 5: Testing & Polish (2-3 hours)
- [ ] Test with multiple browser tabs
- [ ] Test localStorage across sessions
- [ ] Test with network failures (offline regions)
- [ ] Add retry logic for failed API calls
- [ ] Add TypeScript type checking
- [ ] Load testing for user count aggregation
- [ ] Documentation updates

**Success Criteria:**
- All TypeScript compiles without errors
- Handles edge cases gracefully
- Performance is acceptable (< 2s initial load)

**Total Estimated Time:** 11-15 hours

## Trade-offs & Design Decisions

### ✅ Client-Side Ping Measurement
**Decision:** Measure latency from browser to each region
**Rationale:** More accurate than server-side GeoIP estimation
**Trade-off:** Requires CORS and adds ~1-2s to initial load

### ✅ Periodic User Count Refresh
**Decision:** Refresh every 5 seconds (matches server update frequency)
**Rationale:** Real-time data is core feature
**Trade-off:** 1 API call every 5 seconds per active user

### ✅ localStorage Persistence
**Decision:** Remember user's region preference
**Rationale:** Better UX for returning users
**Trade-off:** Users might stick with sub-optimal region if network changes

### ✅ Parallel Ping Measurement
**Decision:** Ping all regions simultaneously
**Rationale:** Minimizes total initialization time
**Trade-off:** More network requests at once (acceptable for 2-5 regions)

### ❌ Server-Side Latency Estimation (Rejected)
**Alternative:** Use GeoIP to estimate user location and assign region
**Rejected Because:** Less accurate, doesn't account for routing/CDN

### ❌ WebSocket for User Counts (Rejected)
**Alternative:** Stream user count updates via WebSocket
**Rejected Because:** HTTP polling is simpler and sufficient for 5s updates

## Configuration

### Development Environment

```bash
# Server 1 (US region)
SNAKETRON_REGION=us-east
PORT=8080

# Server 2 (EU region)
SNAKETRON_REGION=eu-west
PORT=8081
```

### Production Environment

```bash
# Environment variables
SNAKETRON_REGION=us-east
SNAKETRON_ORIGIN=https://use1.snaketron.io
SNAKETRON_WS_URL=wss://use1.snaketron.io/ws
```

## Testing Checklist

### Unit Tests
- [ ] `useRegions` hook with mocked fetch
- [ ] Ping measurement with timeout
- [ ] Auto-selection algorithm
- [ ] localStorage persistence

### Integration Tests
- [ ] Full flow: fetch → ping → select → connect
- [ ] Region switching with WebSocket reconnection
- [ ] User count refresh cycle
- [ ] Error handling (network failure)

### Manual Tests
- [ ] Multiple regions display correctly
- [ ] Ping values are reasonable (< 500ms)
- [ ] User counts update in real-time
- [ ] Manual region switch works
- [ ] localStorage persists across sessions
- [ ] Works with 2+ browser tabs

## Future Enhancements

1. **Region Health Monitoring**
   - Mark regions as degraded/unavailable
   - Auto-failover to next-best region

2. **Advanced Metrics**
   - Average game latency per region
   - Server capacity/load percentage
   - Queue wait times

3. **Smart Re-Selection**
   - Suggest region switch if ping improves significantly
   - Auto-switch if current region becomes unavailable

4. **Region-Specific Features**
   - Different game modes per region
   - Regional leaderboards
   - Language/timezone preferences

## References

- UI Component: `client/web/components/RegionSelector.tsx`
- Type Definitions: `client/web/types.ts`
- Design Discussion: Session summary from 2025-10-05
