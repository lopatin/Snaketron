# Team Match Specification

## Overview
Team Match is a competitive game mode where two teams compete to reach the opponent's goal. This includes 1v1 duels (one player per team) and multi-player team games.

## Arena Layout

### Three-Zone Structure
The team match arena consists of three distinct zones arranged horizontally:
1. **Team A End Zone** (Left) - 5 cells deep
2. **Main Playing Field** - Square arena (X by X cells)
3. **Team B End Zone** (Right) - 5 cells deep

Total arena dimensions: `(X + 10) × X` cells where X is the main field size.

### End Zones
Each end zone features:
- **Depth**: 5 cells from the edge
- **Visual Design**: 
  - Background: Light tint of the team's snake color
  - Team name/number displayed in large white letters (like football field end zones)
- **Function**: Protected scoring area with restricted access

### Goals and Walls
Between each end zone and the main field:
- **Goal Opening**: 
  - Located in the center of the boundary
  - Width: 20% of the edge length (rounded to nearest odd number for symmetry)
  - No barriers - snakes can pass through freely
- **Walls**: 
  - Cover the remaining 80% of the boundary
  - Impassable barriers that kill snakes on contact
  - Visually distinct from regular arena boundaries

## Data Structure Changes

### Arena Enhancements
```rust
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Arena {
    pub width: u16,
    pub height: u16,
    pub snakes: Vec<Snake>,
    pub food: Vec<Position>,
    pub team_zone_config: Option<TeamZoneConfig>, // New field - minimal state
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct TeamZoneConfig {
    pub end_zone_depth: u16,  // Depth of each end zone (5 cells)
    pub goal_width: u16,       // Width of goal opening in cells
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum TeamId {
    TeamA,
    TeamB,
}
```

### Derived Boundaries
All zone boundaries are calculated from the minimal `TeamZoneConfig`:

```rust
impl Arena {
    /// Calculate Team A end zone bounds (left side)
    pub fn team_a_zone_bounds(&self) -> Option<(i16, i16, i16, i16)> {
        self.team_zone_config.as_ref().map(|config| {
            (0, config.end_zone_depth as i16 - 1, 0, self.height as i16 - 1)
        })
    }
    
    /// Calculate Team B end zone bounds (right side)
    pub fn team_b_zone_bounds(&self) -> Option<(i16, i16, i16, i16)> {
        self.team_zone_config.as_ref().map(|config| {
            let x_start = self.width as i16 - config.end_zone_depth as i16;
            (x_start, self.width as i16 - 1, 0, self.height as i16 - 1)
        })
    }
    
    /// Calculate main field bounds
    pub fn main_field_bounds(&self) -> Option<(i16, i16)> {
        self.team_zone_config.as_ref().map(|config| {
            (config.end_zone_depth as i16, 
             self.width as i16 - config.end_zone_depth as i16 - 1)
        })
    }
    
    /// Calculate goal position for a given team
    pub fn goal_bounds(&self, team: TeamId) -> Option<(i16, i16, i16)> {
        self.team_zone_config.as_ref().map(|config| {
            let goal_center = self.height as i16 / 2;
            let half_width = config.goal_width as i16 / 2;
            let y_start = goal_center - half_width;
            let y_end = goal_center + half_width;
            
            let x_pos = match team {
                TeamId::TeamA => config.end_zone_depth as i16 - 1,  // Right edge of Team A zone
                TeamId::TeamB => self.width as i16 - config.end_zone_depth as i16,  // Left edge of Team B zone
            };
            
            (x_pos, y_start, y_end)
        })
    }
    
    /// Check if a position is within a wall (not in goal opening)
    pub fn is_wall_position(&self, pos: &Position) -> bool {
        if let Some(config) = &self.team_zone_config {
            // Check if at zone boundary
            let at_team_a_boundary = pos.x == config.end_zone_depth as i16 - 1;
            let at_team_b_boundary = pos.x == self.width as i16 - config.end_zone_depth as i16;
            
            if at_team_a_boundary || at_team_b_boundary {
                // Check if within goal opening
                if let Some((_x, y_start, y_end)) = self.goal_bounds(
                    if at_team_a_boundary { TeamId::TeamA } else { TeamId::TeamB }
                ) {
                    return pos.y < y_start || pos.y > y_end;
                }
            }
        }
        false
    }
}
```

### Snake Team Assignment
```rust
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Snake {
    pub body: Vec<Position>,
    pub direction: Direction,
    pub is_alive: bool,
    pub food: u32,
    pub team_id: Option<TeamId>, // New field
}
```

## Game Mechanics

### Collision Detection

#### Wall Collisions
- Snakes die when hitting walls between zones
- Wall segments are checked before regular boundary checks
- Walls exist at zone boundaries except for goal openings

#### Goal Detection
- When a snake's head enters the opponent's goal area
- Victory condition: First snake to reach enemy goal wins for their team
- Game ends immediately upon goal entry

### Zone Transitions
- Snakes can only enter end zones through goals
- Movement within end zones follows normal rules
- Food can spawn in any zone including end zones

### Team Assignment
- Players are assigned to teams alternately (A, B, A, B...)
- In 1v1: One player per team
- In team games: Balanced distribution

### Starting Positions
- Team A snakes start in left portion of main field
- Team B snakes start in right portion of main field
- Snakes face toward center initially
- No snakes start in end zones

## Rendering

### Visual Hierarchy
1. **Background Layer**: Grid dots
2. **Zone Layer**: End zone colored backgrounds
3. **Wall Layer**: Boundary walls (dark gray/black)
4. **Text Layer**: Team names in end zones
5. **Game Layer**: Food, snakes
6. **Overlay Layer**: UI elements

### End Zone Rendering
```javascript
// Pseudo-code for end zone rendering - derives boundaries from minimal config
function renderEndZone(ctx, arena, team, cellSize) {
    if (!arena.team_zone_config) return;
    
    const config = arena.team_zone_config;
    const teamColors = getTeamColors(team); // From game config, not arena
    
    // Calculate zone bounds
    let x_start, x_end;
    if (team === 'TeamA') {
        x_start = 0;
        x_end = config.end_zone_depth;
    } else {
        x_start = arena.width - config.end_zone_depth;
        x_end = arena.width;
    }
    
    // Fill background with light team color
    ctx.fillStyle = teamColors.light;
    ctx.fillRect(
        x_start * cellSize,
        0,
        (x_end - x_start) * cellSize,
        arena.height * cellSize
    );
    
    // Draw team name
    ctx.fillStyle = 'white';
    ctx.font = `${cellSize * 2}px bold italic sans-serif`;
    ctx.textAlign = 'center';
    ctx.fillText(
        team === 'TeamA' ? 'TEAM A' : 'TEAM B',
        (x_start + x_end) / 2 * cellSize,
        arena.height / 2 * cellSize
    );
}
```

### Wall Rendering
```javascript
function renderWalls(ctx, arena, cellSize) {
    if (!arena.team_zone_config) return;
    
    const config = arena.team_zone_config;
    ctx.fillStyle = '#2a2a2a';
    
    // Calculate goal bounds
    const goal_center = arena.height / 2;
    const half_width = Math.floor(config.goal_width / 2);
    const goal_y_start = goal_center - half_width;
    const goal_y_end = goal_center + half_width;
    
    // Render Team A boundary walls (right edge of Team A zone)
    const teamA_wall_x = config.end_zone_depth - 1;
    
    // Top wall segment (before goal)
    if (goal_y_start > 0) {
        ctx.fillRect(
            teamA_wall_x * cellSize,
            0,
            cellSize,
            goal_y_start * cellSize
        );
    }
    
    // Bottom wall segment (after goal)
    if (goal_y_end < arena.height - 1) {
        ctx.fillRect(
            teamA_wall_x * cellSize,
            (goal_y_end + 1) * cellSize,
            cellSize,
            (arena.height - goal_y_end - 1) * cellSize
        );
    }
    
    // Render Team B boundary walls (left edge of Team B zone)
    const teamB_wall_x = arena.width - config.end_zone_depth;
    
    // Top wall segment (before goal)
    if (goal_y_start > 0) {
        ctx.fillRect(
            teamB_wall_x * cellSize,
            0,
            cellSize,
            goal_y_start * cellSize
        );
    }
    
    // Bottom wall segment (after goal)
    if (goal_y_end < arena.height - 1) {
        ctx.fillRect(
            teamB_wall_x * cellSize,
            (goal_y_end + 1) * cellSize,
            cellSize,
            (arena.height - goal_y_end - 1) * cellSize
        );
    }
}
```

## Implementation Phases

### Phase 1: Data Structures
1. Extend Arena with minimal `TeamZoneConfig`
2. Add team assignment to snakes
3. Update serialization/deserialization
4. Add helper methods for deriving boundaries

### Phase 2: Game Logic
1. Implement wall collision detection
2. Add goal detection and victory condition
3. Modify starting position logic for teams

### Phase 3: Rendering
1. Render end zone backgrounds
2. Add team names/numbers
3. Draw walls with goal openings

### Phase 4: Testing & Polish
1. Test collision detection edge cases
2. Verify goal scoring
3. Balance team starting positions
4. Optimize rendering performance

## Configuration

### Default Settings
```rust
pub struct TeamMatchConfig {
    pub main_field_size: u16,      // Default: 30
    pub end_zone_depth: u16,        // Fixed: 5
    pub goal_width_percent: f32,    // Fixed: 0.20 (20%)
}

// Team colors are stored in GameType or rendering config, not Arena
pub fn get_team_colors(team: TeamId) -> (String, String, String) {
    match team {
        TeamId::TeamA => (
            "#70bfe3".to_string(),  // Primary
            "#e6f4fa".to_string(),  // Light background
            "#5299bb".to_string(),  // Border
        ),
        TeamId::TeamB => (
            "#ff6b6b".to_string(),  // Primary
            "#ffe6e6".to_string(),  // Light background
            "#b84444".to_string(),  // Border
        ),
    }
}
```

### Team Colors
- **Team A (Blue)**:
  - Primary: `#70bfe3`
  - Light: `#e6f4fa`
  - Border: `#5299bb`
  
- **Team B (Red)**:
  - Primary: `#ff6b6b`
  - Light: `#ffe6e6`
  - Border: `#b84444`

## Victory Conditions

### Goal Scoring
- Game ends when any snake reaches the opponent's goal
- The team of the scoring snake wins
- All team members share the victory

### Elimination
- If all snakes of one team die, the other team wins
- Individual snake deaths don't end the game in team mode

## Future Enhancements

### Orientation Flexibility
- Support for vertical orientation (top/bottom end zones)
- Configurable based on display aspect ratio

### Advanced Features
- Multiple goals per side
- Varying goal sizes
- Power-ups in end zones
- Defensive zones with special rules

### Visual Enhancements
- Animated team names
- Goal celebration effects
- Trail effects for scoring runs
- Dynamic camera for goal approaches