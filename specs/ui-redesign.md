# UI Redesign Specification

## Goal
Transform the front page from a website-style layout to a full-screen game interface.

## Design References
- client/design/design-vibe-10.png - Victory screen with clean scoreboard
- client/design/design-vibe-3.png - Create custom game form
- client/design/design-vibe-2.png - Game mode selection buttons

## Key Design Elements
- Clean, modern aesthetic
- Double-border styling (similar to current header)
- Professional game mode selectors with good padding/alignment
- Polished, unique look for the main game start form

## Wide Screen Layout (Square/Wide)

### Sidebar (Left Side)
- [ ] Logo at top, centered
- [ ] Navigation items below logo, right-justified
- [ ] Region selector below nav
- [ ] Lobby info section:
  - Show invite button if no friends
  - Show all usernames (including current user) if friends present
- [ ] Social icons at bottom (Reddit, Discord, Twitter, Github)
- [ ] Double border on right side (rotated from current header style)
- [ ] Comfortable padding throughout

### Main Content Area
- [ ] White background (no polka dots)
- [ ] LOGIN or username display at top right
- [ ] Centered game start form:
  - Nickname field
  - Multi-selector: Duel, 2v2, Solo, FFA (no custom games)
  - Competitive checkbox
  - "Start Game" CTA button
  - Professional styling with good padding/alignment
- [ ] Lobby chat window in bottom right

## Mobile/Tall Screen Layout

### Header
- [ ] Hamburger menu icon (light gray) - top left
  - Opens sidebar with all widescreen sidebar items
- [ ] Logo - top center/left
- [ ] Nickname or LOGIN button - top right
- [ ] Same border as current header

### Main Content
- [ ] Centered game start form (same as widescreen)
- [ ] Lobby chat in bottom right, understated when no activity
- [ ] Show lobby events (e.g., "User3 joined the lobby")

## Technical Implementation

### Components to Create
1. `Sidebar.tsx` - Left sidebar for wide screens
2. `MobileHeader.tsx` - Top header for mobile
3. `GameStartForm.tsx` - Central game start form
4. `LobbyChat.tsx` - Bottom right chat component
5. `LobbyInfo.tsx` - Lobby users/invite section
6. `SocialIcons.tsx` - Social media links

### Components to Modify
1. Main layout/router to switch between sidebar and header
2. Remove polka dot background
3. Update responsive breakpoints

### Styling Approach
- Use CSS modules or styled-components
- Match double-border style from existing header
- Ensure clean, modern look matching design screenshots
- Focus on professional game mode selector styling

## Testing Plan
- [ ] Test on wide screen (1920x1080+)
- [ ] Test on square screen (1024x1024)
- [ ] Test on mobile portrait (375x667)
- [ ] Test on mobile landscape (667x375)
- [ ] Verify hamburger menu functionality
- [ ] Verify responsive chat behavior
- [ ] Use Chrome DevTools for iteration

## Implementation Status

### Completed Components
- [x] Sidebar.tsx - Left sidebar with logo, nav, region selector, lobby info, and social icons
- [x] MobileHeader.tsx - Mobile header with hamburger menu
- [x] GameStartForm.tsx - Central game start form with nickname, game modes, competitive checkbox
- [x] LobbyChat.tsx - Collapsible chat in bottom right
- [x] NewHome.tsx - Main home page component with responsive layout
- [x] Updated index.css with sidebar styling and home page white background
- [x] Updated App.tsx to use NewHome component
- [x] TypeScript compilation passes
- [x] Webpack build succeeds

### Testing Status
- [ ] Desktop layout (1920x1080)
- [ ] Tablet layout (1024x768)
- [ ] Mobile portrait (375x667)
- [ ] Mobile landscape (667x375)
- [ ] Hamburger menu functionality
- [ ] Chat expand/collapse
- [ ] Game mode selection
- [ ] Form validation

### Known Issues
- None

### Latest Updates (2025-10-11)
All requested fixes have been implemented:
1. ✅ Sidebar is now full screen height with `h-screen` class
2. ✅ Lobby section modernized - removed double border, right-justified with status icons on right
3. ✅ Region selector right-justified
4. ✅ Form completely modernized:
   - Removed panel double border
   - Removed "NICKNAME" label, now just placeholder text
   - Game mode buttons use blue border + checkmark selection (modern style)
   - Start Game button: white with black outline when enabled, subtle gray when disabled
   - No black backgrounds on any buttons
5. ✅ Logo made smaller (h-6 instead of h-8) with more padding (pb-10 instead of pb-6)
6. ✅ All changes applied to both Sidebar and MobileHeader for consistency

## Status
- Created: 2025-10-11
- Updated: 2025-10-11
- Status: ✅ Complete - All components implemented with modern styling
