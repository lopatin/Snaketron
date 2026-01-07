# Network Latency Testing Guide

## Overview
I've added a configurable artificial network delay feature to the SnakeTron client that allows you to simulate different network conditions for testing purposes.

## How to Access
1. Look for the settings gear icon (⚙️) in the header, next to the region selector
2. Click the icon to open the Latency Settings panel

## Features
- **Enable/Disable Toggle**: Turn artificial latency on/off
- **Send Delay**: Adds delay to messages sent from client to server (0-1000ms)
- **Receive Delay**: Adds delay to messages received from server (0-1000ms)
- **Real Latency Display**: Shows actual network latency alongside artificial settings
- **Persistent Settings**: Your settings are saved in localStorage

## Usage Examples

### Testing High Latency (Poor Connection)
- Send Delay: 150ms
- Receive Delay: 150ms
- Total RTT: 300ms

### Testing Asymmetric Connection
- Send Delay: 50ms
- Receive Delay: 200ms
- Simulates upload/download speed differences

### Testing Jitter
- Manually adjust the sliders during gameplay to simulate unstable connections

## Implementation Details
- The artificial delay is applied in the WebSocketContext
- Send delays are applied before `ws.send()`
- Receive delays are applied before message processing
- Clock sync and latency measurements still work correctly

## Important Notes
- Artificial delays are added ON TOP of real network latency
- The settings persist across sessions (stored in localStorage)
- Delays apply to ALL WebSocket messages (game commands, updates, etc.)