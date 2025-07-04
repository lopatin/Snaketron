#!/bin/bash
# Development startup script that builds the web client and starts the server

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Building web client...${NC}"

# Build WASM module
cd /usr/src/app/client
wasm-pack build --target web --out-dir pkg --dev

# Build React app in development mode
cd /usr/src/app/client/web
if [ ! -d "node_modules" ]; then
    echo -e "${YELLOW}Installing npm dependencies...${NC}"
    npm install
else
    # Check if lightningcss is properly installed for this architecture
    if ! npm ls lightningcss >/dev/null 2>&1; then
        echo -e "${YELLOW}Reinstalling dependencies due to architecture mismatch...${NC}"
        rm -rf node_modules package-lock.json
        npm install
    fi
fi

npm run build

echo -e "${GREEN}Web client built successfully!${NC}"

# Create web directory and copy built files
mkdir -p /app/web
cp -r /usr/src/app/client/web/dist/* /app/web/

# Set environment variable for web directory
export SNAKETRON_WEB_DIR=/app/web

echo -e "${GREEN}Starting server with cargo-watch...${NC}"
cd /usr/src/app

# Start the server with cargo-watch
exec cargo watch -x "run --bin server" -w server -w common -w macros