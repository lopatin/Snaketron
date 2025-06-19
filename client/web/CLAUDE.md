# Snaketron Web Client

## Introduction
- It is written in React with TypeScript and uses the WebAssembly library built from the Snaketron Rust code.

## Look and Feel
- Classy, modern, minimalist design. Black on white with a touch of color.
- For example, the logo is a styleized SNAKETRON in bold, italics.
- Please adhere to this design when creating new components.

## User Stories

### 

## Development

### Technology Stack
- React 18 with TypeScript
- React Router v7 for navigation
- WebAssembly (wasm-snaketron) for game logic
- WebSocket for real-time game communication
- Webpack 5 with Babel for bundling

### Guidelines
- Before considering a task done, ensure that the Rust and React TypeScript code compiles without errors.
- All TypeScript files should have proper type annotations
- Use strict TypeScript settings for type safety

### Prerequisites
```
npm install
```

### Building the WebAssembly Library
```
cd client
wasm-pack build --target web
```

### Building the Web Client
```
cd client/web
npm run build
```

### Running the Client (Development Mode)
```
cd client/web
npm run start
```

The client will be available at http://localhost:3000 and will connect to the WebSocket server at ws://localhost:8080.

### Type Checking
To check TypeScript types without building:
```
npx tsc --noEmit
```