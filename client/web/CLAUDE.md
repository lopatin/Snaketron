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
npm run type-check
```

## TypeScript Guidelines

### Essential Information
- **Strict Mode**: The project uses TypeScript strict mode. All variables, parameters, and return types should be explicitly typed.
- **Type Definitions**: Custom types are centralized in `types/index.ts`. Import and use these types instead of creating duplicates.
- **WASM Integration**: The `wasm-snaketron` module types are defined in `wasm-snaketron.d.ts`. Note that `render_game` expects a JSON string, not an object.
- **React Components**: Use functional components with proper prop interfaces. Component props should be defined as interfaces in the types file.
- **Event Handlers**: Use React's built-in event types (e.g., `React.FormEvent`, `React.ChangeEvent<HTMLInputElement>`).
- **State Management**: When using `useState` with nullable values, use union types like `string | null` instead of just `null`.
- **WebSocket Context**: The WebSocket context is fully typed. Use the `WebSocketContextType` interface when consuming it.

### Common Patterns
```typescript
// Component with props
interface MyComponentProps {
  onClose: () => void;
  isOpen: boolean;
}

const MyComponent: React.FC<MyComponentProps> = ({ onClose, isOpen }) => {
  // ...
};

// Typed state
const [error, setError] = useState<string | null>(null);

// Event handlers
const handleSubmit: React.FormEventHandler = (e) => {
  e.preventDefault();
  // ...
};
```