# Snaketron Web Client

## Introduction
- It is written in React and uses the WebAssembly library built from the Snaketron Rust code.

## Look and Feel
- Classy, modern, minimalist design. Black on white with a touch of color.
- For example, the logo is a styleized SNAKETRON in bold, italics.
- Please adhere to this design when creating new components.

## User Stories

### 

## Development

### Guidelines
- Before considering a task done, ensure that the Rust and React code compiles without errors.

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

### Running the Client
```
npm run start
```