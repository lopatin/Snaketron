// Global type declarations
interface WebSocketMonitor {
  messages: any[];
  connections: any[];
  // Add other properties as needed
}

declare global {
  interface Window {
    __wsMonitor?: WebSocketMonitor;
  }
}

export {}; // This file needs to be a module