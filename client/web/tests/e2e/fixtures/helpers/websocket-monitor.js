class WebSocketMonitor {
  constructor(page) {
    this.page = page;
    this.messages = [];
    this.connectionState = 'disconnected';
  }

  async setup() {
    // Inject WebSocket monitoring into the page
    await this.page.addInitScript(() => {
      window.__wsMonitor = {
        sent: [],
        received: [],
        connections: [],
        state: 'disconnected'
      };

      // Override WebSocket constructor
      const OriginalWebSocket = window.WebSocket;
      window.WebSocket = class extends OriginalWebSocket {
        constructor(...args) {
          super(...args);
          
          const url = args[0];
          const connection = {
            url: url,
            openedAt: null,
            closedAt: null,
            messages: { sent: [], received: [] }
          };
          
          // Only monitor game server WebSocket connections
          const isGameServer = url.includes(':8080');
          if (isGameServer) {
            window.__wsMonitor.connections.push(connection);
          }

          // Monitor connection state
          this.addEventListener('open', () => {
            if (isGameServer) {
              window.__wsMonitor.state = 'connected';
              connection.openedAt = Date.now();
            }
          });

          this.addEventListener('close', () => {
            if (isGameServer) {
              window.__wsMonitor.state = 'disconnected';
              connection.closedAt = Date.now();
            }
          });

          this.addEventListener('error', () => {
            if (isGameServer) {
              window.__wsMonitor.state = 'error';
            }
          });

          // Monitor received messages
          this.addEventListener('message', (event) => {
            if (!isGameServer) return;
            
            const message = {
              type: 'received',
              data: event.data,
              timestamp: Date.now()
            };
            
            try {
              const parsed = JSON.parse(event.data);
              message.parsed = parsed;
              // Extract message type from enum-style format
              const messageType = Object.keys(parsed)[0];
              message.messageType = messageType || 'unknown';
            } catch (e) {
              message.messageType = 'raw';
            }
            
            window.__wsMonitor.received.push(message);
            connection.messages.received.push(message);
          });

          // Override send method
          const originalSend = this.send.bind(this);
          this.send = function(data) {
            if (isGameServer) {
              const message = {
                type: 'sent',
                data: data,
                timestamp: Date.now()
              };
              
              try {
                const parsed = JSON.parse(data);
                message.parsed = parsed;
                // Extract message type from enum-style format
                const messageType = Object.keys(parsed)[0];
                message.messageType = messageType || 'unknown';
              } catch (e) {
                message.messageType = 'raw';
              }
              
              window.__wsMonitor.sent.push(message);
              connection.messages.sent.push(message);
            }
            
            return originalSend(data);
          };
        }
      };
    });
  }

  async waitForConnection(timeout = 10000) {
    return await this.page.waitForFunction(
      () => window.__wsMonitor?.state === 'connected',
      { timeout }
    );
  }

  async waitForMessage(messageType, direction = 'received', timeout = 5000) {
    const message = await this.page.waitForFunction(
      (args) => {
        const messages = window.__wsMonitor?.[args.direction] || [];
        return messages.find(m => m.messageType === args.messageType);
      },
      { messageType, direction },
      { timeout }
    );
    
    return await message.jsonValue();
  }

  async waitForMessageMatching(predicate, direction = 'received', timeout = 5000) {
    const message = await this.page.waitForFunction(
      (args) => {
        const messages = window.__wsMonitor?.[args.direction] || [];
        const predicateFn = new Function('message', `return ${args.predicateStr}`);
        return messages.find(m => predicateFn(m));
      },
      { direction, predicateStr: predicate.toString() },
      { timeout }
    );
    
    return await message.jsonValue();
  }

  async getLastMessage(direction = 'received') {
    return await this.page.evaluate((dir) => {
      const messages = window.__wsMonitor?.[dir] || [];
      return messages[messages.length - 1] || null;
    }, direction);
  }

  async getAllMessages(direction = 'both') {
    return await this.page.evaluate((dir) => {
      if (dir === 'both') {
        return {
          sent: window.__wsMonitor?.sent || [],
          received: window.__wsMonitor?.received || []
        };
      }
      return window.__wsMonitor?.[dir] || [];
    }, direction);
  }

  async clearMessages() {
    await this.page.evaluate(() => {
      if (window.__wsMonitor) {
        window.__wsMonitor.sent = [];
        window.__wsMonitor.received = [];
      }
    });
  }

  async getConnectionState() {
    return await this.page.evaluate(() => window.__wsMonitor?.state || 'unknown');
  }

  async assertMessageSent(messageType, payload = null, timeout = 5000) {
    const message = await this.waitForMessage(messageType, 'sent', timeout);
    
    if (payload !== null) {
      const actualPayload = message.parsed;
      
      // Deep comparison of payloads
      const payloadMatches = JSON.stringify(actualPayload) === JSON.stringify(payload);
      
      if (!payloadMatches) {
        throw new Error(
          `Message payload mismatch for ${messageType}.\n` +
          `Expected: ${JSON.stringify(payload, null, 2)}\n` +
          `Actual: ${JSON.stringify(actualPayload, null, 2)}`
        );
      }
    }
    
    return message;
  }

  async assertMessageReceived(messageType, timeout = 5000) {
    return await this.waitForMessage(messageType, 'received', timeout);
  }

  async debugPrintMessages() {
    const messages = await this.getAllMessages();
    console.log('=== WebSocket Messages ===');
    console.log('Sent:', JSON.stringify(messages.sent, null, 2));
    console.log('Received:', JSON.stringify(messages.received, null, 2));
    console.log('========================');
  }
}

module.exports = { WebSocketMonitor };