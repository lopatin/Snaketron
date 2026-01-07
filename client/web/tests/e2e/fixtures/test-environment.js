const { execSync, spawn } = require('child_process');
const { setTimeout } = require('timers/promises');

class TestEnvironment {
  constructor() {
    this.dockerComposeProcess = null;
    this.serverUrl = 'ws://localhost:8080/ws';
    this.appUrl = 'http://localhost:3000';
    this.dockerComposeCommand = 'docker-compose';
    this.maxRetries = 30; // 30 seconds total
    this.retryDelay = 1000; // 1 second between retries
  }

  async setup() {
    console.log('Setting up test environment...');
    
    // Check if Docker is running
    await this.ensureDockerRunning();
    
    // Check if server is already running or start it
    await this.ensureServerRunning();
    
    // The React dev server is started by Playwright's webServer config
    console.log('Test environment ready!');
  }

  async ensureDockerRunning() {
    try {
      execSync('docker info', { stdio: 'ignore' });
      console.log('Docker is running');
    } catch (error) {
      throw new Error('Docker is not running. Please start Docker and try again.');
    }
  }

  async ensureServerRunning() {
    // First check if server is already accessible
    if (await this.isServerHealthy()) {
      console.log('Server is already running and healthy');
      return;
    }

    console.log('Server not accessible, checking Docker containers...');
    
    // Check if containers are running
    try {
      const result = execSync(`${this.dockerComposeCommand} ps -q`, { 
        cwd: process.cwd() + '/../..',
        encoding: 'utf-8' 
      });
      
      if (!result.trim()) {
        console.log('Docker containers not running, starting them...');
        await this.startDockerContainers();
      } else {
        console.log('Docker containers are running, waiting for server health...');
      }
    } catch (error) {
      console.log('Docker compose check failed, attempting to start containers...');
      await this.startDockerContainers();
    }

    // Wait for server to be healthy
    await this.waitForServer();
  }

  async startDockerContainers() {
    console.log('Starting Docker containers with docker-compose up -d...');
    
    try {
      execSync(`${this.dockerComposeCommand} up -d`, {
        cwd: process.cwd() + '/../..',
        stdio: 'inherit'
      });
      
      console.log('Docker containers started');
    } catch (error) {
      throw new Error(`Failed to start Docker containers: ${error.message}`);
    }
  }

  async isServerHealthy() {
    try {
      // Try to connect with a simple HTTP request first
      const http = require('http');
      
      return new Promise((resolve) => {
        const options = {
          hostname: 'localhost',
          port: 8080,
          path: '/ws',
          method: 'GET',
          timeout: 2000
        };
        
        const req = http.request(options, (res) => {
          // Any response means server is running
          resolve(true);
        });
        
        req.on('error', () => {
          resolve(false);
        });
        
        req.on('timeout', () => {
          req.destroy();
          resolve(false);
        });
        
        req.end();
      });
    } catch (error) {
      return false;
    }
  }

  async waitForServer() {
    console.log('Waiting for server to be healthy...');
    
    for (let i = 0; i < this.maxRetries; i++) {
      if (await this.isServerHealthy()) {
        console.log('Server is healthy!');
        return;
      }
      
      if (i < this.maxRetries - 1) {
        console.log(`Server not ready yet, retrying in ${this.retryDelay}ms... (${i + 1}/${this.maxRetries})`);
        await setTimeout(this.retryDelay);
      }
    }
    
    throw new Error('Server did not become healthy within the timeout period');
  }

  async teardown() {
    // We don't stop Docker containers in teardown
    // They should keep running for other tests
    console.log('Test environment teardown complete');
  }

  getServerUrl() {
    return this.serverUrl;
  }

  getAppUrl() {
    return this.appUrl;
  }
}

module.exports = { TestEnvironment };