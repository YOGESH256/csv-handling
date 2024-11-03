const WebSocket = require('ws');
const amqp = require('amqplib');

class NotificationService {
  constructor() {
    this.clients = new Map();
    this.connection = null;
    this.channel = null;
  }

  async start() {
    // Setup WebSocket server
    this.wss = new WebSocket.Server({ port: process.env.WS_PORT || 8080 });
    
    // Setup RabbitMQ connection
    this.connection = await amqp.connect(process.env.RABBITMQ_URL);
    this.channel = await this.connection.createChannel();
    
    await this.channel.assertQueue('notifications', { durable: true });
    
    this.setupWebSocketHandlers();
    this.consumeNotifications();
  }

  setupWebSocketHandlers() {
    this.wss.on('connection', (ws, req) => {
      const userId = req.headers['user-id'];
      if (!userId) {
        ws.close();
        return;
      }

      this.clients.set(userId, ws);
      
      ws.on('close', () => {
        this.clients.delete(userId);
      });
    });
  }

  async consumeNotifications() {
    await this.channel.consume('notifications', (msg) => {
      if (!msg) return;

      const notification = JSON.parse(msg.content.toString());
      this.notifyClient(notification.userId, notification);
      this.channel.ack(msg);
    });
  }

  notifyClient(userId, message) {
    const ws = this.clients.get(userId);
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(message));
    }
  }
}