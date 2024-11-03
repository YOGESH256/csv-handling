const amqp = require('amqplib');
const AWS = require('aws-sdk');
const { Transform } = require('stream');
const mongoose = require('mongoose');
const File = require('../shared/models/File');

class ProcessingService {
  constructor() {
    this.connection = null;
    this.channel = null;
    this.s3 = new AWS.S3();
  }

  async start() {
    // Connect to MongoDB
    await mongoose.connect(process.env.MONGODB_URL);
    
    // Connect to RabbitMQ
    this.connection = await amqp.connect(process.env.RABBITMQ_URL);
    this.channel = await this.connection.createChannel();
    
    await this.setupQueues();
    this.startProcessing();
  }

  async setupQueues() {
    await this.channel.assertQueue('file_processing', {
      durable: true,
      arguments: {
        'x-queue-type': 'classic',
        'x-max-priority': 10
      }
    });

    await this.channel.assertQueue('notifications', { durable: true });
    await this.channel.assertQueue('file_processing_dlq', { durable: true });
    await this.channel.assertQueue('file_processing_retry', {
      durable: true,
      arguments: {
        'x-dead-letter-exchange': '',
        'x-dead-letter-routing-key': 'file_processing',
        'x-message-ttl': 30000
      }
    });
  }

  async startProcessing() {
    await this.channel.consume('file_processing', async (msg) => {
      if (!msg) return;

      try {
        const { fileId, metadata, userId } = JSON.parse(msg.content.toString());
        await this.processFile(fileId, metadata, userId);
        this.channel.ack(msg);
      } catch (error) {
        await this.handleProcessingError(msg, error);
      }
    });
  }

  async processFile(fileId, metadata, userId) {
    // Update status to processing
    await File.findOneAndUpdate(
      { fileId },
      { status: 'processing', updatedAt: new Date() }
    );

    await this.sendNotification(userId, {
      type: 'processing_started',
      fileId,
      status: 'processing'
    });

    const processedUrl = await this.streamFileProcessing(fileId, metadata, userId);

    // Update status to completed
    await File.findOneAndUpdate(
      { fileId },
      {
        status: 'completed',
        processedUrl,
        updatedAt: new Date()
      }
    );

    await this.sendNotification(userId, {
      type: 'processing_completed',
      fileId,
      status: 'completed',
      processedUrl
    });
  }

  async streamFileProcessing(fileId, metadata, userId) {
    return new Promise((resolve, reject) => {
      const CHUNK_SIZE = 64 * 1024; // 64KB chunks
      
      const readStream = this.s3.getObject({
        Bucket: process.env.S3_BUCKET,
        Key: `uploads/${fileId}`,
        RequestOptions: {
          highWaterMark: CHUNK_SIZE
        }
      }).createReadStream();

      let processedBytes = 0;
      
      // Transform stream to handle CSV chunks
      const csvChunkStream = new Transform({
        highWaterMark: CHUNK_SIZE,
        transform: async (chunk, encoding, callback) => {
          try {
            processedBytes += chunk.length;
            
            // Send the CSV chunk directly to the client via notification
            await this.sendNotification(userId, {
              type: 'csv_chunk',
              fileId,
              chunk: chunk.toString(), // Convert buffer to string for CSV data
              bytesProcessed: processedBytes,
              isLastChunk: false
            });
            
            callback();
          } catch (error) {
            callback(error);
          }
        },
        flush: async (callback) => {
          try {
            // Send final notification indicating stream completion
            await this.sendNotification(userId, {
              type: 'csv_chunk',
              fileId,
              bytesProcessed: processedBytes,
              isLastChunk: true
            });
            callback();
          } catch (error) {
            callback(error);
          }
        }
      });

      // Error handling
      readStream.on('error', (error) => {
        console.error('Read stream error:', error);
        reject(error);
      });

      csvChunkStream.on('error', (error) => {
        console.error('CSV chunk stream error:', error);
        reject(error);
      });

      // Pipeline setup
      readStream
        .pipe(csvChunkStream)
        .on('finish', () => {
          resolve({
            totalBytesProcessed: processedBytes,
            status: 'completed'
          });
        });
    });
  }

  async sendNotification(userId, message) {
    // Ensure the channel exists and is ready
    if (!this.channel) {
      throw new Error('RabbitMQ channel not initialized');
    }

    // If the chunk is too large for a single message, you might need to fragment it
    const MAX_MESSAGE_SIZE = 128 * 1024; // 128KB max message size for RabbitMQ
    
    if (message.chunk && Buffer.from(message.chunk).length > MAX_MESSAGE_SIZE) {
      // Split large chunks into smaller messages if needed
      const chunks = message.chunk.match(new RegExp(`.{1,${MAX_MESSAGE_SIZE}}`, 'g')) || [];
      for (let i = 0; i < chunks.length; i++) {
        await this.channel.sendToQueue(
          'notifications',
          Buffer.from(JSON.stringify({
            ...message,
            chunk: chunks[i],
            chunkIndex: i,
            totalChunks: chunks.length,
            isLastChunk: i === chunks.length - 1 && message.isLastChunk
          }))
        );
      }
    } else {
      // Send regular notification
      await this.channel.sendToQueue(
        'notifications',
        Buffer.from(JSON.stringify({ ...message, userId }))
      );
    }
  }

  async handleProcessingError(msg, error) {
    const { fileId, userId } = JSON.parse(msg.content.toString());
    const retryCount = (msg.properties.headers['x-retry-count'] || 0) + 1;

    await File.findOneAndUpdate(
      { fileId },
      {
        status: retryCount <= 3 ? 'processing' : 'failed',
        error: error.message,
        updatedAt: new Date()
      }
    );

    await this.sendNotification(userId, {
      type: 'processing_error',
      fileId,
      error: error.message,
      willRetry: retryCount <= 3
    });

    if (retryCount <= 3) {
      await this.channel.sendToQueue('file_processing_retry', msg.content, {
        headers: { 'x-retry-count': retryCount },
        expiration: String(Math.pow(2, retryCount) * 1000)
      });
      this.channel.ack(msg);
    } else {
      await this.channel.sendToQueue('file_processing_dlq', msg.content, {
        headers: { 'x-error': error.message }
      });
      this.channel.ack(msg);
    }
  }
}
