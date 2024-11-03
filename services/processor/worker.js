const amqp = require('amqplib');
const AWS = require('aws-sdk');
const { Transform } = require('stream');
const mongoose = require('mongoose');
const File = require('../shared/models/File');
const ChunkProcessing = require('../shared/models/ChunkProcessing'); // New model needed

class ChunkProcessingService {
  constructor() {
    this.connection = null;
    this.channel = null;
    this.s3 = new AWS.S3();
  }

  async start() {
    await mongoose.connect(process.env.MONGODB_URL);
    this.connection = await amqp.connect(process.env.RABBITMQ_URL);
    this.channel = await this.connection.createChannel();
    await this.setupQueues();
    this.startProcessing();
  }

  async setupQueues() {
    // Queue for processing individual chunks
    await this.channel.assertQueue('chunk_processing', {
      durable: true,
      arguments: {
        'x-queue-type': 'classic',
        'x-max-priority': 10
      }
    });

    // Queue for final file assembly
    await this.channel.assertQueue('file_assembly', {
      durable: true
    });

    await this.channel.assertQueue('notifications', { durable: true });
    await this.channel.assertQueue('chunk_processing_dlq', { durable: true });
    await this.channel.assertQueue('chunk_processing_retry', {
      durable: true,
      arguments: {
        'x-dead-letter-exchange': '',
        'x-dead-letter-routing-key': 'chunk_processing',
        'x-message-ttl': 30000
      }
    });
  }

  async startProcessing() {
    // Listen for chunk processing
    await this.channel.consume('chunk_processing', async (msg) => {
      if (!msg) return;

      try {
        const chunkInfo = JSON.parse(msg.content.toString());
        await this.processChunk(chunkInfo);
        this.channel.ack(msg);
      } catch (error) {
        await this.handleChunkProcessingError(msg, error);
      }
    });

    // Listen for file assembly completion
    await this.channel.consume('file_assembly', async (msg) => {
      if (!msg) return;

      try {
        const fileInfo = JSON.parse(msg.content.toString());
        if (fileInfo.status === 'complete') {
          await this.handleFileCompletion(fileInfo);
        }
        this.channel.ack(msg);
      } catch (error) {
        console.error('File assembly error:', error);
        this.channel.nack(msg);
      }
    });
  }

  async processChunk(chunkInfo) {
    const {
      fileId,
      partNumber,
      totalChunks,
      uploadId,
      metadata,
      userId
    } = chunkInfo;

    // Update chunk processing status
    await ChunkProcessing.findOneAndUpdate(
      { fileId, partNumber },
      {
        status: 'processing',
        startedAt: new Date(),
        metadata
      },
      { upsert: true }
    );

    // Process the chunk
    const chunkData = await this.processChunkData(fileId, partNumber, metadata);

    // Update chunk status
    await ChunkProcessing.findOneAndUpdate(
      { fileId, partNumber },
      {
        status: 'completed',
        completedAt: new Date(),
        processedData: chunkData
      }
    );

    // Send notification for chunk completion
    await this.sendNotification(userId, {
      type: 'chunk_processed',
      fileId,
      partNumber,
      totalChunks,
      status: 'completed'
    });

    // Check if this was the last chunk
    const processedChunks = await ChunkProcessing.countDocuments({
      fileId,
      status: 'completed'
    });

    if (processedChunks === totalChunks) {
      // Signal for file assembly
      await this.channel.sendToQueue('file_assembly', Buffer.from(JSON.stringify({
        fileId,
        uploadId,
        status: 'ready_for_assembly',
        totalChunks,
        userId
      })));
    }
  }

  async processChunkData(fileId, partNumber, metadata) {
    return new Promise((resolve, reject) => {
      const CHUNK_SIZE = 64 * 1024;

      const readStream = this.s3.getObject({
        Bucket: process.env.S3_BUCKET,
        Key: `uploads/${fileId}/part${partNumber}`,
        RequestOptions: {
          highWaterMark: CHUNK_SIZE
        }
      }).createReadStream();

      let processedData = '';
      
      const processingStream = new Transform({
        transform: (chunk, encoding, callback) => {
          try {
            // Process the chunk data (e.g., CSV parsing, validation, transformation)
            const processed = this.transformChunkData(chunk, metadata);
            processedData += processed;
            callback(null, processed);
          } catch (error) {
            callback(error);
          }
        }
      });

      readStream
        .pipe(processingStream)
        .on('error', reject)
        .on('finish', () => resolve(processedData));
    });
  }

  transformChunkData(chunk, metadata) {
    // Implement your chunk-specific processing logic here
    // This could include CSV parsing, data validation, transformation, etc.
    return chunk.toString();
  }

  async handleFileCompletion(fileInfo) {
    const { fileId, userId } = fileInfo;

    try {
      // Get all processed chunks
      const chunks = await ChunkProcessing.find({ 
        fileId,
        status: 'completed'
      }).sort({ partNumber: 1 });

      // Validate all chunks are present and processed
      if (chunks.length !== fileInfo.totalChunks) {
        throw new Error('Missing processed chunks');
      }

      // Update file status
      await File.findOneAndUpdate(
        { fileId },
        {
          status: 'completed',
          updatedAt: new Date()
        }
      );

      // Send completion notification
      await this.sendNotification(userId, {
        type: 'processing_completed',
        fileId,
        status: 'completed'
      });

    } catch (error) {
      await File.findOneAndUpdate(
        { fileId },
        {
          status: 'failed',
          error: error.message,
          updatedAt: new Date()
        }
      );

      await this.sendNotification(userId, {
        type: 'processing_error',
        fileId,
        error: error.message
      });
    }
  }

  async handleChunkProcessingError(msg, error) {
    const { fileId, partNumber, userId } = JSON.parse(msg.content.toString());
    const retryCount = (msg.properties.headers['x-retry-count'] || 0) + 1;

    await ChunkProcessing.findOneAndUpdate(
      { fileId, partNumber },
      {
        status: retryCount <= 3 ? 'processing' : 'failed',
        error: error.message,
        updatedAt: new Date()
      }
    );

    if (retryCount <= 3) {
      await this.channel.sendToQueue('chunk_processing_retry', msg.content, {
        headers: { 'x-retry-count': retryCount },
        expiration: String(Math.pow(2, retryCount) * 1000)
      });
      this.channel.ack(msg);
    } else {
      await this.channel.sendToQueue('chunk_processing_dlq', msg.content);
      this.channel.ack(msg);
      
      // Notify about chunk failure
      await this.sendNotification(userId, {
        type: 'chunk_processing_error',
        fileId,
        partNumber,
        error: error.message
      });
    }
  }

  async sendNotification(userId, message) {
    if (!this.channel) {
      throw new Error('RabbitMQ channel not initialized');
    }

    await this.channel.sendToQueue(
      'notifications',
      Buffer.from(JSON.stringify({ ...message, userId }))
    );
  }
}

module.exports = ChunkProcessingService;