const express = require('express');
const multer = require('multer');
const amqp = require('amqplib');
const AWS = require('aws-sdk');
const crypto = require('crypto');
const { Readable } = require('stream');

const app = express();

// Configure S3
const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY,
  secretAccessKey: process.env.AWS_SECRET_KEY,
  endpoint: process.env.S3_ENDPOINT
});

// Constants
const CHUNK_SIZE = 5 * 1024 * 1024; // 5MB chunks for optimal S3 performance
const MAX_CONCURRENT_UPLOADS = 4;

// Queue configuration
let channel, connection;

async function setupQueues() {
  connection = await amqp.connect(process.env.RABBITMQ_URL);
  channel = await connection.createChannel();
  
  // Queue for processing individual chunks
  await channel.assertQueue('chunk_processing', {
    durable: true,
    arguments: {
      'x-queue-type': 'classic',
      'x-max-priority': 10
    }
  });
  
  // Queue for final file assembly
  await channel.assertQueue('file_assembly', {
    durable: true,
    arguments: {
      'x-queue-type': 'classic',
      'x-max-priority': 10
    }
  });
  
  await channel.prefetch(100);
  
  // Dead letter queues
  await channel.assertQueue('chunk_processing_dlq', { durable: true });
  await channel.assertQueue('file_assembly_dlq', { durable: true });
  
  // Retry queues
  await channel.assertQueue('chunk_processing_retry', {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': '',
      'x-dead-letter-routing-key': 'chunk_processing',
      'x-message-ttl': 30000
    }
  });
}

setupQueues().catch(console.error);

// Multer configuration
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: CHUNK_SIZE * 1.1 }
});

// Upload initialization
app.post('/upload/init', async (req, res) => {
  const { fileName, totalChunks, fileSize, fileType } = req.body;
  
  try {
    const fileId = crypto.randomBytes(16).toString('hex');
    const uploadId = await initializeMultipartUpload(fileId, fileName, fileType);
    
    // Store metadata for file assembly
    await channel.sendToQueue('file_assembly', Buffer.from(JSON.stringify({
      fileId,
      fileName,
      fileType,
      totalChunks,
      uploadId,
      chunksProcessed: 0,
      metadata: req.body.metadata
    })), {
      persistent: true,
      priority: calculatePriority(fileSize)
    });
    
    res.json({
      fileId,
      uploadId,
      chunkSize: CHUNK_SIZE,
      maxConcurrentUploads: MAX_CONCURRENT_UPLOADS
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

async function initializeMultipartUpload(fileId, fileName, fileType) {
  const params = {
    Bucket: process.env.S3_BUCKET,
    Key: `uploads/${fileId}/${fileName}`,
    ContentType: fileType
  };
  
  const multipartUpload = await s3.createMultipartUpload(params).promise();
  return multipartUpload.UploadId;
}

// Chunk upload handler with immediate processing
app.post('/upload/chunk/:fileId/:uploadId', upload.single('chunk'), async (req, res) => {
  const { fileId, uploadId } = req.params;
  const partNumber = parseInt(req.body.partNumber);
  const totalChunks = parseInt(req.body.totalChunks);
  
  try {
    // Upload chunk to S3
    const partResponse = await uploadChunk(fileId, uploadId, partNumber, req.file.buffer);
    
    // Queue chunk for immediate processing
    await queueChunkForProcessing({
      fileId,
      partNumber,
      totalChunks,
      etag: partResponse.ETag,
      uploadId,
      metadata: req.body.metadata
    });
    
    res.json({
      status: 'success',
      partNumber,
      etag: partResponse.ETag
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

async function uploadChunk(fileId, uploadId, partNumber, buffer) {
  const params = {
    Bucket: process.env.S3_BUCKET,
    Key: `uploads/${fileId}`,
    PartNumber: partNumber,
    UploadId: uploadId,
    Body: buffer
  };
  
  return await s3.uploadPart(params).promise();
}

async function queueChunkForProcessing(chunkInfo) {
  await channel.sendToQueue('chunk_processing', Buffer.from(JSON.stringify({
    ...chunkInfo,
    timestamp: Date.now()
  })), {
    persistent: true,
    priority: 5, // Medium priority for all chunks
    headers: {
      'x-chunk-number': chunkInfo.partNumber,
      'x-total-chunks': chunkInfo.totalChunks
    }
  });
}

// Modified complete upload endpoint
app.post('/upload/complete/:fileId/:uploadId', async (req, res) => {
  const { fileId, uploadId } = req.params;
  const { parts } = req.body;
  
  try {
    // Complete multipart upload in S3
    await completeUpload(fileId, uploadId, parts);
    
    // Signal completion to assembly queue
    await channel.sendToQueue('file_assembly', Buffer.from(JSON.stringify({
      fileId,
      uploadId,
      status: 'complete',
      timestamp: Date.now()
    })), {
      persistent: true,
      priority: 10 // High priority for completion signals
    });
    
    res.json({ status: 'success', fileId });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

async function completeUpload(fileId, uploadId, parts) {
  const params = {
    Bucket: process.env.S3_BUCKET,
    Key: `uploads/${fileId}`,
    UploadId: uploadId,
    MultipartUpload: { Parts: parts }
  };
  
  return await s3.completeMultipartUpload(params).promise();
}

function calculatePriority(fileSize) {
  if (fileSize < 10 * 1024 * 1024) return 10;
  if (fileSize < 100 * 1024 * 1024) return 5;
  return 1;
}

// Graceful shutdown
process.on('SIGTERM', async () => {
  try {
    await channel.close();
    await connection.close();
  } catch (error) {
    console.error('Error during shutdown:', error);
  }
  process.exit(0);
});

module.exports = { app };