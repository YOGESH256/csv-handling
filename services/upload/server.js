// upload-service.js
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
  // Single connection for better resource usage
  connection = await amqp.connect(process.env.RABBITMQ_URL);
  channel = await connection.createChannel();
  
  // Single queue for processing with proper prefetch
  await channel.prefetch(100); // Adjust based on your processing capacity
  await channel.assertQueue('file_processing', {
    durable: true,
    // Enable message persistence
    arguments: {
      'x-queue-type': 'classic',
      'x-max-priority': 10
    }
  });
  
  // Dead letter queue for failed processing
  await channel.assertQueue('file_processing_dlq', { durable: true });
  
  // Delayed retry queue for temporary failures
  await channel.assertQueue('file_processing_retry', {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': '',
      'x-dead-letter-routing-key': 'file_processing',
      'x-message-ttl': 30000 // 30 second retry delay
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

// Chunk upload handler
app.post('/upload/chunk/:fileId/:uploadId', upload.single('chunk'), async (req, res) => {
  const { fileId, uploadId } = req.params;
  const partNumber = parseInt(req.body.partNumber);
  
  try {
    // Upload chunk directly to S3
    const partResponse = await uploadChunk(fileId, uploadId, partNumber, req.file.buffer);
    
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

// Complete upload and trigger processing
app.post('/upload/complete/:fileId/:uploadId', async (req, res) => {
  const { fileId, uploadId } = req.params;
  const { parts } = req.body; // Array of { PartNumber, ETag }
  
  try {
    // Complete multipart upload
    await completeUpload(fileId, uploadId, parts);
    
    // Queue for processing with priority based on file size
    await queueForProcessing(fileId, req.body.metadata);
    
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

async function queueForProcessing(fileId, metadata) {
  // Queue message with file information and priority
  await channel.sendToQueue('file_processing', Buffer.from(JSON.stringify({
    fileId,
    metadata,
    timestamp: Date.now()
  })), {
    persistent: true,
    priority: calculatePriority(metadata.fileSize)
  });
}

function calculatePriority(fileSize) {
  // Assign priority based on file size (smaller files get higher priority)
  if (fileSize < 10 * 1024 * 1024) return 10; // < 10MB
  if (fileSize < 100 * 1024 * 1024) return 5; // < 100MB
  return 1; // Large files
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





