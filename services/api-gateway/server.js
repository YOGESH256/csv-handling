const express = require('express');
const { createProxyMiddleware } = require('http-proxy-middleware');
const jwt = require('jsonwebtoken');

const app = express();

// JWT authentication middleware
const authenticateJWT = (req, res, next) => {
  const token = req.headers.authorization?.split(' ')[1];
  
  if (!token) {
    return res.status(401).json({ error: 'Authentication required' });
  }
  
  try {
    const user = jwt.verify(token, process.env.JWT_SECRET);
    req.user = user;
    next();
  } catch (error) {
    res.status(403).json({ error: 'Invalid token' });
  }
};

// Route proxies
app.use('/auth', createProxyMiddleware({
  target: process.env.AUTH_SERVICE_URL,
  changeOrigin: true
}));

app.use('/upload', authenticateJWT, createProxyMiddleware({
  target: process.env.UPLOAD_SERVICE_URL,
  changeOrigin: true
}));

app.use('/ws', authenticateJWT, createProxyMiddleware({
  target: process.env.STREAMING_SERVICE_URL,
  changeOrigin: true,
  ws: true
}));