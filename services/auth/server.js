const express = require('express');
const jwt = require('jsonwebtoken');
const { Pool } = require('pg');
const Redis = require('ioredis');
const rateLimit = require('express-rate-limit');

const app = express();
const pool = new Pool({
  connectionString: process.env.POSTGRES_URL
});
const redis = new Redis(process.env.REDIS_URL);

// Rate limiting middleware
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100
});

// RBAC roles and permissions
const roles = {
  admin: ['read', 'write', 'delete', 'manage_users'],
  manager: ['read', 'write'],
  user: ['read']
};

// User authentication
app.post('/auth/login', limiter, async (req, res) => {
  const { username, password } = req.body;
  
  try {
    const user = await pool.query(
      'SELECT * FROM users WHERE username = $1',
      [username]
    );
    
    if (!user.rows[0]) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }
    
    // Generate JWT
    const token = jwt.sign(
      { 
        id: user.rows[0].id,
        role: user.rows[0].role
      },
      process.env.JWT_SECRET,
      { expiresIn: '24h' }
    );
    
    // Cache user permissions
    await redis.set(
      `permissions:${user.rows[0].id}`,
      JSON.stringify(roles[user.rows[0].role])
    );
    
    res.json({ token });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});
