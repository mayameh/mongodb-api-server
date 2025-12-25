const express = require('express');
const { MongoClient } = require('mongodb');
require('dotenv').config();

const app = express();
app.use(express.json({ limit: '10mb' }));

const MONGO_URI = process.env.MONGO_URI;
const API_KEY = process.env.API_KEY;
const PORT = process.env.PORT || 3000;

let db = null;
let client = null;

// Connect to MongoDB
async function connectDB() {
  try {
    client = new MongoClient(MONGO_URI, {
      serverSelectionTimeoutMS: 5000,
      socketTimeoutMS: 45000,
    });
    await client.connect();
    db = client.db('quantconnect');
    console.log('✅ Connected to MongoDB');
  } catch (error) {
    console.error('❌ MongoDB connection failed:', error.message);
    // Don't exit - server can still start, just log errors for API calls
  }
}

// Middleware: Authenticate API key
function authenticate(req, res, next) {
  const apiKey = req.body.api_key || req.query.api_key;
  
  if (!apiKey) {
    return res.status(401).json({ error: 'Missing api_key' });
  }
  
  if (apiKey !== API_KEY) {
    return res.status(401).json({ error: 'Invalid api_key' });
  }
  
  next();
}

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok',
    timestamp: new Date().toISOString(),
    mongodb: db ? 'connected' : 'disconnected'
  });
});

// Insert single document
app.post('/api/insert', authenticate, async (req, res) => {
  try {
    const { collection, document } = req.body;
    
    if (!collection || !document) {
      return res.status(400).json({ error: 'Missing collection or document' });
    }
    
    if (!db) {
      return res.status(503).json({ error: 'MongoDB not connected' });
    }
    
    const result = await db.collection(collection).insertOne({
      ...document,
      createdAt: new Date()
    });
    
    console.log(`✅ Inserted 1 document into ${collection}`);
    res.json({ 
      success: true, 
      insertedId: result.insertedId,
      collection: collection
    });
  } catch (error) {
    console.error('❌ Insert error:', error.message);
    res.status(500).json({ error: error.message });
  }
});

// Insert many documents
app.post('/api/insert_many', authenticate, async (req, res) => {
  try {
    const { collection, documents } = req.body;
    
    if (!collection || !documents || !Array.isArray(documents)) {
      return res.status(400).json({ error: 'Missing collection or documents array' });
    }
    
    if (!db) {
      return res.status(503).json({ error: 'MongoDB not connected' });
    }
    
    const docsWithTimestamp = documents.map(doc => ({
      ...doc,
      createdAt: new Date()
    }));
    
    const result = await db.collection(collection).insertMany(docsWithTimestamp);
    
    console.log(`✅ Inserted ${documents.length} documents into ${collection}`);
    res.json({ 
      success: true, 
      insertedCount: result.insertedCount,
      collection: collection
    });
  } catch (error) {
    console.error('❌ Insert many error:', error.message);
    res.status(500).json({ error: error.message });
  }
});

// Query documents
app.post('/api/query', authenticate, async (req, res) => {
  try {
    const { collection, filter = {}, limit = 10 } = req.body;
    
    if (!collection) {
      return res.status(400).json({ error: 'Missing collection' });
    }
    
    if (!db) {
      return res.status(503).json({ error: 'MongoDB not connected' });
    }
    
    const docs = await db.collection(collection)
      .find(filter)
      .limit(limit)
      .toArray();
    
    res.json({ 
      success: true, 
      count: docs.length,
      documents: docs
    });
  } catch (error) {
    console.error('❌ Query error:', error.message);
    res.status(500).json({ error: error.message });
  }
});

// Update document
app.put('/api/update', authenticate, async (req, res) => {
  try {
    const { collection, filter, update } = req.body;
    
    if (!collection || !filter || !update) {
      return res.status(400).json({ error: 'Missing collection, filter, or update' });
    }
    
    if (!db) {
      return res.status(503).json({ error: 'MongoDB not connected' });
    }
    
    const result = await db.collection(collection).updateOne(filter, { $set: update });
    
    console.log(`✅ Updated ${result.modifiedCount} document(s) in ${collection}`);
    res.json({ 
      success: true, 
      modifiedCount: result.modifiedCount,
      collection: collection
    });
  } catch (error) {
    console.error('❌ Update error:', error.message);
    res.status(500).json({ error: error.message });
  }
});

// Error handler
app.use((err, req, res, next) => {
  console.error('Server error:', err);
  res.status(500).json({ error: 'Internal server error' });
});

// Start server
async function startServer() {
  await connectDB();
  
  const server = app.listen(PORT, '0.0.0.0', () => {
    console.log(`\n${'='.repeat(60)}`);
    console.log('🚀 MongoDB REST API Server Running');
    console.log(`${'='.repeat(60)}`);
    console.log(`📍 Server: http://0.0.0.0:${PORT}`);
    console.log(`🏥 Health: http://localhost:${PORT}/health`);
    console.log(`📚 Database: quantconnect`);
    console.log(`${'='.repeat(60)}\n`);
  });

  // Handle graceful shutdown
  process.on('SIGTERM', async () => {
    console.log('\n🛑 Shutting down gracefully...');
    server.close(async () => {
      if (client) await client.close();
      process.exit(0);
    });
  });

  // Keep process alive
  process.on('exit', async () => {
    if (client) await client.close();
  });
}

startServer().catch(console.error);
