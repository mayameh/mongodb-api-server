const express = require('express');
const { MongoClient } = require('mongodb');

const app = express();

// Increase payload size limit for batched data
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));

// Environment variables
const MONGO_URI = process.env.MONGO_URI;
const API_KEY = process.env.API_KEY;
const PORT = process.env.PORT || 3000;

let db = null;
let client = null;
let isConnected = false;

// Connect to MongoDB on startup
async function connectDB() {
  try {
    console.log('🔄 Connecting to MongoDB...');
    client = new MongoClient(MONGO_URI, {
      serverSelectionTimeoutMS: 10000,
      socketTimeoutMS: 45000,
    });
    await client.connect();
    await client.db('admin').command({ ping: 1 });
    db = client.db('quantconnect');
    isConnected = true;
    console.log('✅ MongoDB connected successfully');
  } catch (error) {
    console.error('❌ MongoDB connection failed:', error.message);
    isConnected = false;
    // Retry connection after 5 seconds
    setTimeout(connectDB, 5000);
  }
}

// Middleware: Authenticate via API key in payload (QuantConnect style)
function authenticate(req, res, next) {
  // Check for api_key in body (QuantConnect sends it here)
  const apiKey = req.body.api_key;
  
  if (!apiKey) {
    console.log('❌ Missing api_key in request body');
    return res.status(401).json({ 
      success: false,
      error: 'Missing api_key in request body' 
    });
  }
  
  if (apiKey !== API_KEY) {
    console.log('❌ Invalid api_key provided');
    return res.status(401).json({ 
      success: false,
      error: 'Invalid api_key' 
    });
  }
  
  // Remove api_key from body before processing
  delete req.body.api_key;
  next();
}

// Root endpoint
app.get('/', (req, res) => {
  res.json({
    service: 'QuantConnect MongoDB API',
    version: '1.0.0',
    status: isConnected ? 'connected' : 'connecting',
    endpoints: ['/health', '/api/insert', '/api/insert_many', '/api/update']
  });
});

// Health check (no auth required)
app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok',
    mongodb: isConnected ? 'connected' : 'disconnected',
    timestamp: new Date().toISOString(),
    uptime: process.uptime()
  });
});

// Insert single document
app.post('/api/insert', authenticate, async (req, res) => {
  console.log(`📥 INSERT request received`);
  
  try {
    const { collection, document, metadata } = req.body;
    
    if (!collection || !document) {
      return res.status(400).json({ 
        success: false,
        error: 'Missing required fields: collection, document' 
      });
    }
    
    if (!isConnected || !db) {
      console.log('❌ MongoDB not connected');
      return res.status(503).json({ 
        success: false,
        error: 'Database not connected' 
      });
    }
    
    // Add metadata and timestamp
    const docToInsert = {
      ...document,
      ...metadata,
      createdAt: new Date()
    };
    
    const result = await db.collection(collection).insertOne(docToInsert);
    
    console.log(`✅ Inserted 1 document into ${collection}`);
    
    res.json({ 
      success: true,
      insertedId: result.insertedId.toString(),
      collection: collection
    });
    
  } catch (error) {
    console.error('❌ Insert error:', error.message);
    res.status(500).json({ 
      success: false,
      error: error.message 
    });
  }
});

// Insert many documents (for batching)
app.post('/api/insert_many', authenticate, async (req, res) => {
  console.log(`📥 INSERT_MANY request received`);
  
  try {
    const { collection, documents, metadata } = req.body;
    
    if (!collection || !documents || !Array.isArray(documents)) {
      return res.status(400).json({ 
        success: false,
        error: 'Missing required fields: collection, documents (array)' 
      });
    }
    
    if (!isConnected || !db) {
      console.log('❌ MongoDB not connected');
      return res.status(503).json({ 
        success: false,
        error: 'Database not connected' 
      });
    }
    
    // Add metadata and timestamp to all documents
    const docsToInsert = documents.map(doc => ({
      ...doc,
      ...metadata,
      createdAt: new Date()
    }));
    
    const result = await db.collection(collection).insertMany(docsToInsert);
    
    console.log(`✅ Inserted ${documents.length} documents into ${collection}`);
    
    res.json({ 
      success: true,
      insertedCount: result.insertedCount,
      collection: collection
    });
    
  } catch (error) {
    console.error('❌ Insert many error:', error.message);
    res.status(500).json({ 
      success: false,
      error: error.message 
    });
  }
});

// Update document
app.put('/api/update', authenticate, async (req, res) => {
  console.log(`📥 UPDATE request received`);
  
  try {
    const { collection, filter, update, metadata } = req.body;
    
    if (!collection || !filter || !update) {
      return res.status(400).json({ 
        success: false,
        error: 'Missing required fields: collection, filter, update' 
      });
    }
    
    if (!isConnected || !db) {
      console.log('❌ MongoDB not connected');
      return res.status(503).json({ 
        success: false,
        error: 'Database not connected' 
      });
    }
    
    const result = await db.collection(collection).updateOne(
      filter,
      { 
        $set: { 
          ...update,
          updatedAt: new Date() 
        } 
      }
    );
    
    console.log(`✅ Updated ${result.modifiedCount} document(s) in ${collection}`);
    
    res.json({ 
      success: true,
      matchedCount: result.matchedCount,
      modifiedCount: result.modifiedCount,
      collection: collection
    });
    
  } catch (error) {
    console.error('❌ Update error:', error.message);
    res.status(500).json({ 
      success: false,
      error: error.message 
    });
  }
});

// Catch-all for undefined routes
app.use((req, res) => {
  res.status(404).json({ 
    success: false,
    error: 'Endpoint not found' 
  });
});

// Global error handler
app.use((err, req, res, next) => {
  console.error('❌ Server error:', err);
  res.status(500).json({ 
    success: false,
    error: 'Internal server error' 
  });
});

// Start server
async function startServer() {
  // Connect to MongoDB first
  await connectDB();
  
  // Start HTTP server
  const server = app.listen(PORT, '0.0.0.0', () => {
    console.log('\n' + '='.repeat(60));
    console.log('🚀 QuantConnect MongoDB API Server');
    console.log('='.repeat(60));
    console.log(`📍 Listening on: http://0.0.0.0:${PORT}`);
    console.log(`🏥 Health check: /health`);
    console.log(`📚 Database: quantconnect`);
    console.log(`🔐 Auth: API key in request body`);
    console.log('='.repeat(60) + '\n');
  });

  // Graceful shutdown
  const shutdown = async (signal) => {
    console.log(`\n${signal} received, shutting down gracefully...`);
    
    server.close(async () => {
      console.log('📴 HTTP server closed');
      
      if (client) {
        await client.close();
        console.log('📴 MongoDB connection closed');
      }
      
      console.log('👋 Shutdown complete');
      process.exit(0);
    });

    // Force shutdown after 10 seconds
    setTimeout(() => {
      console.error('⚠️  Forced shutdown after timeout');
      process.exit(1);
    }, 10000);
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

// Handle uncaught errors
process.on('uncaughtException', (error) => {
  console.error('❌ Uncaught exception:', error);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Unhandled rejection at:', promise, 'reason:', reason);
});

// Start the server
startServer().catch((error) => {
  console.error('❌ Failed to start server:', error);
  process.exit(1);
});
