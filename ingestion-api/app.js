const express = require('express');
const bodyParser = require('body-parser');
const { Client } = require('pg');
const { MongoClient } = require('mongodb');

const app = express();
app.use(bodyParser.json());

// PostgreSQL configuration
const pgClient = new Client({
  user: 'myuser',
  host: 'postgres', // Docker service name
  database: 'datapipeline',
  password: 'mypassword',
  port: 5432,
});

async function connectToPostgresWithRetry(retries = 10, delay = 2000) {
  for (let i = 0; i < retries; i++) {
    try {
      await pgClient.connect();
      console.log("✅ Connected to PostgreSQL");
      return;
    } catch (err) {
      console.error(`❌ PostgreSQL connection failed (attempt ${i + 1}/${retries}):`, err.message);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  console.error("💥 Could not connect to PostgreSQL after multiple attempts. Exiting.");
  process.exit(1);
}

connectToPostgresWithRetry();


// MongoDB configuration
const mongoUrl = "mongodb://mongodb:27017";
const mongoClient = new MongoClient(mongoUrl);
let mongoDB;

mongoClient.connect()
  .then(client => {
    mongoDB = client.db("datapipeline");
    console.log("✅ Connected to MongoDB");
  })
  .catch(err => {
    console.error("❌ MongoDB connection error:", err.message);
    process.exit(1); // Exit if MongoDB fails to connect
  });


// Endpoint to receive synthetic data
app.post('/data', async (req, res) => {
    const record = req.body;
    try {
        // Insert into PostgreSQL
        const pgQuery = 'INSERT INTO transactions(transaction_id, customer_id, amount, timestamp) VALUES($1, $2, $3, $4)';
        await pgClient.query(pgQuery, [record.transaction_id, record.customer_id, record.amount, record.timestamp]);

        // Insert into MongoDB
        await mongoDB.collection('transactions').insertOne(record);

        res.status(200).send({status: 'success'});
    } catch (error) {
        console.error("Error storing data:", error);
        res.status(500).send({status: 'error'});
    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`Ingestion API is running on port ${PORT}`);
});
