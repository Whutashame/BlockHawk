const express = require('express');
const mysql = require('mysql2');
const cors = require('cors');
const app = express();
app.use(express.json());
app.use(cors());


const db = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: '',
    database: 'pfs'
});





app.get('/data', (req, res) => {
    new Promise((resolve, reject) => {
        db.query('SELECT * FROM transactions3 ORDER BY block_number DESC LIMIT 5', (err, results) => {
            if (err) {
                reject(err);
            } else {
                resolve(results);
            }
        });
    })
    .then(results => res.json(results))
    .catch(err => res.status(500).json({ error: err.message }));
});

app.get('/data3/:tx_hash', (req, res) => {
    const txHash = req.params.tx_hash;

    // Execute a SQL query to fetch data from both tables using JOIN
    const query = `
        SELECT t3.*, ts.score
        FROM transactions3 t3
        LEFT JOIN transaction_scores ts ON t3.tx_hash = ts.tx_hash
        WHERE t3.tx_hash = ?
    `;

    db.query(query, [txHash], (err, results) => {
        if (err) {
            res.status(500).json({ error: err.message });
        } else {
            if (results.length === 0) {
                res.status(404).json({ error: "Transaction not found" });
            } else {
                res.json(results[0]); // Assuming only one result is expected
            }
        }
    });
});


   

app.get('/data2', (req, res) => {
    new Promise((resolve, reject) => {
        db.query('SELECT block_number, block_timestamp, COUNT(*) AS transaction_count FROM transactions3 GROUP BY block_number ORDER BY block_number DESC LIMIT 7', (err, results) => {
            if (err) {
                reject(err);
            } else {
                resolve(results);
            }
        });
    })
    .then(results => res.json(results))
    .catch(err => res.status(500).json({ error: err.message }));
});

app.listen(3001, () => {
    console.log('Server is running on port 3001');
});


