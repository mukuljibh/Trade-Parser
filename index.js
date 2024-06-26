import { config } from "dotenv";
import express from 'express';
import bodyParser from "body-parser";
import { connect } from 'mongoose';
import { createReadStream } from 'fs';
import { parse } from 'csv-parse';
import Transaction from './model.js'
import multer, { diskStorage } from "multer";

const app = express();
const port = 4000;

config({ path: "credentials.env" });
//middlewares
app.use(bodyParser.urlencoded({ extended: true })); // middleware which fetches the form data
app.use(bodyParser.json());

const storage = diskStorage({
    destination: function (req, file, cb) {
        cb(null, 'uploads/'); // Destination folder for uploaded files
    },
    filename: function (req, file, cb) {
        // Ensure original filename with extenstion
        const filename = `${file.originalname}`;
        cb(null, filename);
    }
});

const upload = multer({ storage: storage });

app.post('/upload_files', upload.single("files"), (req, res) => {
    let data = [];
    createReadStream('./uploads/sample.csv')
        .pipe(parse({ delimiter: ",", from_line: 1, columns: true }))
        .on("data", function (row) {
            // Process each row here
            data.push(row);
        })
        .on("error", function (error) {
            // Handle errors during parsing
            console.error("Error parsing CSV:", error.message);
        })
        .on("end", async function () {
            try {
                const insertedTransactions = await Transaction.insertMany(data);
                data = []
                res.status(201).json({ message: 'Transactions inserted successfully', data: insertedTransactions });
            } catch (error) {
                console.error('Error inserting transactions:', error.message);
                data = []
                res.status(500).json({ error: 'Failed to insert transactions' });
            }
        });
})
app.get('/balance', (req, res) => {
    const timeStamp = req.body.timestamp;

})



app.listen(port, () => {
    connect(process.env.CONNNECTIONSTRING)
        .then(() => {
            console.log("Connected successfully to Database");
        })
        .catch((err) => {
            console.log(err)
        });
});