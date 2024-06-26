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
// configuration for private the connection string
config({ path: "credentials.env" });
//middlewares
app.use(bodyParser.urlencoded({ extended: true })); // middleware which fetches the form data
app.use(bodyParser.json());
//this is the path variable which will have exact location of the file that is being uploaded through frontend.
let path = "";
//Ensure the destination file has the exact same name and extension as the source file
//uploaded files store in upload folder automatically.
const storage = diskStorage({
    destination: function (req, file, cb) {
        cb(null, 'uploads/'); // Destination folder for uploaded files
    },
    filename: function (req, file, cb) {
        // Ensure original filename with extenstion
        const filename = `${file.originalname}`;
        //this path is now used in extracting details from the files
        path = `uploads/${filename}`
        cb(null, filename);
    }
});

// Used for uploading CSV file
const upload = multer({ storage: storage });

//upload csv file route
app.post('/upload_file', upload.single("files"), (req, res) => {
    let data = [];
    //Accessing CSV file 
    //Traverse an entire CSV file row by row and convert each row into an object
    //Where the keys are taken from the first row (header row) and values are taken from subsequent rows
    //At last it covert each rows into an object and push it into the array called data.
    createReadStream(path)
        //skipping header row as columns: true
        .pipe(parse({ delimiter: ",", from_line: 1, columns: true }))
        .on("data", function (row) {
            // Process each row here
            data.push(row);
        })
        .on("error", function (error) {
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
// Bellow mongo query is written by using idea of writing following SQL query-:

/*SELECT Market,
 SUM(CASE WHEN Operation = 'Buy' THEN "Buy/Sell Amount" ELSE -"Buy/Sell Amount" END) AS RemainingBals
FROM trades
WHERE UTC_time <= {input timestamp}
GROUP BY Market;
*/
app.get('/balance', async (req, res) => {

    const timeStamp = req.body.timestamp;
    try {
        const result = await Transaction.aggregate([
            {
                // Filter stage to include only documents where UTC_Time is less than or equal to timeStamp
                $match: { UTC_Time: { $lte: timeStamp } }
            },
            {
                $group: {
                    // Group by the 'Market' field
                    _id: "$Market",
                    remainingBals: {
                        $sum: {
                            $cond: {
                                if: { $eq: ["$Operation", "Buy"] },
                                then: "$Buy/Sell Amount",
                                else: { $subtract: [0, "$Buy/Sell Amount"] }
                            }
                        }
                    }
                }
            },
            //This additional project are used for renaming the property name
            {
                $project: {
                    _id: 0,
                    key: "$_id",
                    value: "$remainingBals"
                }
            },
            {
                $group: {
                    _id: null,
                    data: { $push: { k: "$key", v: "$value" } }
                }
            },
            {
                $project: {
                    _id: 0,
                    result: { $arrayToObject: "$data" }
                }
            },
            {
                $replaceRoot: { newRoot: "$result" }
            }
        ])
        res.send(result)
    }
    catch (err) {
        res.send(err)
    }
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


