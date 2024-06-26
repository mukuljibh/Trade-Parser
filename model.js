import { Schema, model } from "mongoose";

const tradeObj = new Schema({
    User_ID: {
        type: Number,
    },
    UTC_Time: {
        type: String,
    },
    Operation: {
        type: String,
    },
    Market: {
        type: String,
    },
    TradeType: {
        type: String,
    },
    Price: {
        type: Number,
    },
});

const Transaction = model(
    "Transaction",
    tradeObj,
);

export default Transaction 