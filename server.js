// LOAD AWS SDK for NodeJS
var AWS = require('aws-sdk');

// AWS CONFIG FROM ENV
//const awsAccessKey = process.env.AWS_ACCESS_KEY;
//const awsSecretKey = process.env.AWS_ACCESS_SECRET;
//const awsRegion = process.env.AWS_REGION_ID;
const bucketName = process.env.AWS_S3_BUCKET_NAME;
const streamName = process.env.AWS_KINESIS_STREAM_NAME;

var errCode = 0

/*
if (typeof awsAccessKey === 'undefined') {
    errCode = errCode + 1
}
if (typeof awsSecretKey === 'undefined') {
    errCode = errCode + 10
}
if (typeof awsRegion === 'undefined') {
    errCode = errCode + 100
}
*/
if (typeof bucketName === 'undefined') {
    errCode = errCode + 1000
}
if (typeof streamName === 'undefined') {
    errCode = errCode + 10000
}

var continuationToken = null;

/*
AWS.config.update(
    {
        accessKeyId: awsAccessKey,
        secretAccessKey: awsSecretKey,
        region: awsRegion
    }
);
*/
var s3 = new AWS.S3();
var kinesis = new AWS.Kinesis();

switch ( errCode ) {
    case 0:
        listFileByPrefix();
        break;
    default:
        let ts = getTimestamp();
        console.log(`${ts} UNABLE TO START...`)
        //if ( errCode >0 ) { console.log(`${ts} environment variable AWS_ACCESS_KEY is not configured. Please provide a valid one.`)}
        //if ( errCode >9 ) { console.log(`${ts} environment variable AWS_ACCESS_SECRET is not configured. Please provide a valid one.`)}
        //if ( errCode >99 ) { console.log(`${ts} environment variable AWS_REGION_ID is not configured. Please provide a valid one.`)}
        if ( errCode >999 ) { console.log(`${ts} environment variable AWS_S3_BUCKET_NAME is not configured. Please provide a valid one.`)}
        if ( errCode >9999 ) { console.log(`${ts} environment variable AWS_KINESIS_STREAM_NAME is not configured. Please provide a valid one.`)}
        break;
}

function getTimestamp() {
    let date_ob = new Date();
    let day = ("0" + date_ob.getDate()).slice(-2);
    let month = ("0" + (date_ob.getMonth().valueOf() + 1)).slice(-2);
    let year = date_ob.getFullYear();
    let hours = ("0" + date_ob.getHours()).slice(-2);
    let minutes = ("0" + date_ob.getMinutes()).slice(-2);
    let seconds = ("0" + date_ob.getSeconds()).slice(-2);
    return year + "-" + month + "-" + day + " " + hours + ":" + minutes + ":" + seconds;
}

async function listFileByPrefix() {
    counter = 1
    token = null
    keepgoing = true
    do {
        var paramsList = {
            Bucket: bucketName,
            ContinuationToken: token
        }
        const objs = await s3.listObjectsV2(paramsList).promise();
        console.log('counter = ' + counter)
        counter++
        token = objs.NextContinuationToken
        for (obj of objs.Contents) {

            const key = obj.Key;
            const extension = key.split('.').pop();

            if (extension === 'xml') {
                const getObjectParams = {
                    Bucket: bucketName,
                    Key: key
                };

                s3.getObject(getObjectParams, function (err, fileData) {
                    if (err) {
                        console.log(err, err.stack);
                    } else {
                        const dataParams = {
                            Data: fileData.Body.toString(),
                            PartitionKey: key,
                            StreamName: streamName
                        };

                        kinesis.putRecord(dataParams, function (err, data) {
                            if (err) {
                                let ts = getTimestamp();
                                console.log(`${ts} failed to send file ${key} to stream ${streamName}`);
                                console.log(`${ts} reason:`);
                                console.log(err, err.stack);
                            } else {
                                let ts = getTimestamp();
                                console.log(`${ts} - ${counter} - successfully sent file ${key} to stream ${streamName}`);
                            }
                        });
                    }
                });
            }
        }
        if (typeof token === 'undefined') {
            keepgoing = false;
        }
    } while (keepgoing == true);
}
