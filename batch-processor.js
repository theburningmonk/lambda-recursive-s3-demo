'use strict';

const co      = require('co');
const _       = require('lodash');
const AWS     = require('aws-sdk');
const Promise = require('bluebird');
const lambda  = new AWS.Lambda();
const s3      = new AWS.S3();

// Data loaded from S3 and chached in case of recursion.
let cached;

let loadData = co.wrap(function* (bucket, key) {
  try {
    console.log('Loading data from S3', { bucket, key });

    let req = { 
      Bucket: bucket, 
      Key: key, 
      IfNoneMatch: _.get(cached, 'etag') 
    };
    let resp = yield s3.getObject(req).promise();

    console.log('Caching data', { bucket, key, etag: resp.ETag });
    let data = JSON.parse(resp.Body);
    cached = { bucket, key, data, etag: resp.ETag };
    return data;
  } catch (err) {
    if (err.code === "NotModified") {
      console.log('Loading cached data', { bucket, key, etag: cached.etag });
      return cached.data;
    } else {
      throw err;
    }
  }
});

let recurse = co.wrap(function* (payload) {
  let req = {
    FunctionName: process.env.AWS_LAMBDA_FUNCTION_NAME,
    InvocationType: 'Event',
    Payload: JSON.stringify(payload)
  };

  console.log('Recursing...', req);
  let resp = yield lambda.invoke(req).promise();
  console.log('Invocation complete', resp);

  return resp;
});

module.exports.handler = co.wrap(function* (event, context, callback) {
  console.log(JSON.stringify(event));

  let bucket   = _.get(event, 'Records[0].s3.bucket.name');
  let key      = _.get(event, 'Records[0].s3.object.key');
  let position = event.position || 0;
  let data     = yield loadData(bucket, key);

  let totalTaskCount = data.tasks.length;
  let batchSize      = process.env.BATCH_SIZE || 5;

  try {
    do {
      console.log('Processing next batch...');
      let batch = data.tasks.slice(position, position + batchSize);
      position = position + batch.length;
      
      for (let task of batch) {
        yield Promise.delay(1000); // each task takes a second to process
      }
    } while (position < totalTaskCount && 
            context.getRemainingTimeInMillis() > 10000);

    if (position < totalTaskCount) {
      let newEvent = Object.assign(event, { position });
      yield recurse(newEvent);
      callback(null, `to be continued...[${position}]`);
    } else {
      callback(null, "all done");
    }
  } catch (err) {
    callback(err);
  }
});