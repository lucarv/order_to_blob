// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

'use strict';
require('dotenv').load();

var orders = [];
var timeStamps = [];
var current_order = 0;
const storage = require('azure-storage');

var blobService = storage.createBlobService();
var containerName = process.env.IOT_HUB_CONNECTION_STRING;
var hubCS = process.env.IOT_HUB_CONNECTION_STRING

var {
  EventHubClient,
  EventPosition
} = require('azure-event-hubs');
var printError = function (err) {
  console.log(err.message);
};

var recordTime = function (message) {
  if (current_order !== 0) {
    if (message.body.work_order_number !== current_order) {
      current_order = message.body.work_order_number;
      orders.push(current_order);
      timeStamps.push(new Date());
      let completed = {
        "work_order_number": orders[0],
        "timeStamps": {
          "start": timeStamps[0],
          "end": timeStamps[1]
        }
      }
      uploadBlob(completed, 'work_order_number_' + orders[0] + '.json')
      orders.shift();
      timeStamps.shift();
    }
  } else {
    current_order = message.body.work_order_number;
    orders.push(current_order);
    timeStamps.push(new Date())
  }
};

var ehClient;
EventHubClient.createFromIotHubConnectionString(hubCS).then(function (client) {
  console.log("Successully created the EventHub Client from iothub connection string.");
  ehClient = client;
  return ehClient.getPartitionIds();
}).then(function (ids) {
  return ids.map(function (id) {
    return ehClient.receive(id, recordTime, printError, {
      eventPosition: EventPosition.fromEnqueuedTime(Date.now())
    });
  });
}).catch(printError);

function uploadBlob(payload, blobName) {
  blobService.createBlockBlobFromText(
    containerName,
    JSON.stringify(payload),
    order,
    function (error, result, response) {
      if (error) {
        console.log("Couldn't upload string");
        console.error(error);
      } else {
        console.log('String uploaded successfully');
      }
    });
};