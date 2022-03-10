'use strict'
var AWS = require('aws-sdk')
var validate = require('./validate')
var readline = require('readline')
const AsyncQueue = require('./async_queue');

function copy(options, fn) {

  try {
    validate.config(options)
  } catch (err) {
    if (err) {
      return fn(err, {
        count: 0,
        status: 'FAIL'
      })
    }
  }

  var options = {
    config: options.config,
    source: {
      tableName: options.source.tableName,
      dynamoClient: options.source.dynamoClient || new AWS.DynamoDB.DocumentClient(options.source.config || options.config),
      dynamodb: options.source.dynamodb || new AWS.DynamoDB(options.source.config || options.config),
      active: options.source.active
    },
    destination: {
      tableName: options.destination.tableName,
      dynamoClient: options.destination.dynamoClient || new AWS.DynamoDB.DocumentClient(options.destination.config || options.config),
      dynamodb: options.destination.dynamodb || new AWS.DynamoDB(options.destination.config || options.config),
      active: options.destination.active,
      createTableStr: 'Creating Destination Table '
    },
    key: options.key,
    counter: options.counter || 0,
    queue: options.queue || new AsyncQueue({ backpressureMax: 100000 }),
    retries: 0,
    data: {},
    maxOutgoingRequests: options.maxOutgoingRequests || 100,
    transform: options.transform,
    log: options.log,
    create: options.create,
    schemaOnly: options.schemaOnly,
    continuousBackups: options.continuousBackups,
    readComplete: false,
    readCount: 0
  }

  // if (options.source.active && options.destination.active) { // both tables are active
  //   return startCopying(options, fn)
  // }

  if (options.create) { // create table if not exist
    return options.source.dynamodb.describeTable({ TableName: options.source.tableName }, function (err, data) {
      if (err) {
        return fn(err, data)
      }

      options.source.active = data.Table.TableStatus === 'ACTIVE'
      data.Table.TableName = options.destination.tableName
      options.destination.dynamodb.createTable(clearTableSchema(data.Table), function (err) {
        if (err && err.code !== 'ResourceInUseException') {
          return fn(err, data)
        }
        waitForActive(options, fn)
        // wait for TableStatus to be ACTIVE
      })
    })
  }

  checkTables(options, async function (err, data) { // check if source and destination table exist
    if (err) {
      return fn(err, data)
    }
    await startCopying(options);
    console.log(+ new Date());
  })

}

function enableBackups(options, fn) {
  options.destination.dynamodb.updateContinuousBackups({
    PointInTimeRecoverySpecification: {
      PointInTimeRecoveryEnabled: true
    },
    TableName: options.destination.tableName
  }, fn)
}

function setContinuousBackups(options, fn) {
  options.source.dynamodb.describeContinuousBackups({ TableName: options.source.tableName }, function (err, data) {
    if (err) {
      return fn(err, data);
    }
    if (data.ContinuousBackupsDescription.ContinuousBackupsStatus === 'ENABLED') {
      return enableBackups(options, fn);
    }
    fn(null)
  })

}

function clearTableSchema(table) {

  delete table.TableStatus;
  delete table.CreationDateTime;
  if (table.ProvisionedThroughput.ReadCapacityUnits === 0 && table.ProvisionedThroughput.WriteCapacityUnits === 0) {
    delete table.ProvisionedThroughput
  }
  else {
    delete table.ProvisionedThroughput.LastIncreaseDateTime;
    delete table.ProvisionedThroughput.LastDecreaseDateTime;
    delete table.ProvisionedThroughput.NumberOfDecreasesToday;
  }

  delete table.TableSizeBytes;
  delete table.ItemCount;
  delete table.TableArn;
  delete table.TableId;
  delete table.LatestStreamLabel;
  delete table.LatestStreamArn;

  if (table.LocalSecondaryIndexes && table.LocalSecondaryIndexes.length > 0) {
    for (var i = 0; i < table.LocalSecondaryIndexes.length; i++) {
      delete table.LocalSecondaryIndexes[i].IndexStatus;
      delete table.LocalSecondaryIndexes[i].IndexSizeBytes;
      delete table.LocalSecondaryIndexes[i].ItemCount;
      delete table.LocalSecondaryIndexes[i].IndexArn;
      delete table.LocalSecondaryIndexes[i].LatestStreamLabel;
      delete table.LocalSecondaryIndexes[i].LatestStreamArn;
    }
  }


  if (table.GlobalSecondaryIndexes && table.GlobalSecondaryIndexes.length > 0) {
    for (var j = 0; j < table.GlobalSecondaryIndexes.length; j++) {
      delete table.GlobalSecondaryIndexes[j].IndexStatus;
      if (table.GlobalSecondaryIndexes[j].ProvisionedThroughput.ReadCapacityUnits === 0 && table.GlobalSecondaryIndexes[j].ProvisionedThroughput.WriteCapacityUnits === 0) {
        delete table.GlobalSecondaryIndexes[j].ProvisionedThroughput
      }
      else {
        delete table.GlobalSecondaryIndexes[j].ProvisionedThroughput.LastIncreaseDateTime;
        delete table.GlobalSecondaryIndexes[j].ProvisionedThroughput.LastDecreaseDateTime;
        delete table.GlobalSecondaryIndexes[j].ProvisionedThroughput.NumberOfDecreasesToday;
      }
      delete table.GlobalSecondaryIndexes[j].IndexSizeBytes;
      delete table.GlobalSecondaryIndexes[j].ItemCount;
      delete table.GlobalSecondaryIndexes[j].IndexArn;
      delete table.GlobalSecondaryIndexes[j].LatestStreamLabel;
      delete table.GlobalSecondaryIndexes[j].LatestStreamArn;
    }
  }

  if (table.SSEDescription) {
    table.SSESpecification = {
      Enabled: (table.SSEDescription.Status === 'ENABLED' || table.SSEDescription.Status === 'ENABLING'),
    };
    delete table.SSEDescription;
  }

  if (table.BillingModeSummary) {
    table.BillingMode = table.BillingModeSummary.BillingMode
  }
  delete table.BillingModeSummary;

  return table;
}


function checkTables(options, fn) {
  options.source.dynamodb.describeTable({ TableName: options.source.tableName }, function (err, sourceData) {
    if (err) {
      return fn(err, sourceData)
    }
    if (sourceData.Table.TableStatus !== 'ACTIVE') {
      return fn(new Error('Source table not active'), null)
    }
    options.source.active = true
    options.destination.dynamodb.describeTable({ TableName: options.destination.tableName }, function (err, destData) {
      if (err) {
        return fn(err, destData)
      }
      if (destData.Table.TableStatus !== 'ACTIVE') {
        return fn(new Error('Destination table not active'), null)
      }
      options.destination.active = true
      fn(null)
    })
  })
}

function waitForActive(options, fn) {
  setTimeout(function () {
    options.destination.dynamodb.describeTable({ TableName: options.destination.tableName }, async function (err, data) {
      if (err) {
        return fn(err, data)
      }
      if (options.log) {
        options.destination.createTableStr += '.'
        readline.clearLine(process.stdout)
        readline.cursorTo(process.stdout, 0)
        process.stdout.write(options.destination.createTableStr)
      }
      if (data.Table.TableStatus !== 'ACTIVE') { // wait for active
        return waitForActive(options, fn)
      }
      options.create = false
      options.destination.active = true
      if (options.schemaOnly) { // schema only copied
        return fn(err, {
          count: options.counter,
          schemaOnly: true,
          status: 'SUCCESS'
        })
      }
      if (options.continuousBackups) { // copy backup options
        return setContinuousBackups(options, async function (err) {
          if (err) {
            return fn(err, data)
          }
          await startCopying(options);
          console.log(+new Date());
        })
      }
      await startCopying(options);
      console.log(+new Date());
    })
  }, 1000) // check every second
}

async function startReading(options) {
  while (true) {
    const data = await getItems(options);
    options.readCount += (data.Items || []).length;
    options.key = data.LastEvaluatedKey;
    log(options);
    await options.queue.push(...data.Items);
    if (typeof data.LastEvaluatedKey === 'undefined') {
      break;
    }
  }
}

async function startCopying(options) {
  startReading(options).then(() => options.readComplete = true);
  const writeWorkers = [];
  for (let i = 0; i < options.maxOutgoingRequests; i++) {
    console.log("starting worker " + (i + 1));
    writeWorkers.push(startWriteWorker(options));
  }
  await Promise.all(writeWorkers);
}

async function startWriteWorker(options) {
  while (!options.readComplete || !options.queue.empty()) {
    try {
      const records = await options.queue.take(25);
    const successful = await putItems(options, records);
    options.counter += successful;
    log(options);
    if (successful < records.length) {
      if (options.log) {
        process.stdout.write('*')
      }
      await timeout(250);
    } else {
      process.stdout.write(' ');
    }
    } catch (e) {
      console.error(e);
    }
    
  }
}

function log(options) {
  if (options.log) {
    readline.clearLine(process.stdout);
    readline.cursorTo(process.stdout, 0);
    process.stdout.write(`Read ${options.readCount} Wrote ${options.counter} `);
  }
}

async function getItems(options) {
  return mapItems(options, await scan(options));
}


async function timeout(duration) {
  return new Promise(resolve => {
    setTimeout(resolve, duration);
  });
}

function scan(options) {
  return new Promise((resolve, reject) => {
    options.source.dynamoClient.scan({
      TableName: options.source.tableName,
      Limit: 25,
      ExclusiveStartKey: options.key
    }, (error, data) => {
      if (error) { return reject(error) }
      resolve(data);
    });
  });
}

// const up = new Array(100).fill(undefined).map((x, i) => i);

function mapItems(options, data) {
  data.Items = data.Items.map(options.transform ? options.transform : (i) => i);
  return data;
}

async function putItems(options, items) {
  if (!items || items.length === 0) {
    return 0;
  }

  var batchWriteItems = {}
  batchWriteItems.RequestItems = {}
  batchWriteItems.RequestItems[options.destination.tableName] = items.map(item => ({ PutRequest: { Item: item } }));
  return new Promise((resolve, reject) => {
    options.destination.dynamoClient.batchWrite(batchWriteItems, async function (err, data) {
      if (err) {
        await options.queue.push(...items);
        return reject(err);
      }
      const unprocessed = data.UnprocessedItems[options.destination.tableName];
      if (unprocessed && unprocessed.length) {
        options.queue.push(...unprocessed.map(item => item.PutRequest.Item));
      }
      return resolve(items.length - (unprocessed ? unprocessed.length : 0));
    });
  });
}

module.exports.copy = copy
