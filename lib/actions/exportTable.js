const { doSearch } = require('../util')
const aws = require('aws-sdk');

const findAllElements = context => {
  return doSearch(context.dynamodb, context.tableName)
    .then(items => {
      context.items = items
      return context
    })
}

const formatExport = (items, tableName, awsFormat = false) => {
  if (!awsFormat) {
    return items.map(x => aws.DynamoDB.Converter.unmarshall(x))
  } else {
    return {
      [tableName]: items.map(x => ({
        PutRequest: {
          Item: x
        }
      }))
    }
  }
}

/**
 * This function exports all record from a given table within dynamodb.
 *
 * It functions as follows:
 *  1) Scan all records and store them in an array
 *  2) Pass the records to #formatExport which in turn changes items format to json
 *  3) Return a list of items
 *
 * @param tableName the table we want to export
 * @param dynamodb the AWS dynamodb service that holds the connection
 * @returns {Promise<any[] | never>} concatenation of all delete request promises
 */
const exportTable = (tableName, dynamodb, format = 'json') => {
  const context = {
    tableName,
    dynamodb
  }

  return findAllElements(context)
    .then(res => formatExport(res.items, tableName, format !== 'json'))
}

module.exports = { exportTable }
