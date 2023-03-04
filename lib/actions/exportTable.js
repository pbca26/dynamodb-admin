const { doSearch } = require('../util')

const dbTypes = ['N', 'S', 'BOOL', 'SS', 'NS']
const dbTypesFormat = (type, val) => {
  switch (type) {
    case 'N':
      return Number(val)
    case 'BOOL':
      return Number(val)
    case 'NS':
      return val.map(x => Number(x))
    default:
      return val
  }
}

const findAllElements = context => {
  return doSearch(context.dynamodb, context.tableName)
    .then(items => {
      context.items = items
      return context
    })
}

const extractObjectEntries = x => {
  const keys = Object.keys(x)
  const newItem = {}

  for (let i = 0; i < keys.length; i++) {
    const key = Object.keys(x[keys[i]])[0]

    if (dbTypes.indexOf(key) > -1) {
      newItem[keys[i]] = dbTypesFormat(key, x[keys[i]][key])
    } else if (key === 'M') {
      newItem[keys[i]] = extractObjectEntries(x[keys[i]][key])
    }
  }

  return newItem
}

const formatExport = (items, awsFormat = false) => {
  if (!awsFormat) {
    return items.map(x => extractObjectEntries(x))
  }

  return items
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
const exportTable = (tableName, dynamodb) => {
  const context = {
    tableName,
    dynamodb
  }

  return findAllElements(context)
    .then(res => formatExport(res.items))
}

module.exports = { exportTable }
