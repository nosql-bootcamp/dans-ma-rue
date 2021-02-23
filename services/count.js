const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.count = async (client, from, to, callback) => {
  let count

  const body = {
    query: {
      range: {
        "@timestamp": {
          gte: from,
          lt: to,
        }
      }
    }
  }
  console.info(from, to)
  console.info(JSON.stringify(body, null, 4))
  try {
    count = (await client.count({
      index: 'dansmarue',
      body: body,
    }, {
      ignore: [404],
      maxRetries: 3
    })).body.count
  } catch (e) {
    console.error(e.body)
  }
  callback({
    count: count
  })
}

exports.countAround = async (client, lat, lon, radius, callback) => {
  let count
  const body = {
    query: {
      bool: {
        must: {
          match_all: {}
        },
        filter: [
          {
            geo_distance: {
              distance: radius,
              geo_point_2d: {
                lat: lat,
                lon: lon,
              }
            }
          }
        ]
      }
    }
  }
  console.info(lat, lon, radius)
  console.info(JSON.stringify(body, null, 4))

  try {
    count = (await client.count({
      index: 'dansmarue',
      body: body,
    }, {
      ignore: [404],
      maxRetries: 3
    })).body.count
  } catch (e) {
    console.error(e.body)
  }
  callback({
    count: count
  })
}
