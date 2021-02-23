const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.count = (client, from, to, callback) => {
    res = client.count({
        index: indexName,
        body: {
            query: {
                range: {
                    "timestamp": {
                        "gte": from,
                        "lte": to
                    }
                }
            }
        }
    }).then(res => {
        callback({ count: res.body.count })
    })
        .catch(err => {
            console.log(err)
        });
}

exports.countAround = (client, lat, lon, radius, callback) => {

    res = client.count({
        index: indexName,
        body: {
            query: {
                geo_distance: {
                    "distance": radius,
                    "location": lat + "," + lon
                }
            }
        }
    }).then(res => {
        callback({ count: res.body.count })
    })
        .catch(err => {
            console.log(err)
        });

}