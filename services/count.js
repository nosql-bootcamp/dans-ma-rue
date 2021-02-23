const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.count = (client, from, to, callback) => {
    // Compte le nombre d'anomalies entre deux dates
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
    // Compte le nombre d'anomalies autour d'un point géographique, dans un rayon donné
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