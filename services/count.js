const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.count = (client, from, to, callback) => {
    // TODO Compter le nombre d'anomalies entre deux dates
    console.log(from, to);
    client.count({
        index: indexName,
        body: {
            query: {
                range: {
                    timestamp: {
                        gte: from,
                        lt: to
                    }
                }
            }
        }
    })
    .then(resp => {
        callback({
            count: resp.body.count
        })
    });
}

exports.countAround = (client, lat, lon, radius, callback) => {
    // TODO Compter le nombre d'anomalies autour d'un point géographique, dans un rayon donné
    client.count({
        index: indexName,
        body: {
            query: {
                range: {
                    timestamp: {
                        gte: lon,
                        lt: to
                    }
                }
            }
        }
    })
    .then(resp => {
        callback({
            count: resp.body.count
        })
    });
}