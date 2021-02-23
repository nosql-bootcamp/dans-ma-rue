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
        callback({count : res.body.count})
    })
    .catch(err => { 
        console.log(err)
    });
}

exports.countAround = (client, lat, lon, radius, callback) => {
    // TODO Compter le nombre d'anomalies autour d'un point géographique, dans un rayon donné
    callback({
        count: client
            .count({
                index: indexName,
                body: {
                    query: {
                        geo_distance: {
                            "distance": "12km",
                            "location": {
                                "lat": 40,//Problème, la position se trouve dans un seul champ commun, comment faire ?
                                "lon": -70
                            }
                        }
                    }
                }
            })
    })
}