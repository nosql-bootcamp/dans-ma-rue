const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.count = (client, from, to, callback) => {
    // TODO Compter le nombre d'anomalies entre deux dates
    
    client.count({
                index: indexName,
                body: {
                    query:{ 
                        range:  {
                            timestamp: {
                                gte: from,
                                lt: to
                            }
                        }
                    }
                }
            }).then(resp => {
                callback({
                    count: resp.body.count
            })
        })
            
}
/* 
GET /dansmarue/_count
{
  "query": {
    "range": {
      "timestamp": {
        "gte": "2016-01-01",
        "lt": "2018-01-01"
      }
    }
  }
}
*/

exports.countAround = (client, lat, lon, radius, callback) => {
    // TODO Compter le nombre d'anomalies autour d'un point géographique, dans un rayon donné
    callback({
        count: 0
    })
}