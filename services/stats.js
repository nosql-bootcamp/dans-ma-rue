const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.statsByArrondissement = (client, callback) => {
    // TODO Compter le nombre d'anomalies par arondissement
    client.search({
        index: 'dansmarue',
        body: {
            aggs: {
                result: {
                    terms: {
                        field: "arrondissement.keyword"
                    }
                }
            }
        }
    }).then(resp => {
        var resultArray = resp.body.aggregations.result.buckets.map(function (elm) {
            return { arrondissement: elm["key"], count: elm["doc_count"] };
        });
        callback(resultArray)
    });
}

exports.statsByType = (client, callback) => {
    // TODO Trouver le top 5 des types et sous types d'anomalies

    client.search({
        index: 'dansmarue',
        body: {
            "size": 0,
            "aggs": {
                "type": {
                    "terms": {
                        "field": "type.keyword",
                        "size": 5
                    },
                    "aggs": {
                        "sous_type": {
                            "terms": {
                                "field": "sous_type.keyword",
                                "size": 5
                            }
                        }
                    }

                }
            }
        }
    }).then(resp => {
        callback(resp.body.aggregations.type.buckets);
    });
}

exports.statsByMonth = (client, callback) => {
    // TODO Trouver le top 10 des mois avec le plus d'anomalies
    client.search({
        index: 'dansmarue',
        body: {
            "size": 0,
            "aggs": {
              "timestamp": {
                "date_histogram": {
                  "field": "timestamp",
                  "calendar_interval": "month",
                  "min_doc_count": 1
                }
              }
            }
        }
    }).then(resp => {
        var resultArray = resp.body.aggregations.timestamp.buckets.map(function (elm) {
            return { month: elm["key_as_string"], count: elm["doc_count"] };
        });
        resultArray.sort(function(first, second) {
            return second.count - first.count;
           });
        callback(resultArray.slice(0,10))
    });
}

exports.statsPropreteByArrondissement = (client, callback) => {
    // TODO Trouver le top 3 des arrondissements avec le plus d'anomalies concernant la propreté
    client.search({
        index: 'dansmarue',
        body: {
            "query": {
                "match": {
                    "type": {
                        "query": "Propreté"
                    }
                }

            },
            "aggs": {
                "result": {
                    "terms": {
                        "field": "arrondissement.keyword",
                        "size": 3
                    }
                }
            }
        }
    }).then(resp => {
        var resultArray = resp.body.aggregations.result.buckets.map(function (elm) {
            return { arrondissement: elm["key"], count: elm["doc_count"] };
        });
        callback(resultArray);
    });
}
