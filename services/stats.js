const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.statsByArrondissement = (client, callback) => {
    //compte le nombre d'anomalies par arondissement
    res = client.search({
        index: indexName,
        body: {
            "size": 0,
            "aggs": {
                "count_by_arrondissement": {
                    "terms": {
                        "field": "arrondissement.keyword"
                    }
                }
            }
        }
    }).then(res => {
        let list = [];
        let buckets = res.body.aggregations.count_by_arrondissement.buckets;
        for (let bucket of buckets) {
            list.push({ "arrondissement": bucket.key, "count": bucket.doc_count });
        }
        callback(list)
    })
        .catch(err => {
            console.log(err)
        });
}

exports.statsByType = (client, callback) => {
    //trouve le top 5 des types et sous types d'anomalies
    res = client.search({
        index: indexName,
        body: {
            "size": 0,
            "aggs": {
                "count_by_type": {
                    "terms": {
                        "field": "type.keyword",
                        "size": 5
                    },
                    "aggs": {
                        "count_by_subtype": {
                            "terms": {
                                "field": "sous_type.keyword",
                                "size": 5
                            }
                        }
                    }

                }
            }
        }
    }).then(res => {
        let list = [];
        let buckets = res.body.aggregations.count_by_type.buckets;
        for (let bucket of buckets) {
            let subbuckets = bucket.count_by_subtype.buckets;
            let sublist = [];
            for (let subbucket of subbuckets) {
                sublist.push({ "sous_type": subbucket.key, "count": subbucket.doc_count });
            }
            list.push({ "type": bucket.key, "count": bucket.doc_count, "sous_types": sublist });
        }
        callback(list)
    })
        .catch(err => {
            console.log(err)
        });
}

exports.statsByMonth = (client, callback) => {
    //trouve le top 10 des mois avec le plus d'anomalies
    res = client.search({
        index: indexName,
        body: {
            "size": 0,
            "aggs": {
                "count_by_month": {
                    "date_histogram": {
                        "field": "timestamp",
                        "interval": "month",
                        "format": "MM/yyyy",
                        "order": {
                            "_key": "desc"
                        }
                    }
                }
            }
        }
    }).then(res => {
        let list = [];
        let buckets = res.body.aggregations.count_by_month.buckets;
        let cpt = 0;
        for (let bucket of buckets) {
            if (cpt >= 10) { break; }
            list.push({ "month": bucket.key_as_string, "count": bucket.doc_count });
            cpt++;
        }
        callback(list)
    })
        .catch(err => {
            console.log(err)
        });
}

exports.statsPropreteByArrondissement = (client, callback) => {
    // Trouve le top 3 des arrondissements avec le plus d'anomalies concernant la propreté
    res = client.search({
        index: indexName,
        body: {
            "size": 0,
            "query": {
                "term": {
                    "type.keyword": {
                        "value": "Propreté"
                    }
                }
            },
            "aggs": {
                "count_by_arrondissement": {
                    "terms": {
                        "field": "arrondissement.keyword"
                    }
                }
            }
        }
    }).then(res => {
        let list = [];
        let buckets = res.body.aggregations.count_by_arrondissement.buckets;
        let cpt = 0;
        for (let bucket of buckets) {
            if (cpt >= 3) { break; }
            list.push({ "arrondissement": bucket.key, "count": bucket.doc_count });
            cpt++;
        }
        callback(list)
    })
        .catch(err => {
            console.log(err)
        });
}
