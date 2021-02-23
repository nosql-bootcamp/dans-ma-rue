const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.statsByArrondissement = (client, callback) => {
    client.search({
        index: indexName,
        body: {
            aggs: {
                arrondissements: {
                    terms: {
                        field: "arrondissement.keyword",
                        size: 20,
                        order: {
                            "_count": "desc"
                        }
                    },
                }
            }
        }
    }).then(resp => {
        let res = resp.body.aggregations.arrondissements.buckets.map(element => {
            return {
                arrondissement : element.key,
                count : element.doc_count
            }
        })
        callback(res)
    });
}

exports.statsByType = (client, callback) => {
    client.search({
        index: indexName,
        body: {
            aggs: {
                types: {
                    terms: {
                        field: "type.keyword",
                        size: 50
                    },
                    aggs: {
                        sous_types: {
                            terms: {
                                field: "sous_type.keyword",
                                size: 5,
                                order: {
                                    "_count": "desc"
                                }
                            }
                        }
                    }
                }
            }
        }
    }).then(resp => {
        let res = resp.body.aggregations.types.buckets.map(element => {
            return {
                type : element.key,
                count : element.doc_count,
                sous_types : element.sous_types.buckets.map((sousType) => {
                    return {
                        sous_type : sousType.key,
                        count : sousType.doc_count
                    }
                })
            }
        })
        callback(res)
    });
}

exports.statsByMonth = (client, callback) => {
    client.search({
        index: indexName,
        body: {
            size: 0,
            aggs: {
                annee: {
                    date_histogram: {
                        field: "timestamp",
                        interval: "month",
                        order: {
                            _count: "desc"
                        },
                        format: "MM-yyyy"
                    },
                    aggs: {
                        top10: {
                            bucket_sort: {
                                sort: [],
                                size: 10
                            }
                        }
                    }
                }
            }
        }
    }).then(resp => {
        let res = resp.body.aggregations.annee.buckets.map(element => {
            return {
                month : element.key_as_string,
                count : element.doc_count,
            }
        })
        callback(res)
    });
}

exports.statsPropreteByArrondissement = (client, callback) => {
    // TODO Trouver le top 3 des arrondissements avec le plus d'anomalies concernant la propreté
    client.search({
        index: indexName,
        body: {
            size: 0,
            aggs:
            {
                arrondissements:
                {
                    terms:
                    {
                        field: "arrondissement.keyword",
                        order: {
                            "proprete": "desc"
                        },
                        size: 3
                    },
                    aggs: {
                        proprete: {
                            filter: {
                                term: {
                                    "type": "propreté"
                                }
                            }
                        }
                    }
                }
            }
        }
    }).then(resp => {
        let res = resp.body.aggregations.arrondissements.buckets.map(element => {
            return {
                arrondissement : element.key,
                count : element.proprete.doc_count,
            }
        })
        callback(res)
    });
}
