const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.statsByArrondissement = (client, callback) => {
    client.search({
        index: indexName,
        body: {
            aggs: {
                ano_par_arrondissement: {
                    terms: {
                        field: "arrondissement.keyword",
                        size: 20
                    }
                }
            }
        }
    }).then(response => {
        const ano_par_arrondissement = response.body.aggregations.
            ano_par_arrondissement.buckets.
            map(object => {
                return {arrondissement: object.key, count: object.doc_count}
            });
        callback(ano_par_arrondissement);
    })
}

exports.statsByType = (client, callback) => {
    client.search({
        index: indexName,
        body: {
            aggs: {
                types: {
                    terms: {
                        field: "type.keyword",
                        size: 5
                    },
                    aggs: {
                        sous_types: {
                            terms: {
                                field: "sous_type.keyword",
                                size: 5
                            }
                        }
                    }
                }
            }
        }
    }).then(response => {
        const types = response.body.aggregations.
            types.buckets.
            map(type => {
                return {
                    type: type.key, 
                    count: type.doc_count,
                    sous_types: type.sous_types.buckets.
                        map(sous_type =>{
                            return {
                                sous_type: sous_type.key,
                                count: sous_type.doc_count
                            }
                        })
                }
            });
        callback(types);
    })
}

exports.statsByMonth = (client, callback) => {
    client.search({
        index: indexName,
        body: {
            aggs: {
                ano_par_mois: {
                    date_histogram: {
                        field: "timestamp",
                        calendar_interval: "month",
                        order: { _count: "desc" }
                    }
                }
            }
        }
    }).then(response => {
        const ano_par_mois = response.body.aggregations.
            ano_par_mois.buckets.slice(0,10).
            map(object => {
                let month_arr = object.key_as_string.split("-", 2);
                let month = month_arr[1]+"/"+month_arr[0];
                return {month: month, 
                    count: object.doc_count}
            });
        callback(ano_par_mois);
    })
}

exports.statsPropreteByArrondissement = (client, callback) => {
    client.search({
        index: indexName,
        body: {
            aggs: {
                ano_proprete_par_arrond: {
                    terms: {
                        field: "arrondissement.keyword",
                        order: {
                            proprete: "desc"
                        },
                        size: 3
                    },
                    aggs: {
                        proprete: {
                            filter: {
                                term: {
                                    type: "propreté"
                                }
                            }
                        }
                    }
                }
            }
        }
    }).then(response => {
        const ano_proprete_par_arrond = response.body.aggregations.
            ano_proprete_par_arrond.buckets.
            map(object => {
                return {arrondissement: object.key, 
                    count: object.proprete.doc_count}
            });
        callback(ano_proprete_par_arrond);
    })
}
