const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.statsByArrondissement = (client, callback) => {
    client
        .search({
            index: indexName,
            body: {
                aggs: {
                    arrondissements: {
                        terms: {
                            field: 'arrondissement.keyword',
                            size: 30
                        },
                    }
                }
            }
        })
        .then(resp => {
            callback(resp.body.aggregations.arrondissements.buckets.map(a => {
                return { arrondissement: a.key, count: a.doc_count };
            }))
        })
        .catch(err => {
            console.log(err.message);
        });
}

exports.statsByType = (client, callback) => {
    client
        .search({
            index: indexName,
            body: {
                aggs: {
                    types: {
                        terms: {
                            field: 'type.keyword',
                            size: 5
                        },
                        aggs: {
                            sous_types: {
                                terms: {
                                    field: 'sous_type.keyword',
                                    size: 5
                                }
                            }
                        }
                    }
                }
            }
        })
        .then(resp => {
            let types = resp.body.aggregations.types.buckets.map(t => {
                return {
                    type: t.key,
                    count: t.doc_count,
                    sous_types: t.sous_types.buckets.map(st => {
                        return {
                            sous_type: st.key,
                            count: st.doc_count
                        };
                    })
                };
            });


            callback(types);
        })
        .catch(err => {
            console.log(err.message);
        });
}

exports.statsByMonth = (client, callback) => {
    client
        .search({
            index: indexName,
            body: {
                aggs: {
                    group_by_month: {
                        date_histogram: {
                            field: "@timestamp",
                            calendar_interval: "month",
                        },
                    },
                },
            },
        })
        .then((resp) => {
            const top_10_months = resp.body.aggregations.group_by_month.buckets.reverse().splice(0, 10);

            callback(
                top_10_months.map((a) => {
                    let formattedDate = new Date(a.key_as_string);

                    let month = (formattedDate.getMonth() + 1) + ""
                    if (month.length == 1) month = "0" + month

                    let monthAndYear = month + "/" + formattedDate.getFullYear();

                    return { month: monthAndYear, count: a.doc_count };
                })
            );
        })
        .catch(err => {
            console.log(err.message);
        });
};

exports.statsPropreteByArrondissement = (client, callback) => {
    client
        .search({
            index: indexName,
            body: {
                query: {
                    match: {
                        type: 'Propreté'
                    }
                },
                aggs: {
                    arrondissements: {
                        terms: {
                            field: 'arrondissement.keyword',
                            size: 3
                        },
                    }
                }
            }
        })
        .then(resp => {
            callback(resp.body.aggregations.arrondissements.buckets.map(a => {
                return { arrondissement: a.key, count: a.doc_count };
            }))
        })
        .catch(err => {
            console.log(err.message);
        });
}
