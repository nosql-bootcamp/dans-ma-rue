const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.statsByArrondissement = (client, callback) => {
    client.search({
        index: indexName,
        body: {
            size: 0,
            aggs: {
                arrondissement: {
                    terms: {
                        field: "arrondissement.keyword",
                        size: 10000
                    }
                }
            }
        }
    })
        .then(resp => {

            let buckets = resp.body.aggregations.arrondissement.buckets;

            ret = buckets.map(bucket => {
                bucket.arrondissement = bucket.key;
                bucket.count = bucket.doc_count;
                delete bucket.key;
                delete bucket.doc_count;
                return bucket;
            })

            callback(ret);
        })
}

exports.statsByType = (client, callback) => {
    client.search({
        index: indexName,
        body: {
            size: 0,
            aggs: {
                type: {
                    terms: {
                        field: "type.keyword",
                        size: 5
                    },
                    aggs: {
                        sous_type: {
                            terms: {
                                field: "sous_type.keyword",
                                size: 5
                            }
                        }
                    }
                }
            }
        }
    })
        .then(resp => {

            let buckets = resp.body.aggregations.type.buckets;

            ret = buckets.map(bucket => {
                bucket.type = bucket.key;
                bucket.count = bucket.doc_count;
                bucket.sous_type = bucket.sous_type.buckets

                bucket.sous_type.map(sous_type_bucket => {
                    sous_type_bucket.sous_type = sous_type_bucket.key;
                    sous_type_bucket.count = sous_type_bucket.doc_count;
                    delete sous_type_bucket.key;
                    delete sous_type_bucket.doc_count;
                })

                delete bucket.key;
                delete bucket.doc_count;
                return bucket;
            })

            callback(buckets);
        })
}

exports.statsByMonth = (client, callback) => {
    client.search({
        index: indexName,
        body: {
            size: 0,
            aggs: {
                byMonth: {
                    date_histogram: {
                        field: "timestamp",
                        calendar_interval: "1M",
                        format: "MM/yyyy",
                        order: { _count: "desc" }
                    }
                }
            }
        }
    })
        .then(resp => {

            let buckets = resp.body.aggregations.byMonth.buckets.slice(0, 10);

            ret = buckets.map(bucket => {
                bucket.month = bucket.key_as_string;
                bucket.count = bucket.doc_count;
                delete bucket.key;
                delete bucket.key_as_string;
                delete bucket.doc_count;
                return bucket;
            })

            callback(ret);
        })
}

exports.statsPropreteByArrondissement = (client, callback) => {
    client.search({
        index: indexName,
        body: {
            query: { 
                bool: { 
                  must: [
                    { match: { type:"Propreté"}}
                  ]
                }
              },
              size: 0,
              aggs: {
                  arrondissement: {
                      terms: {
                          field: "arrondissement.keyword",
                          size: 3
                      }
                  }
              }
        }
    })
        .then(resp => {

            let buckets = resp.body.aggregations.arrondissement.buckets;

            ret = buckets.map(bucket => {
                bucket.arrondissement = bucket.key;
                bucket.count = bucket.doc_count;
                delete bucket.key;
                delete bucket.doc_count;
                return bucket;
            })

            callback(ret);
        })
}
