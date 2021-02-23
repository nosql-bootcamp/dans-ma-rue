const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.statsByArrondissement = (client, callback) => {
    // TODO Compter le nombre d'anomalies par arondissement
    client.search({
        index: indexName,
        body: {
            size: 0,
            aggs: {
                'Term_Aggregation': {
                    terms : { field: 'arrondissement.keyword' }
                }
            }
            
        }
    }).then(resp => {
        callback(resp.body.aggregations['Term_Aggregation'].buckets
    )
    })
}

/* POST /alert/_search
{
    "size": 0,
    "aggs": {
      "Term_Aggregation" : {
        "terms" :
        { "field" : "arrondissement.keyword" }
      }
    }
} */

exports.statsByType = (client, callback) => {
    // TODO Trouver le top 5 des types et sous types d'anomalies
    client.search({
        index: indexName,
        body: {
            size: 0,
            aggs: {
                'Agg_Type': {
                    terms: {
                        field: 'type.keyword',
                        size: 5,
                        order: { _count: 'desc'}
                    },
                    aggs: {
                        'Agg_SousType': {
                            terms: {
                                field: 'sous_type.keyword',
                                size: 5,
                                order: { _count: 'desc'}
                            }
                        }
                    }
                }
            }
        }
    }).then(resp => {callback(resp.body.aggregations.Agg_Type.buckets
    )
    })
}

/*POST /alert/_search
{
    "size": 0, 
    "aggs": {
      "Agg_Type" : {
        "terms": {
          "field": "type.keyword",
          "size": 5,
          "order": { "_count": "desc" }
        },
        "aggs":{
          "Agg_SousType" : {
            "terms": {
              "field": "sous_type.keyword",
              "size": 5,
              "order": { "_count": "desc" }
            }
          }
        }
      }
    }
}*/

exports.statsByMonth = (client, callback) => {
    // TODO Trouver le top 10 des mois avec le plus d'anomalies
    client.search({
                index: indexName,
                body: {
                    aggs: {
                        'Agg_MoisDeclaration': {
                            terms: {
                                field: 'mois_declaration.keyword',
                                size: 10,
                                order: { _count: 'desc' }
                            },
                            aggs: {
                                'Agg_AnneeDeclaration': {
                                    terms: {
                                        field: 'annee_declaration.keyword',
                                        size: 1,
                                        order: { _count: 'desc' }
                                    }
                                }
                            }
                        }
                    }
                }
            }).then(resp => {callback(resp.body.aggregations['Agg_MoisDeclaration'].buckets
                )}
                )
}

/* POST /alert/_search
{
    "aggs": {
      "Agg_MoisDeclaration" : {
        "terms": {
          "field": "mois_declaration.keyword",
          "size": 10,
          "order": { "_count": "desc" }
        },
        "aggs":{
          "Agg_AnneeDeclaration" : {
            "terms": {
              "field": "annee_declaration.keyword",
              "size": 1,
              "order": { "_count": "desc" }
            }
          }
        }
      }
      }
    } */

exports.statsPropreteByArrondissement = (client, callback) => {
    // TODO Trouver le top 3 des arrondissements avec le plus d'anomalies concernant la propreté
    callback([]);
}
