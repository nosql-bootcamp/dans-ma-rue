const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.statsByArrondissement = async (client, callback) => {
  let res = []
  const body = {
    aggs: {
      arrondissement: {
        terms: {
          field: "arrondissement.keyword"
        }
      }
    }
  }
  console.info(JSON.stringify(body, null, 4))
  try {
    res = (await client.search({
      index: 'dansmarue',
      body: body,
    }, {
      ignore: [404],
      maxRetries: 3
    })).body.aggregations.arrondissement.buckets
  } catch (e) {
    console.error(e.body)
  }
  callback(res);
}

exports.statsByType = async (client, callback) => {
  const res = []
  const body = {
    aggs: {
      type: {
        terms: {
          field: "type.keyword"
        }
      }
    }
  }
  console.info(JSON.stringify(body, null, 4))
  try {
    const types = (await client.search({
      index: 'dansmarue',
      body: body,
    }, {
      ignore: [404],
      maxRetries: 3
    })).body.aggregations.type.buckets
      .sort((a, b) => b.doc_count - a.doc_count)
      .slice(0, 5)
    console.info(types)
    for (const type of types) {
      const subBody = {
        query: {
          match: {
            type: type.key
          }
        },
        aggs: {
          soustype: {
            terms: {
              field: "soustype.keyword"
            }
          }
        }
      }
      const subtypes = (await client.search({
        index: 'dansmarue',
        body: subBody,
      }, {
        ignore: [404],
        maxRetries: 3
      })).body.aggregations.soustype.buckets
        .sort((a, b) => b.doc_count - a.doc_count)
        .slice(0, 5)
      curRes = {
        type: type.key,
        count: type.doc_count,
        sous_types: [],
      }
      for (const subtype of subtypes) {
        curRes.sous_types.push({
          sous_type: subtype.key,
          count: subtype.doc_count,
        })
      }
      res.push(curRes)
    }
  } catch (e) {
    console.error(e.body)
  }
  callback(res);
}

exports.statsByMonth = (client, callback) => {
  // TODO Trouver le top 10 des mois avec le plus d'anomalies
  callback([]);
}

exports.statsPropreteByArrondissement = (client, callback) => {
  // TODO Trouver le top 3 des arrondissements avec le plus d'anomalies concernant la propreté
  callback([]);
}
