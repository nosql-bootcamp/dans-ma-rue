const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const indexName = config.get('elasticsearch.index_name');


async function run() {
  // Create Elasticsearch client
  const client = new Client({ node: config.get('elasticsearch.uri') });

  // Création de l'indice
  client.indices.delete({index: indexName})
  client.indices.create({
    index: indexName,
    body: {
      "mappings": {
        "properties": {
          "timestamp": {
            "type": "date",
            "format": "yyyy-MM-dd"
          }, "location": {
            "type": "geo_point"
          }
        }
      }
    }
  }, (err, resp) => {
    if (err) console.trace(err.message);
  });


    let anomalies = [];
    // Read CSV file
    fs.createReadStream('dataset/dans-ma-rue.csv')
        .pipe(csv({
            separator: ';'
        }))
        .on('data', (data) => {
            //console.log(data);
            anomalies.push({
                "timestamp" : data.DATEDECL,
                "object_id" : data.NUMERO,
                "annee_declaration" : data['ANNEE DECLARATION'],
                "mois_declaration" : data['MOIS DECLARATION'],
                "type" : data.TYPE,
                "sous_type" : data.SOUSTYPE,
                "code_postal" : data.CODE_POSTAL,
                "ville" : data.VILLE,
                "arrondissement" : data.ARRONDISSEMENT,
                "prefixe" : data.PREFIXE,
                "intervenant" : data.INTERVANANT,
                "conseil_de_quartier" : data['CONSEIL DE QUARTIER'],
                "location" : data.geo_point_2d
            });
        })
        .on('end', async () => {
            console.log(anomalies.length)
            while(anomalies.length>0){
                anms = []
                while (anms.length<1000 && anomalies.length>0){
                    anms.push(anomalies.pop());
                }
                let result = await client.bulk(createBulkInsertQuery(anms)).catch(console.error)
                if(result.body.errors || result.statusCode!=200){
                  console.log(result)
                }
            }
            client.close();
        });

}

function createBulkInsertQuery(anomalies) {
  const body = anomalies.reduce((acc, anomalie) => {
    const {
      timestamp,
      object_id,
      annee_declaration,
      mois_declaration,
      type,
      sous_type,
      code_postal,
      ville,
      arrondissement,
      prefixe,
      intervenant,
      conseil_de_quartier,
      location
    } = anomalie;
    acc.push({ index: { _index: indexName, _type: '_doc', _id: object_id } });
    res = {
      timestamp,
      annee_declaration,
      mois_declaration,
      type,
      sous_type,
      code_postal,
      ville,
      arrondissement,
      prefixe,
      intervenant,
      conseil_de_quartier,
      location
    };
    acc.push(res)
    return acc
  }, []);

  return { body };
}

run().catch(console.error);
