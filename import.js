const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const indexName = config.get('elasticsearch.index_name');

async function run () {
    // Create Elasticsearch client
    const client = new Client({ node: config.get('elasticsearch.uri') });

    // Suppression de l'index si il existe
    if(client.indices.exists({ index: indexName})) {
        client.indices.delete({ index: indexName }, (err, resp) => {
        
            if (err) console.trace(err.message);})
    }

    // Création de l'indice
    client.indices.create({ index: indexName }, (err, resp) => {
        
        if (err) console.trace(err.message);

        client.indices.putMapping({  
            index: indexName,
            body: {
              properties: {
                'location': {
                  'type': 'geo_point'
                },
              }
            }
          },function(err,resp,status){
            if (err) {
              console.log(err);
            }
        });
    });

    let anos = [];

    // Read CSV file
    fs.createReadStream('dataset/dans-ma-rue.csv')
        .pipe(csv({
            separator: ';'
        }))
        .on('data', (data) => {
            anos.push({
                timestamp : data.DATEDECL,
                object_id : data.OBJECTID,
                annee_declaration : data["ANNEE DECLARATION"],
                mois_declaration : data["MOIS DECLARATION"],
                type : data.TYPE,
                sous_type : data.SOUSTYPE,
                code_postal : data.CODE_POSTAL,
                ville : data.VILLE,
                arrondissement : data.ARRONDISSEMENT,
                prefixe : data.PREFIXE,
                intervenant : data.INTERVENANT,
                conseil_de_quartier : data['CONSEIL DE QUARTIER'],
                location : data.geo_point_2d
            });
            
            // console.log(data);
        })
        .on('end', () => {
            
            ano_array = chunkArray(anos, 100);
            
            ano_array.forEach(array => {
                client.bulk(createBulkInsertQuery(array), (err, resp) => {
                    if (err) console.trace(err.message);
                    else console.log(`Inserted ${resp.body.items.length} anos`);
                });
            }, () => {
                client.close();
                console.log('Terminated!');
            });

        });
}

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery(anos) {
    const body = anos.reduce((acc, ano) => {
      const { timestamp, annee_declaration, mois_declaration, type, sous_type, code_postal, ville, arrondissement, prefixe, intervenant, conseil_de_quartier, location } = ano;
      acc.push({ index: { _index: indexName, _type: '_doc', _id: ano.object_id } })
      acc.push({ timestamp, annee_declaration, mois_declaration, type, sous_type, code_postal, ville, arrondissement, prefixe, intervenant, conseil_de_quartier, location })
      return acc
    }, []);
  
    return { body };
}

run().catch(console.error);

function chunkArray(myArray, chunk_size){
    var results = [];
    
    while (myArray.length) {
        results.push(myArray.splice(0, chunk_size));
    }
    
    return results;
}
