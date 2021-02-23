const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const indexName = config.get('elasticsearch.index_name');

async function run () {
    // Create Elasticsearch client
    //const client = new Client({ node: config.get('elasticsearch.uri') });
    const client = new Client({ node: 'http://localhost:9200' });

    // TODO il y a peut être des choses à faire ici avant de commencer ... 
    // Création de l'indice
    client.indices.create({ index: indexName}, (err, resp) => {
        if (err) console.trace(err.message);
    });

    let alerts = [];

    // Read CSV file
    fs.createReadStream('dataset/dans-ma-rue.csv')
        .pipe(csv({
            separator: ';',
            trim: true
        }))
        .on('data', (data) => {
            // TODO ici on récupère les lignes du CSV ...
            alerts.push({
                timestamp: data.DATEDECL,
                object_id: data.OBJECTID,
                annee_declaration: data['ANNEE DECLARATION'],
                mois_declaration: data['MOIS DECLARATION'],
                type: data.TYPE,
                sous_type: data.SOUSTYPE,
                code_postal: data.CODE_POSTAL,
                ville: data.VILLE,
                arrondissement: data.ARRONDISSEMENT,
                prefixe: data.PREFIXE,
                intervenant: data.INTERVENANT,
                conseil_de_quartier: data['CONSEIL DE QUARTIER'],
                location: data.geo_point_2d
            })
            
            //console.log(data);
        })
        .on('end', () => {
            // TODO il y a peut être des choses à faire à la fin aussi ?
            var i,j,temparray,chunk = 50;
            for (i=0,j=alerts.length; i<j; i+=chunk) {
                temparray = alerts.slice(i,i+chunk);

                client.bulk(createBulkInsertQuery(temparray), (err, resp) => {
                    if (err) console.trace(err.message);
                    else console.log(`Inserted ${resp.body.items.length} ${indexName}`);
                    client.close(); // TODO move it ?
                  });
                console.log('Done : ' + i + "/" + alerts.length);
            }
            console.log('Terminated!');
        });

}

        function createBulkInsertQuery(alerts) {
            const body = alerts.reduce((acc, alert) => {
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
                    } = alert;
                
              acc.push({ index: { _index: indexName, _type: '_doc', _id: alert.alert_id } })
              acc.push({ timestamp,
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
                        })
              return acc
            }, []);
          
            return { body };
          }


run().catch(console.error);
