const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const indexName = config.get('elasticsearch.index_name');

async function run () {
    // Create Elasticsearch client
    const client = new Client({ node: config.get('elasticsearch.uri') });

    if (client.indices.exists({ index: indexName })) {
        /* Delete index */
        try {
            await client.indices.delete({
                index: indexName,
            });
            console.log(`index ${indexName} deleted`);
        } catch(err) {
            console.trace(err.message);
        }
    }

    // Création de l'indice
    client.indices.create({ index: indexName }, (err, resp) => {
        if (err) console.trace(err.message);
        else{
            client.indices.putMapping({
                index: indexName,
                body: {
                    properties: { 
                        location: {type : 'geo_point'}
                    }
                }
            }, (err,resp, status) => {
                if (err) {
                  console.error(err, status);
                }
                else {
                    console.log('Successfully Created Mapping ', status, resp);
                }
            });
        }
    });

    

    let events = [];

    // Read CSV file
    fs.createReadStream('dataset/dans-ma-rue.csv')
        .pipe(csv({
            separator: ';'
        }))
        .on('data', (data) => {
            events.push({
                timestamp: data.DATEDECL,
                object_id: data.OBJECTID,
                annee_declaration: data["ANNEE DECLARATION"],
                mois_declaration: data["MOIS DECLARATION"],
                type: data.TYPE,
                sous_type: data.SOUSTYPE,
                code_postal: data.CODE_POSTAL,
                ville: data.VILLE,
                arrondissement: data.ARRONDISSEMENT,
                prefixe: data.PREFIXE,
                intervenant: data.INTERVENANT,
                conseil_de_quartier: data["CONSEIL DE QUARTIER"],
                location: data.geo_point_2d
            })
        })
        .on('end', async () => {
            const step = 10000;
            let nb_inserted = 0;
            console.log("Nombre d'éléments : ",events.length)
            for(let i = 0; i<events.length; i+=step){
                const resp = await client.bulk(createBulkInsertQuery(events.slice(i, i+step)))

                nb_inserted+=resp.body.items.length;
                console.log(`Inserted ${resp.body.items.length} events`);

                if(nb_inserted>=events.length){
                    client.close();
                    console.log("Total elems inserted : ", nb_inserted);
                    console.log('Terminated!');
                }
            }
        });
}

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery(events) {
    const body = events.reduce((acc, event) => {
        const { timestamp, annee_declaration, mois_declaration, type, sous_type, code_postal, ville, arrondissement, prefixe, intervenant, conseil_de_quartier, location } = event;
        acc.push({ index: { _index: indexName, _type: '_doc', _id: event.object_id } })
        acc.push({ timestamp, annee_declaration, mois_declaration, type, sous_type, code_postal, ville, arrondissement, prefixe, intervenant, conseil_de_quartier, location })
        return acc
    }, []);
  
    return { body };
}

run().catch(console.error);
