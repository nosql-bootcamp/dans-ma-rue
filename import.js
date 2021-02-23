const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const { timeStamp } = require('console');
const indexName = config.get('elasticsearch.index_name');

async function run() {
    // Create Elasticsearch client
    const client = new Client({ node: config.get('elasticsearch.uri') });

    // TODO il y a peut être des choses à faire ici avant de commencer ... 
    await client.indices.create({
        index: 'dansmarue',
        body: {
            mappings: {
                properties: {
                    "timestamp": { type: 'date' },
                    "location": {type : 'geo_point'}
                }
            }
        }
    } );

    // Read CSV file
    let anomalies = [];
    fs.createReadStream('dataset/dans-ma-rue.csv')
        .pipe(csv({
            separator: ';'
        }))
        .on('data', (data) => {
            // TODO ici on récupère les lignes du CSV ...
            anomalies.push({
                "timestamp": data.DATEDECL,
                "object_id": data.OBJECTID,
                "annee_declaration": data.ANNEE_DECLARATION,
                "mois_declaration": data.MOIS_DECLARATION,
                "type": data.TYPE,
                "sous_type": data.SOUSTYPE,
                "code_postal": data.CODE_POSTAL,
                "ville": data.VILLE,
                "arrondissement": data.ARRONDISSEMENT,
                "prefixe": data.PREFIXE,
                "intervenant": data.INTERVENANT,
                "conseil_de_quartier": data.CONSEIL_DE_QUARTIER,
                "location": data.geo_point_2d
            });
        })
        .on('end', () => {
            // TODO il y a peut être des choses à faire à la fin aussi ?
            // bodies est un tableau de tableau ici, chaque sous tableau représente une des requêtes à envoyer
            const bodies = Object.values(createBulkInsertQuery(anomalies, 100));
            // mot clé async obligatoire pour utiliser await
            bodies.map(async body => {
                // await permet d'attendre jusqu'à ce que le bulk soit terminé
                await client.bulk({body}).catch(console.error)
                console.log("Inserted ", body.length)
            })
            console.log('Terminated!');
        });
}

function createBulkInsertQuery(anomalies, slice_number) {
    const body = anomalies.reduce((acc, anomalie) => {
        const { timestamp, annee_declaration, mois_declaration, type, sous_type, code_postal, ville, arrondissement, prefixe, intervenant, conseil_de_quartier, location } = anomalie;
        acc.push({ index: { _index: 'dansmarue', _type: '_doc', _id: anomalie.object_id } })
        acc.push({ timestamp, annee_declaration, mois_declaration, type, sous_type, code_postal, ville, arrondissement, prefixe, intervenant, conseil_de_quartier, location })
        return acc
    }, []);
    var result = []
    for (var i = 0; i < body.length; i++) {
        if (i % slice_number == 0) result.push([]);
        result[Math.floor(i / slice_number)].push(body[i]);
    }
    return result ;
}
run().catch(console.error);
