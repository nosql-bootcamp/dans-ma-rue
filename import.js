const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const indexName = config.get('elasticsearch.index_name');

async function run() {
    // Create Elasticsearch client
    const client = new Client({ node: config.get('elasticsearch.uri') });

    if(client.indices.exists({ index: indexName})) {
        client.indices.delete({ index: indexName }, (err, resp) => {
            if (err) console.trace(err.message);})
    }
    
    client.indices.create({ index: indexName }, (err, resp) => {
        if (err) console.trace(err.message);
        else {
            client.indices.putMapping({
                index: indexName,
                body: {
                    properties: {
                        location: { type: 'geo_point' }
                    }
                }
            });
        }
    });
    // client.indices.delete({ index: indexName }).then(() => {
    //     client.indices.create({ index: indexName }, (err, resp) => {
    //         if (err) console.trace(err.message);
    //         else {
    //             client.indices.putMapping({
    //                 index: indexName,
    //                 body: {
    //                     properties: {
    //                         location: { type: 'geo_point' }
    //                     }
    //                 }
    //             });
    //         }
    //     });
    // })

    let anomalies = [];
    // Read CSV file
    fs.createReadStream('dataset/dans-ma-rue.csv')
        .pipe(csv({
            separator: ';'
        }))
        .on('data', (data) => {
            anomalies.push({
                timestamp: data["DATEDECL"],
                object_id: data["OBJECTID"],
                annee_declaration: data["ANNEE DECLARATION"],
                mois_declaration: data["MOIS DECLARATION"],
                type: data["TYPE"],
                sous_type: data["SOUSTYPE"],
                code_postal: data["CODE_POSTAL"],
                ville: data["VILLE"],
                arrondissement: data["ARRONDISSEMENT"],
                prefixe: data["PREFIXE"],
                intervenant: data["INTERVENANT"],
                conseil_de_quartier: data["CONSEIL DE QUARTIER"],
                location: data["geo_point_2d"]
            });
        }) //GOAL : 856101
        .on('end', () => {
            // bodies est un tableau de tableau ici, chaque sous tableau représente une des requêtes à envoyer
            const bodies = createBulkInsertQuery(anomalies, 500);
            // mot clé async obligatoire pour utiliser await
            bodies.map(async body => {
                // await permet d'attendre jusqu'à ce que le bulk soit terminé
                await client.bulk({ body }).catch(console.error)
                console.log("Inserted ", body.length)
            })
        })
}

run().catch(console.error);

function createBulkInsertQuery(anomalies, count) {
    anos = split(anomalies, count)
    bodies = []
    anos.forEach(anomalies => {
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
            acc.push({ index: { _index: indexName, _type: '_doc', _id: anomalie.object_id } })
            acc.push({
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
            })
            return acc
        }, []);
        bodies.push(body)
    });

    return bodies;
}

function split(array, n) {
    var res = [];
    while (array.length) {
        res.push(array.splice(0, n));
    }
    return res;
}
