const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const indexName = config.get('elasticsearch.index_name');

async function run() {
    // Create Elasticsearch client
    const client = new Client({ node: config.get('elasticsearch.uri') });

    client.indices.create({
        index: indexName,
        body: {
            mappings: {
                properties: {
                    location: {
                        type: 'geo_point'
                    },
                },
            },
        },
    }, (err, resp) => {
        if (err) console.trace(err.message);
    });

    // TODO il y a peut être des choses à faire ici avant de commencer ... 
    let declarations = [];
    let total_declarations_count = 0;
    let inserted_declarations_count = 0;
    // Read CSV file
    fs.createReadStream('dataset/dans-ma-rue.csv')
        .pipe(csv({
            separator: ';'
        }))
        .on('data', (data) => {
            // TODO ici on récupère les lignes du CSV ...
            // TYPE;SOUSTYPE;ADRESSE;CODE_POSTAL;VILLE;ARRONDISSEMENT;DATEDECL;ANNEE DECLARATION;MOIS DECLARATION;NUMERO;PREFIXE;INTERVENANT;CONSEIL DE QUARTIER;OBJECTID;geo_shape;geo_point_2d
            declarations.push({
                '@timestamp': data.DATEDECL,
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
                location: data.geo_point_2d,
            });
        })
        .on('end', async () => {
            // TODO il y a peut être des choses à faire à la fin aussi ?
            total_declarations_count = declarations.length;
            const split_factor = 30;
            let length = Math.ceil(total_declarations_count / split_factor);

            for (let i = 0; i < split_factor; i++) {
                if (declarations.length < length) {
                    length = declarations.length
                }
                const current_batch = declarations.splice(0, length);
                const bulk_insert_query = createBulkInsertQuery(current_batch);
                await insert_declarations(client, bulk_insert_query);
            };

            client.close();
            console.log('\r\nTerminated!');
        });

    async function insert_declarations(client, bulk_insert_query) {
        try {
            const resp = await client.bulk(bulk_insert_query);
            const inserted_count = resp.body.items.length;
            inserted_declarations_count += inserted_count;
            printProgress()

        } catch (err) {
            console.trace(err);
        }
    }
    function printProgress() {
        const progress = Math.floor(inserted_declarations_count / total_declarations_count * 100);
        process.stdout.clearLine();
        process.stdout.cursorTo(0);
        process.stdout.write(`Documents insertion... (${progress}%)`);
    }
}


function createBulkInsertQuery(declarations) {
    const body = declarations.reduce((acc, declaration) => {
        const { annee_declaration, mois_declaration, type, sous_type, code_postal, ville, arrondissement, prefixe, intervenant, conseil_de_quartier, location } = declaration;
        acc.push({ index: { _index: indexName, _type: '_doc', _id: declaration.object_id } })
        acc.push({ '@timestamp': declaration['@timestamp'], annee_declaration, mois_declaration, type, sous_type, code_postal, ville, arrondissement, prefixe, intervenant, conseil_de_quartier, location })
        return acc
    }, []);

    return { body };
}

run().catch(console.error);
