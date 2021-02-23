const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const indexName = config.get('elasticsearch.index_name');

const toTimestamp = data => {
    const date = data["DATEDECL"];
    const [year, month, day] = date.split("-").map(e => Number(e))

    return new Date(year, month, day, 0, 0);
}

const clean = val => {
    if (!val) return null;

    const cleaned = val.replace("<", "").replace("ND", "");
    return cleaned === "" ? null : parseInt(cleaned);
}

async function run () {
    // Create Elasticsearch client
    const client = new Client({ node: config.get('elasticsearch.uri') });

    const DATA = []
    let totDataIns = 0
    // Read CSV file
    fs.createReadStream('dataset/dans-ma-rue.csv')
        .pipe(csv({
            separator: ';'
        }))
        .on('data', (data) => {
            DATA.push({ "index" : { "_index" : "dansmarue", "_type" : "entry" } });
            let entries = {"@timestamp": toTimestamp(data)}
            for (const entry of Object.keys(data)) {
                entries[String(entry).replace(' ', '_').toLowerCase()] = clean(data[entry])
            }
            DATA.push(entries);

        })
        .on('end', () => {
            const sliceLen = Math.trunc(DATA.length / 5)
            for (let i = 0; i < 4; i++) {
                client.bulk({
                    body: DATA.slice(i*sliceLen, i!==5 ? (i+1)*sliceLen: DATA.length)
                }, (err, resp) => {
                    if (err) { throw err; }
                    const {length} = resp.body.items
                    totDataIns += length
                    console.info(`${length} data inserted`);
                    console.info(`${totDataIns} data inserted in total`);
                });
            }
            console.info('Terminated!');
        });
}

run().catch(console.error);
