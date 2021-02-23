const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const {Client} = require('@elastic/elasticsearch');
const indexName = config.get('elasticsearch.index_name');

const toTimestamp = data => data["DATEDECL"]

const clean = (entry, val) => {
  if (!val) return null;

  return val
}

async function run() {
  // Create Elasticsearch client
  const client = new Client({node: config.get('elasticsearch.uri')});

  const DATA = []
  let totDataIns = 0
  // Read CSV file
  fs.createReadStream('dataset/dans-ma-rue.csv')
    .pipe(csv({
      separator: ';'
    }))
    .on('data', (data) => {
      DATA.push({"index": {"_index": "dansmarue"}});
      let entries = {"@timestamp": toTimestamp(data)}
      for (const entry of Object.keys(data).filter(k => k !== 'DATEDECL')) {
        entries[String(entry).replace(' ', '_').toLowerCase()] = clean(entry, data[entry])
      }
      DATA.push(entries);

    })
    .on('end', async () => {
      const nbDataByBulk = 1000
      const bulkSize = nbDataByBulk * 2
      console.info(`${DATA.length / 2} Data to insert`)

      try {
        await client.indices.delete({index: 'dansmarue', ignore_unavailable: true});
        await client.indices.create({
          index: 'dansmarue',
          body: {
            mappings: {
              properties: {
                geo_point_2d: {type: 'geo_point'},
              }
            }
          }
        }, {ignore: [400]})
      } catch (e) {
        console.error('indice creation error :', e.body)
      }
      for(let cursor = 0; cursor < DATA.length; cursor += bulkSize) {
        const [inf, sup] = [cursor, cursor + bulkSize]
        client.bulk({
          body: sup < DATA.length ? DATA.slice(inf, sup) : DATA.slice(inf)
        }, (err, res) => {
          if (err) throw new Error(`Bulk error : ${err}`)
          const {length} = res.body.items

          totDataIns += length
          console.info(`${length} data inserted`);
          console.info(`${totDataIns} data inserted in total`);
        });
      }

      console.info('Terminated!');
    })
}

run().catch(console.error);
