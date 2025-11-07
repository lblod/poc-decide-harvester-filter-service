# harvesting filter decide service

This service takes the output graph of the consumer step and filters only
the useful data. the output of this step will be a file, just for the example.

## Usage

Add the following to your docker-compose file:

```yml
harvester-consumer-service:
  image: lblod/poc-decide-harvester-consumer-service
  environment:
            DCR_START_FROM_DELTA_TIMESTAMP: 2025-09-01T00:00:00
            DCR_SYNC_BASE_URL: https://lokaalbeslist-harvester-1.s.redhost.be/
            DCR_SYNC_FILES_PATH: /sync/besluiten/files
            DCR_SYNC_DATASET_SUBJECT: http://data.lblod.info/datasets/delta-producer/dumps/lblod-harvester/BesluitenCacheGraphDump
            TARGET_GRAPH: "http://mu.semte.ch/graphs/public"
            HTTP_MAX_QUERY_SIZE_BYTES: 120000
            DCR_BATCH_SIZE: 2000
            HIGH_LOAD_DATABASE_ENDPOINT: http://triplestore:8890/sparql
   links:
    - database:database
```

Add the delta rule:

```json
{
  "match": {
    "predicate": {
      "type": "uri",
      "value": "http://www.w3.org/ns/adms#status"
    },
    "object": {
      "type": "uri",
      "value": "http://redpencil.data.gift/id/concept/JobStatus/scheduled"
    }
  },
  "callback": {
    "method": "POST",
    "url": "http://harvester-consumer-service/delta"
  },
  "options": {
    "resourceFormat": "v0.0.1",
    "gracePeriod": 1000,
    "ignoreFromSelf": true,
    "foldEffectiveChanges": true
  }
}
```
