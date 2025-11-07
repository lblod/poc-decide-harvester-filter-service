export const TASK_FILTER = "http://lblod.data.gift/id/jobs/concept/TaskOperation/decide-filter";

export const STATUS_BUSY = "http://redpencil.data.gift/id/concept/JobStatus/busy";
export const STATUS_SCHEDULED = "http://redpencil.data.gift/id/concept/JobStatus/scheduled";
export const STATUS_SUCCESS = "http://redpencil.data.gift/id/concept/JobStatus/success";
export const STATUS_FAILED = "http://redpencil.data.gift/id/concept/JobStatus/failed";

export const JOB_TYPE = "http://vocab.deri.ie/cogs#Job";
export const TASK_TYPE = "http://redpencil.data.gift/vocabularies/tasks/Task";
export const ERROR_TYPE = "http://open-services.net/ns/core#Error";
export const ERROR_URI_PREFIX = "http://redpencil.data.gift/id/jobs/error/";
export const connectionOptions = {
    // scope: "http://services.redpencil.io/decide-consumer-service"
};
export const PREFIXES = `
  PREFIX harvesting: <http://lblod.data.gift/vocabularies/harvesting/>
  PREFIX terms: <http://purl.org/dc/terms/>
  PREFIX prov: <http://www.w3.org/ns/prov#>
  PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
  PREFIX dbpedia: <http://dbpedia.org/resource/>
  PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX oslc: <http://open-services.net/ns/core#>
  PREFIX cogs: <http://vocab.deri.ie/cogs#>
  PREFIX adms: <http://www.w3.org/ns/adms#>
`;

export const HIGH_LOAD_DATABASE_ENDPOINT =
    process.env.HIGH_LOAD_DATABASE_ENDPOINT || "http://database:8890/sparql";
export const TARGET_GRAPH = process.env.TARGET_GRAPH || "http://mu.semte.ch/graphs/public";

export const PUBLISHER_URI =
    process.env.PUBLISHER_URI || "http://data.lblod.info/services/decide-consumer-service";

export const DEFAULT_GRAPH = process.env.DEFAULT_GRAPH || "http://mu.semte.ch/graphs/harvesting";

export const BATCH_SIZE = parseInt(process.env.DCR_BATCH_SIZE) || 100;


