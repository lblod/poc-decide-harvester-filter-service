import { sparqlEscapeUri, sparqlEscapeString, sparqlEscapeDateTime, uuid, } from "mu";
import { updateSudo as update, querySudo as query } from '@lblod/mu-auth-sudo';

import {
    TASK_TYPE,
    PREFIXES,
    STATUS_BUSY,
    STATUS_FAILED,
    ERROR_URI_PREFIX,
    TASK_FILTER,
    ERROR_TYPE,
    connectionOptions,
} from "../constant";
import { parseResult } from "./super-utils";


export async function waitForDatabase() {
    const maxRetries = 30;
    const delayMs = 2000;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            await query('ASK { ?s ?p ?o }');
            console.log('Database connection established');
            return;
        } catch {
            console.log(`Waiting for database... (attempt ${attempt}/${maxRetries})`);
            if (attempt === maxRetries) {
                throw new Error(
                    `Failed to connect to database after ${maxRetries} attempts`,
                );
            }
            await new Promise((resolve) => setTimeout(resolve, delayMs));
        }
    }
}

export async function failBusyTasks() {
    await waitForDatabase();
    const queryStr = `
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    DELETE {
        GRAPH ?g {
        ?task adms:status ${sparqlEscapeUri(STATUS_BUSY)} .
        ?task dct:modified ?modified.
        }
    }
    INSERT {
    GRAPH ?g {
       ?task adms:status ${sparqlEscapeUri(STATUS_FAILED)} .
       ?task dct:modified ${sparqlEscapeDateTime(new Date())}.
       }
    }
    WHERE {
       GRAPH ?g {
        ?task a ${sparqlEscapeUri(TASK_TYPE)};
              adms:status ${sparqlEscapeUri(STATUS_BUSY)};
              task:operation ${sparqlEscapeUri(TASK_FILTER)}.
        OPTIONAL { ?task dct:modified ?modified. }
        }
    }
   `;
    try {
        await update(queryStr, connectionOptions);
    } catch (e) {
        console.warn(`WARNING: failed to move busy tasks to failed status on startup.`, e);
    }
}

export async function isTask(subject) {
    const queryStr = `
   ${PREFIXES}
   SELECT ?subject WHERE {
      BIND(${sparqlEscapeUri(subject)} as ?subject)
      ?subject a ${sparqlEscapeUri(TASK_TYPE)}.
   }
  `;
    const result = await query(queryStr, connectionOptions);
    return result.results.bindings.length;
}

export async function loadTask(subject) {
    const queryTask = `
   ${PREFIXES}
   SELECT DISTINCT ?graph ?task ?inputContainer ?id ?job ?created ?modified ?status ?index ?operation ?error WHERE {
     GRAPH ?graph {
      BIND(${sparqlEscapeUri(subject)} as ?task)
      ?task a ${sparqlEscapeUri(TASK_TYPE)}.
      ?task dct:isPartOf ?job;
                    mu:uuid ?id;
                    dct:created ?created;
                    dct:modified ?modified;
                    adms:status ?status;
                    task:index ?index;
                    task:inputContainer ?inputContainer;

                    task:operation ${sparqlEscapeUri(TASK_FILTER)}.

      OPTIONAL { ?task task:error ?error. }
     }
    }
  `;

    const task = parseResult(await query(queryTask, connectionOptions))[0];

    return task;
}
export async function getHarvestCollectionForTask(task) {
    const queryStr = `
    PREFIX tasks: <http://redpencil.data.gift/vocabularies/tasks/>
    SELECT ?collection
    WHERE {
        <${task.task}> tasks:inputContainer ?inputContainer.
        ?inputContainer tasks:hasHarvestingCollection ?collection.
    }
    `;
    const collection = parseResult(await query(queryStr, connectionOptions));
    if (!collection?.length) {
        return null;
    }
    return collection[0];
}

export async function getRemoteDataObjects(task, collection) {
    const queryStr = `
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    PREFIX    mu:   <http://mu.semte.ch/vocabularies/core/>
    PREFIX    nie:  <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
    PREFIX    dct:  <http://purl.org/dc/terms/>
    PREFIX    nfo:  <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX    nuao: <http://www.semanticdesktop.org/ontologies/2010/01/25/nuao#>
    PREFIX    ext:  <http://mu.semte.ch/vocabularies/ext/>
    SELECT DISTINCT ?dataObject ?taskType ?uuid 
    WHERE {
        <${collection.collection}> dct:hasPart ?dataObject.
        ?dataObject a nfo:RemoteDataObject;
             mu:uuid ?uuid;
             nie:url ?taskType.
    }
`;
    const rdo = parseResult(await query(queryStr, connectionOptions));
    return rdo;
}

export async function updateTaskStatus(task, status, extMetadataUri = undefined) {
    await update(
        `
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX ext:  <http://mu.semte.ch/vocabularies/ext/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX dct: <http://purl.org/dc/terms/>
    DELETE {
      GRAPH ?g {
        ?subject adms:status ?status .
        ?subject dct:modified ?modified.
        ${extMetadataUri ? `?subject ext:metadata ?metadata.` : ''}
      }
    }
    INSERT {
       GRAPH ?g {
       ?subject adms:status ${sparqlEscapeUri(status)}.
       ?subject dct:modified ${sparqlEscapeDateTime(new Date())}.
       ${extMetadataUri ? `?subject ext:metadata ${sparqlEscapeUri(extMetadataUri)}.` : ''}
       }
    }
    WHERE {
        GRAPH ?g {
            BIND(${sparqlEscapeUri(task.task)} as ?subject)
            ?subject adms:status ?status .
            OPTIONAL { ?subject dct:modified ?modified. }
            OPTIONAL { ${extMetadataUri ? `?subject ext:metadata ?metadata.` : ''} }
        }
    }
  `,
        connectionOptions,
    );
}

export async function appendTaskResultGraph(task, container, graphUri) {
    // prettier-ignore
    const queryStr = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    INSERT DATA {
       GRAPH <${task.graph}> {
        ${sparqlEscapeUri(container.uri)} a nfo:DataContainer.
        ${sparqlEscapeUri(container.uri)} mu:uuid ${sparqlEscapeString(container.id)}.
        ${sparqlEscapeUri(container.uri)} task:hasGraph ${sparqlEscapeUri(graphUri)}.

        ${sparqlEscapeUri(task.task)} task:resultsContainer ${sparqlEscapeUri(container.uri)}.
        }
    }
  `;

    await update(queryStr, connectionOptions);
}
export async function appendTaskResultFile(task, container, file) {
    return update(
        `
    ${PREFIXES}
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(task.graph)} {
        ${sparqlEscapeUri(container.uri)}
          a nfo:DataContainer ;
          mu:uuid ${sparqlEscapeString(container.id)} ;
          task:hasFile ${sparqlEscapeUri(file)} .
        ${sparqlEscapeUri(task.task)}
          task:resultsContainer ${sparqlEscapeUri(container.uri)} .
      }
    }`,
        {},
        connectionOptions,
    );
}

export async function appendTaskError(task, errorMsg) {
    const id = uuid();
    const uri = ERROR_URI_PREFIX + id;

    const queryError = `
   ${PREFIXES}
   INSERT DATA {
      GRAPH ${sparqlEscapeUri(task.graph)} {

      ${sparqlEscapeUri(uri)} a ${sparqlEscapeUri(ERROR_TYPE)};
        mu:uuid ${sparqlEscapeString(id)};
        oslc:message ${sparqlEscapeString(errorMsg)}.
      ${sparqlEscapeUri(task.task)} task:error ${sparqlEscapeUri(uri)}.
   }}
  `;

    await update(queryError, connectionOptions);
}
