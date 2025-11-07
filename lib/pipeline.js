import { uuid } from "mu";
import { querySudo as query } from "@lblod/mu-auth-sudo";

import {
    STATUS_BUSY,
    STATUS_SUCCESS,
    STATUS_FAILED,
    PREFIXES,
    HIGH_LOAD_DATABASE_ENDPOINT,
} from "../constant";
import { loadTask, updateTaskStatus, appendTaskResultFile, appendTaskResultGraph, appendTaskError } from "./task";
import { appendTempFile, makeEmptyFile, writeFile } from "./file-helper";
import { toTermObjectArray } from "./super-utils";

export async function run(deltaEntry) {
    const task = await loadTask(deltaEntry);
    if (!task) return;
    try {
        await updateTaskStatus(task, STATUS_BUSY);
        const graphContainer = { id: uuid() };
        graphContainer.uri = `http://redpencil.data.gift/id/dataContainers/${graphContainer.id}`;
        const fileContainer = { id: uuid() };
        fileContainer.uri = `http://redpencil.data.gift/id/dataContainers/${fileContainer.id}`;

        let tempFileResult = "/share/" + uuid() + ".ttl";
        await makeEmptyFile(tempFileResult);

        // oversimplified mapping query. we just extract the contact punt with a non empty email and save them to a ttl file.
        let q = (limit, offset) => `
            ${PREFIXES}
            construct {?s a ?type; <http://schema.org/email> ?email;mu:uuid ?uuid} where {
              GRAPH <${task.inputContainer}> {
                ?s a ?type; mu:uuid ?uuid;<http://schema.org/email> ?email.
                FILTER( !isBlank(?email) && STRLEN(?email) > 0)
              }
            } limit ${limit} offset ${offset}`;
        let limit = 100,
            offset = 0;
        while (true) {
            let response = await query(q(limit, offset), {
                sparqlEndpoint: HIGH_LOAD_DATABASE_ENDPOINT,
                mayRetry: true,
            });

            let bindings = response.results.bindings?.map(({ s, p, o }) => {
                return {
                    subject: s,
                    predicate: p,
                    object: o,
                };
            });
            if (!bindings?.length) {
                break;
            }
            let triples = toTermObjectArray(bindings);
            await appendTempFile(
                triples
                    .map(({ subject, predicate, object }) => `${subject}${predicate}${object}.`)
                    .join("\n") + "\n",
                tempFileResult
            );
            offset += limit;
        }
        const fileResult = await writeFile(task.graph, tempFileResult, uuid() + ".ttl", task.id);
        await appendTaskResultFile(task, fileContainer, fileResult);

        await appendTaskResultGraph(task, graphContainer, fileContainer.uri); // resultContainer is a file container (can contain many files)
        await updateTaskStatus(task, STATUS_SUCCESS);
    } catch (e) {
        console.error(e);
        if (task) {
            await appendTaskError(task, e.message);
            await updateTaskStatus(task, STATUS_FAILED);
        }
    }
}
