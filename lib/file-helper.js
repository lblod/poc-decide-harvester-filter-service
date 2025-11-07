import {
    sparqlEscapeUri,
    uuid,
    sparqlEscapeString,
    sparqlEscapeDateTime,
    sparqlEscapeInt,
} from 'mu';
import { updateSudo } from '@lblod/mu-auth-sudo';
import fs from 'fs/promises';
import { join } from 'path';

import { HIGH_LOAD_DATABASE_ENDPOINT, PREFIXES, PUBLISHER_URI } from '../constant';
const connectionOptions = {
    sparqlEndpoint: HIGH_LOAD_DATABASE_ENDPOINT,
    mayRetry: true,
};
export async function makeEmptyFile(path) {
    const handle = await fs.open(path, 'w');
    await handle.close();
}
export async function appendTempFile(content, path) {
    try {
        await fs.appendFile(path, content, 'utf-8');
    } catch (e) {
        console.log(`Failed to append TTL to file <${path}>.`);
        throw e;
    }
}

/**
 * Write the given TTL content to a file and insert the logical and physical
 * counterparts in the triplestore.
 *
 * @public
 * @async
 * @function
 * @param {NamedNode} graph - Represents the graph in which to insert the data.
 * @param {String} content - This is the TTL formatted content you want to
 * write to a file.
 * @param {String} logicalFileName - The filename for the logical file. This
 * will only be used in the triplestore. The filename on disk will be a random
 * UUID (as per usual).
 * @returns {NamedNode} Representing the logical file written to the
 * triplestore.
 */
export async function writeTtlFile(
    graph,
    tempFile,
    logicalFileName,
    folderId = "",
    step = "delta"
) {
    const baseFolder = join('/share', folderId, step);
    await fs.mkdir(baseFolder, { recursive: true });

    const phyId = uuid();
    const phyFilename = `${phyId}.${extension}`;
    const path = `${baseFolder}/${phyFilename}`;
    const physicalFile = path.replace("/share/", "share://");
    const loId = uuid();
    const logicalFile = `http://data.lblod.info/id/files/${loId}`;
    const now = new Date();

    try {
        await fs.rename(tempFile, path, 'utf-8');
    } catch (e) {
        console.log(`Failed to write TTL to file <${physicalFile}>.`);
        throw e;
    }

    try {
        const stats = await fs.stat(path);

        const fileSize = stats.size;


        await updateSudo(
            `
      ${PREFIXES}
      INSERT DATA {
        GRAPH ${sparqlEscapeUri(graph)} {
          ${sparqlEscapeUri(physicalFile)}
            a nfo:FileDataObject ;
            nie:dataSource ${sparqlEscapeUri(logicalFile)} ;
            mu:uuid ${sparqlEscapeString(phyId)} ;
            nfo:fileName ${sparqlEscapeString(phyFilename)} ;
            dct:creator ${sparqlEscapeUri(PUBLISHER_URI)} ;
            dct:created ${sparqlEscapeDateTime(now)} ;
            dct:modified ${sparqlEscapeDateTime(now)} ;
            dct:format "text/turtle" ;
            nfo:fileSize ${sparqlEscapeInt(fileSize)} ;
            dbpedia:fileExtension "ttl" .
          ${sparqlEscapeUri(logicalFile)}
            a nfo:FileDataObject ;
            mu:uuid ${sparqlEscapeString(loId)} ;
            nfo:fileName ${sparqlEscapeString(logicalFileName)} ;
            dct:creator ${sparqlEscapeUri(PUBLISHER_URI)} ;
            dct:created ${sparqlEscapeDateTime(now)} ;
            dct:modified ${sparqlEscapeDateTime(now)} ;
            dct:format "text/turtle" ;
            nfo:fileSize ${sparqlEscapeInt(fileSize)} ;
            dbpedia:fileExtension "ttl" .
         ${sparqlEscapeUri(logicalFile)}
            a nfo:FileDataObject ;
            mu:uuid ${sparqlEscapeString(loId)} ;
            nfo:fileName ${sparqlEscapeString(logicalFileName)} ;
            dct:creator ${sparqlEscapeUri(PUBLISHER_URI)} ;
            dct:created ${termToString(now)} ;
            dct:modified ${termToString(now)} ;
            dct:format "text/turtle" ;
            nfo:fileSize ${termToString(fileSize)} ;
            dbpedia:fileExtension "ttl" .
        }
      }`,
            {},
            connectionOptions,
        );
    } catch (e) {
        console.log(
            `Failed to write TTL resource <${logicalFile}> to triplestore.`,
        );
        throw e;
    }
    return logicalFile;
}
