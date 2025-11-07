// see https://github.com/mu-semtech/mu-javascript-template for more info

import { app, errorHandler } from "mu";
import bodyParser from "body-parser";

import { run } from "./lib/pipeline";
import { Delta } from "./lib/delta";
import { STATUS_SCHEDULED } from "./constant";
import { failBusyTasks } from "./lib/task";

failBusyTasks();

app.get("/", function (_req, res) {
    res.send("Hello mu-javascript-template");
});

app.post("/delta", bodyParser.json({ limit: "50mb" }), async function (req, res, next) {
    try {
        const entries = new Delta(req.body).getInsertsFor(
            "http://www.w3.org/ns/adms#status",
            STATUS_SCHEDULED,
        );
        if (!entries.length) {
            console.log(
                "Delta did not contain potential tasks that are ready for scanning a docker image. awaiting the next batch!",
            );
            return res.status(204).send();
        }
        for (let entry of entries) {
            run(entry);
        }
        return res.status(200).send().end();
    } catch (e) {
        console.log(`Something unexpected went wrong while handling delta harvesting-tasks!`);
        console.error(e);
        return next(e);
    }
});

app.use(errorHandler);
