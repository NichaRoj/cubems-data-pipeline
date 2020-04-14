import * as functions from "firebase-functions";
import axios from "axios";
import { cloneDeep } from "lodash";
import { bucket } from "./firebase";
import { google } from "googleapis";

const baseUrl = (
  id: number,
  period: "day" | "month" | "year",
  type: "peak" | "energy"
) =>
  `https://www.bems.chula.ac.th/web/cham5-api/api/v1/building/${id}/building_usage/${period}/${type}`;

const extractLocation = (data: CubemsData) => {
  const locations = data.pointid[0].name.substring(30).split("/");
  return (
    locations.slice(0, 4).join("/") +
    "/" +
    data.name.toLowerCase().replace(" ", "_")
  );
};

const headers = ["Timestamp", "Value"];

const extractData = (data: CubemsData) => {
  const raw = cloneDeep(data.graph);
  let points = raw.map((point) => `${point.x},${point.y}`);
  points.unshift(headers.join(","));
  const csv = points.join("\r\n");
  return csv;
};

export const import_every_fifteen = functions
  .region("asia-east2")
  .pubsub.schedule("every 15 minutes")
  .timeZone("Asia/Bangkok")
  .onRun(async (context) => {
    const result = await axios.get(baseUrl(101, "day", "peak"));
    const data = result.data as CubemsData;
    await bucket.file(extractLocation(data) + ".csv").save(extractData(data));
  });

export const csv_to_bq = functions
  .region("asia-east2")
  .storage.object()
  .onFinalize(async (object) => {
    try {
      const filePath = object.name?.replace(".csv", "");

      // Exit function if file changes are in temporary or staging folder
      if (
        filePath?.includes("staging") ||
        filePath?.includes("temp") ||
        filePath?.includes("templates")
      )
        return;

      const dataflow = google.dataflow("v1b3");
      const auth = await google.auth.getClient({
        scopes: ["https://www.googleapis.com/auth/cloud-platform"],
      });

      const biqQueryOutput = `cubems-data-pipeline:raw_${
        filePath?.split("/")[0]
      }.${filePath
        ?.replace(`${filePath?.split("/")[0]}/`, "")
        .replace(/\//g, "_")}`;

      let request = {
        auth,
        projectId: process.env.GCLOUD_PROJECT,
        location: "asia-east1",
        gcsPath: "gs://cubems-data-pipeline.appspot.com/templates/csv_to_bq",
        requestBody: {
          jobName: `csv-to-bq-${filePath?.replace(/\//g, "-")}`,
          environment: {
            tempLocation: "gs://staging.cubems-data-pipeline.appspot.com/temp",
          },
          parameters: {
            input: `gs://cubems-data-pipeline.appspot.com/${object.name}`,
            output: biqQueryOutput,
          },
        },
      };

      return dataflow.projects.locations.templates.launch(request);
    } catch (error) {
      throw new Error(error);
    }
  });
