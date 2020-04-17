import * as functions from "firebase-functions";
import axios from "axios";
import { google } from "googleapis";
import { bucket } from "./firebase";
import endpoints from "./endpoints.json";

const baseUrl = (
  id: string | number,
  period: "day" | "month" | "year",
  type: "peak" | "energy"
) =>
  `https://www.bems.chula.ac.th/web/cham5-api/api/v1/building/${id}/building_usage/${period}/${type}`;

const headers = ["table", "timestamp", "value"];

const formatToCsv = (raw: any[]) => {
  let rows = raw.map((item) => headers.map((header) => item[header]).join(","));
  rows.unshift(headers.join(","));
  const csv = rows.join("\r\n");
  return csv;
};

const now = Date.now();

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const importData = async () => {
  let allData = [];
  for (const endpoint of endpoints) {
    try {
      const result = await axios.get(baseUrl(endpoint.id, "day", "peak"));
      const data = result.data as CubemsData;
      const recentData = data.graph.filter(
        (point) => now - point.x <= 15 * 60 * 1000 && now - point.x >= 0
      )[0];

      allData.push({
        table: endpoint.path,
        timestamp: recentData.x,
        value: recentData.y,
      });

      await delay(1000);
    } catch (error) {
      console.error(`Error at ID ${endpoint.id}: ${error}`);
    }
  }

  try {
    await bucket.file("raw_data.csv").save(formatToCsv(allData));
  } catch (error) {
    console.error(`Error during upload: ${error}`);
  }
};

export const import_every_fifteen = functions
  .runWith({ timeoutSeconds: 500 })
  .region("asia-east2")
  .pubsub.schedule("every 15 minutes")
  .timeZone("Asia/Bangkok")
  .onRun(async (context) => {
    await importData();

    try {
      const dataflow = google.dataflow("v1b3");
      const auth = await google.auth.getClient({
        scopes: ["https://www.googleapis.com/auth/cloud-platform"],
      });

      let request = {
        auth,
        projectId: process.env.GCLOUD_PROJECT,
        location: "asia-east1",
        gcsPath: "gs://cubems-data-pipeline.appspot.com/templates/csv_to_bq",
        requestBody: {
          jobName: `csv-to-bq`,
          environment: {
            tempLocation: "gs://staging.cubems-data-pipeline.appspot.com/temp",
          },
          parameters: {
            input: `gs://cubems-data-pipeline.appspot.com/raw_data.csv`,
          },
        },
      };

      return dataflow.projects.locations.templates.launch(request);
    } catch (error) {
      throw new Error(error);
    }
  });

// export const import_light = functions
//   .runWith({ timeoutSeconds: 500 })
//   .region("asia-east2")
//   .pubsub.schedule("every 15 minutes")
//   .timeZone("Asia/Bangkok")
//   .onRun(async (context) => await importData("light"));

// export const import_outlet = functions
//   .runWith({ timeoutSeconds: 500 })
//   .region("asia-east2")
//   .pubsub.schedule("every 15 minutes")
//   .timeZone("Asia/Bangkok")
//   .onRun(async (context) => await importData("outlet"));

// export const csv_to_bq = functions
//   .region("asia-east2")
//   .storage.object()
//   .onFinalize(async (object) => {
//     try {
//       const filePath = object.name?.replace(".csv", "");

//       // Exit function if file changes are in temporary or staging folder
//       if (
//         filePath?.includes("staging") ||
//         filePath?.includes("temp") ||
//         filePath?.includes("templates")
//       )
//         return;

//       const dataflow = google.dataflow("v1b3");
//       const auth = await google.auth.getClient({
//         scopes: ["https://www.googleapis.com/auth/cloud-platform"],
//       });

//       const biqQueryOutput = `cubems-data-pipeline:raw_${
//         filePath?.split("/")[0]
//       }.${filePath
//         ?.replace(`${filePath?.split("/")[0]}/`, "")
//         .replace(/\//g, "_")}`;

//       let request = {
//         auth,
//         projectId: process.env.GCLOUD_PROJECT,
//         location: "asia-east1",
//         gcsPath: "gs://cubems-data-pipeline.appspot.com/templates/csv_to_bq",
//         requestBody: {
//           jobName: `csv-to-bq-${filePath?.replace(/\//g, "-")}`,
//           environment: {
//             tempLocation: "gs://staging.cubems-data-pipeline.appspot.com/temp",
//           },
//           parameters: {
//             input: `gs://cubems-data-pipeline.appspot.com/${object.name}`,
//             output: biqQueryOutput,
//           },
//         },
//       };

//       return dataflow.projects.locations.templates.launch(request);
//     } catch (error) {
//       throw new Error(error);
//     }
//   });
