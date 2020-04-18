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

const headers = [
  "path",
  "building",
  "floor",
  "zone",
  "area",
  "sub_area",
  "sensor",
  "pointid",
  "datetime",
  "date",
  "time",
  "value",
  "last_created",
];

const formatToCsv = (raw: any[]) => {
  let rows = raw.map((item) => headers.map((header) => item[header]).join(","));
  rows.unshift(headers.join(","));
  const csv = rows.join("\r\n");
  return csv;
};

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const formatSensor = (sensor: string) => {
  if (sensor.includes("air")) return "aircon";
  if (sensor.includes("light")) return "light";
  if (sensor.includes("outlet")) return "outlet";
  if (sensor.includes("smartplug")) return "smart_plug";
  if (sensor.includes("smartlight")) return "smart_light";
  return sensor;
};

const importData = async () => {
  // Import and format data
  const now = Date.now();
  let allData = [];
  for (const endpoint of endpoints) {
    try {
      const result = await axios.get(baseUrl(endpoint.id, "day", "peak"));
      const data = result.data as CubemsData;
      const [
        building,
        floor,
        zone,
        area,
        sub_area,
        sensor,
      ] = endpoint.path.split("/");

      allData.push(
        ...data.graph.map((point) => ({
          path: endpoint.path,
          building,
          floor,
          zone,
          area,
          sub_area,
          sensor: formatSensor(sensor),
          pointid: endpoint.pointid.join(" | "),
          datetime: point.x,
          date: point.x,
          time: point.x,
          value: point.y,
          last_created: now,
        }))
      );

      await delay(1000);
    } catch (error) {
      console.error(`Error at ID ${endpoint.id}: ${error}`);
    }
  }

  // Save data to Cloud Storage
  try {
    await bucket.file("raw_data.csv").save(formatToCsv(allData));
  } catch (error) {
    console.error(`Error during upload: ${error}`);
  }
};

export const import_every_fifteen = functions
  .runWith({ timeoutSeconds: 540 })
  .region("asia-east2")
  .pubsub.schedule("every 15 minutes")
  .timeZone("Asia/Bangkok")
  .onRun(async (context) => {
    await importData();

    // Trigger dataflow job
    try {
      const dataflow = google.dataflow("v1b3");
      const auth = await google.auth.getClient({
        scopes: ["https://www.googleapis.com/auth/cloud-platform"],
      });

      let request = {
        auth,
        projectId: process.env.GCLOUD_PROJECT,
        location: "asia-east1",
        gcsPath:
          "gs://cubems-data-pipeline_asia-southeast1/templates/csv_to_bq",
        requestBody: {
          jobName: `csv-to-bq`,
          environment: {
            tempLocation: "gs://cubems-data-pipeline_asia-southeast1/temp",
          },
          parameters: {
            input: `gs://cubems-data-pipeline_asia-southeast1/raw_data.csv`,
          },
        },
      };

      return dataflow.projects.locations.templates.launch(request);
    } catch (error) {
      throw new Error(error);
    }
  });
