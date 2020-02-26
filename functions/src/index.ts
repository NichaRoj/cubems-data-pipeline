import * as functions from "firebase-functions";
import axios from "axios";
import { cloneDeep } from "lodash";
import { bucket } from "./firebase";

// id: 1* is overall energy/power usage, 2* is for chamchuri5 energy/power usage
// 3-9 are energy/power usage for each floor (floor 1-7)
// 10-42* are energy/power usage by
//
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
  let points = raw.map(point => `${point.x},${point.y}`);
  points.unshift(headers.join(","));
  const csv = points.join("\r\n");
  return csv;
};

export const import_every_fifteen = functions
  .region("asia-east2")
  .pubsub.schedule("every 15 minutes")
  .timeZone("Asia/Bangkok")
  .onRun(async context => {
    const result = await axios.get(baseUrl(101, "day", "peak"));
    const data = result.data as CubemsData;
    await bucket.file(extractLocation(data) + ".csv").save(extractData(data));
  });
