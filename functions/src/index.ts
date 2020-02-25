import * as functions from "firebase-functions";
import axios from "axios";

const baseUrl = (
  id: number,
  period: "day" | "month" | "year",
  type: "peak" | "energy"
) =>
  `https://www.bems.chula.ac.th/web/cham5-api/api/v1/building/${id}/building_usage/${period}/${type}`;

export const import_every_fifteen = functions
  .region("asia-east2")
  .pubsub.schedule("every 15 minutes")
  .timeZone("Asia/Bangkok")
  .onRun(async context => {
    const result = await axios.get(baseUrl(2, "day", "peak"));
    const data = result.data;
    console.log(data["name"]);
  });
