import axios from "axios";
import { flattenDeep } from "lodash";
import { url, writeToCsv, delay } from "./util";
import endpoints from "./endpoints.json";
import { CubemsData } from "./interface";

export default async () => {
  try {
    let data = [];
    for (const endpoint of endpoints) {
      console.log(`ID: ${endpoint.id}...`);
      const result = (await axios.get(url(endpoint.id)))
        ?.data as CubemsData | null;
      const formatted = result.graph.map((each) => {
        if (+each.x <= Date.now()) {
          return {
            path: endpoint.path,
            timestamp: each.x,
            value: each.y,
          };
        }
      });
      data.push(formatted);
      await delay(1000);
    }
    writeToCsv(flattenDeep(data).filter((v) => v));
  } catch (error) {
    console.error(error);
  }
};
