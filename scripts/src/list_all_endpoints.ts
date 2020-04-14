import axios from "axios";
import { range } from "lodash";
import { promises as fs } from "fs";
import { Storage } from "@google-cloud/storage";
import csv from "csvtojson";

interface PointId {
  id: number;
  type: string;
  name: string;
}

interface CubemsData {
  id: number;
  name: string;
  level_name: string;
  pointid: PointId[];
}

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const url = (id) =>
  `https://www.bems.chula.ac.th/web/cham5-api/api/v1/building/${id}/building_usage/day/peak`;

const writeToJson = async (data) => {
  await fs.writeFile("endpoints.json", JSON.stringify(data, null, 2));
};

const writeToStorage = async (data) => {
  const storage = new Storage();
  const bucket = storage.bucket("cubems-data-pipeline.appspot.com");
  await bucket.file("endpoints.json").save(JSON.stringify(data, null, 2));
};

const formatPath = (data: CubemsData, idInfo: any[]) => {
  const url = String(data.pointid[0].name);
  const info = idInfo.filter((i) => i.PointID === url);
  let zone;
  if (info[0]?.Z1 === "1") zone = 1;
  if (info[0]?.Z2 === "1") zone = 2;
  if (info[0]?.Z3 === "1") zone = 3;
  if (info[0]?.Z4 === "1") zone = 4;
  if (info[0]?.Z5 === "1") zone = 5;
  const removed = url
    .replace("http://chamchuri5.chula.ac.th/", "")
    .replace("chamchuri5.chula.ac.th/", "")
    .split("/")
    .splice(0, 4);
  if (zone) removed.splice(2, 0, `z${zone}`);
  removed.push(data.name);
  return removed
    .join("/")
    .replace(/ - /g, "_")
    .replace(/ /g, "_")
    .replace(/-/g, "_");
};

export default async (num: number) => {
  axios.interceptors.response.use(
    (res) => res,
    function (error) {
      if (error.response.status == 404) return null;
      return Promise.reject(error);
    }
  );

  try {
    // Read CSV file
    const idInfos = await csv().fromFile(__dirname + "/pointid.csv");

    // Gather data on each point id
    let data = [];
    for (const i in range(num)) {
      const result = (await axios.get(url(i)))?.data as CubemsData | null;
      data.push(
        result && result.pointid.length > 0
          ? {
              id: i,
              path: formatPath(result, idInfos).toLowerCase(),
            }
          : null
      );

      await delay(500);
    }

    await writeToJson(data.filter((val) => val));
    // await writeToStorage(data);
  } catch (error) {
    console.error(error);
  }
};
