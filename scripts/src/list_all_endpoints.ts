import axios from "axios";
import { range } from "lodash";
import csv from "csvtojson";
import { delay, writeToJson, url } from "./util";
import { CubemsData } from "./interface";

const formatPath = (data: CubemsData, idInfo: any[]) => {
  const url = String(data.pointid[0].name);
  const info = idInfo.filter((i) => i.PointID === url);
  if (info[0]?.S === "1") return null;
  let zone = 0;
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
  removed.splice(2, 0, `z${zone}`);
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
              path: formatPath(result, idInfos)?.toLowerCase(),
              pointid: result.pointid.map((each) => each.id),
            }
          : null
      );

      await delay(500);
    }

    await writeToJson(data.filter((val) => val && val.path));
    // await writeToStorage(data);
  } catch (error) {
    console.error(error);
  }
};
