import axios from "axios";
import { range } from "lodash";
import { promises as fs } from "fs";
import { Storage } from "@google-cloud/storage";

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

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const url = id =>
  `https://www.bems.chula.ac.th/web/cham5-api/api/v1/building/${id}/building_usage/day/peak`;

const writeToJson = async data => {
  await fs.writeFile("endpoints.json", JSON.stringify(data, null, 2));
};

const writeToStorage = async data => {
  const storage = new Storage();
  const bucket = storage.bucket("cubems-data-pipeline.appspot.com");
  await bucket.file("endpoints.json").save(JSON.stringify(data, null, 2));
};

export default async (num: number) => {
  axios.interceptors.response.use(
    res => res,
    function(error) {
      if (error.response.status == 404) return null;
      return Promise.reject(error);
    }
  );

  try {
    let data = {};
    for (const i in range(num)) {
      const result = (await axios.get(url(i)))?.data as CubemsData | null;
      data = {
        ...data,
        [i]: result
          ? {
              id: result?.id,
              name: result?.name,
              level_name: result?.level_name,
              pointid: result?.pointid
            }
          : null
      };
      await delay(300);
    }

    await writeToJson(data);
    await writeToStorage(data);
  } catch (error) {
    console.error(error);
  }
};
