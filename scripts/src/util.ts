import { promises as fs } from "fs";
import { Storage } from "@google-cloud/storage";

export const delay = (ms: number) =>
  new Promise((resolve) => setTimeout(resolve, ms));

export const url = (id) =>
  `https://www.bems.chula.ac.th/web/cham5-api/api/v1/building/${id}/building_usage/day/peak`;

export const writeToJson = async (data) => {
  await fs.writeFile("endpoints.json", JSON.stringify(data, null, 2));
};

// Assume array
export const writeToCsv = async (data: any[]) => {
  const keys = ["path", "timestamp", "value"];
  const formatted = data.map((each) => keys.map((key) => each[key]).join(","));
  const headers = keys.join(",");
  formatted.unshift(headers);

  await fs.writeFile("raw_data.csv", formatted.join("\r\n"));
};

export const writeToStorage = async (data) => {
  const storage = new Storage();
  const bucket = storage.bucket("cubems-data-pipeline.appspot.com");
  await bucket.file("endpoints.json").save(JSON.stringify(data, null, 2));
};
