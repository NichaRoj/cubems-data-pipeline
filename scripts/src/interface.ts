interface PointId {
  id: number;
  type: string;
  name: string;
}

interface Quota {
  total: number;
  red: number;
  yellow: number;
  mode: string;
}

interface PointData {
  x: number; // Date in millisecond from epoch
  y: number; // Value
}

export interface CubemsData {
  id: number;
  name: string;
  level_name: string;
  pointid: PointId[];
  create_date: string;
  quota: {
    en: Quota;
    pw: Quota;
  };
  graph: PointData[];
}
