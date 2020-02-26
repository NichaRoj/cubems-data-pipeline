import * as functions from "firebase-functions";
import * as admin from "firebase-admin";

const serviceAccount = functions.config().service_account;

export default admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: "https://cubems-data-pipeline.firebaseio.com",
  storageBucket: "cubems-data-pipeline.appspot.com"
});

export const bucket = admin.storage().bucket();
