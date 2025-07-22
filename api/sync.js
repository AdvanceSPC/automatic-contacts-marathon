// ./api/sync.js
import {
  fetchCSVFromS3,
  readProcessedList,
  saveProcessedList,
  testS3Connections,
} from "../utils/s3Helpers.js";
import { sendToHubspot } from "../utils/hubspot.js";
import { ListObjectsV2Command } from "@aws-sdk/client-s3";

const AWS1_BUCKET = process.env.AWS1_BUCKET;
const s3Read = new (await import("@aws-sdk/client-s3")).S3Client({
  region: process.env.AWS1_REGION,
  credentials: {
    accessKeyId: process.env.AWS1_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS1_SECRET_ACCESS_KEY,
  },
});

export const config = {
  runtime: "nodejs",
};

export default async function handler(req, res) {
  console.log("🔌 Verificando conexión con buckets S3...");

  const s3Ok = await testS3Connections();
  if (!s3Ok) {
    return res.status(500).send("❌ Fallo en conexión a uno o ambos buckets S3.");
  }

  console.log("📃 Cargando historial...");
  const processed = await readProcessedList();

  // 🔍 Listar todos los archivos en el bucket de lectura
  const command = new ListObjectsV2Command({
    Bucket: AWS1_BUCKET,
    Prefix: "delta_contacto_",
  });

  const { Contents = [] } = await s3Read.send(command);

  const nuevosArchivos = Contents.map((obj) => obj.Key)
    .filter((key) => key.endsWith(".csv"))
    .filter((key) => !processed.includes(key));

  if (nuevosArchivos.length === 0) {
    console.log("🟡 No hay nuevos archivos para procesar.");
    return res.status(200).send("🟡 No hay archivos nuevos.");
  }

  for (const fileName of nuevosArchivos) {
    try {
      console.log(`⬇️ Procesando archivo: ${fileName}`);
      const contacts = await fetchCSVFromS3(fileName);

      if (!contacts.length) {
        console.warn(`⚠️ Archivo vacío: ${fileName}`);
        continue;
      }

      console.log(`📨 Enviando ${contacts.length} contactos a HubSpot...`);
      await sendToHubspot(contacts);

      processed.push(fileName);
      console.log(`✅ Procesado exitosamente: ${fileName}`);
    } catch (error) {
      console.error(`❌ Error procesando ${fileName}:`, error);
    }
  }

  console.log("💾 Actualizando historial...");
  await saveProcessedList(processed);

  return res.status(200).send(`✅ Procesados ${nuevosArchivos.length} archivos nuevos.`);
}
