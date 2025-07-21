import dayjs from "dayjs";
import { fetchCSVFromS3 } from "../utils/s3Helpers.js";
import { readProcessedList, saveProcessedList } from "../utils/s3Helpers.js";
import { sendToHubspot } from "../utils/hubspot.js";

export const config = {
  runtime: "nodejs18.x",
};

export default async function handler(req, res) {
  const fecha = dayjs().subtract(1, "day").format("YYYYMMDD");
  const fileName = `delta_contacto_${fecha}.csv`;

  console.log(`📁 Buscando archivo: ${fileName}`);

  const processed = await readProcessedList();
  console.log("📃 Archivos procesados hasta ahora:", processed);

  if (processed.includes(fileName)) {
    console.log(`🟡 Ya se procesó anteriormente: ${fileName}`);
    return res.status(200).send(`🟡 Ya fue procesado: ${fileName}`);
  }

  try {
    console.log("⬇️ Descargando archivo desde S3...");
    const contacts = await fetchCSVFromS3(fileName);

    if (contacts.length === 0) {
      console.warn("⚠️ Archivo vacío.");
      return res.status(200).send("⚠️ El archivo está vacío.");
    }

    console.log(`👥 ${contacts.length} contactos encontrados. Enviando a HubSpot...`);
    await sendToHubspot(contacts);

    processed.push(fileName);
    console.log(`💾 Guardando ${fileName} como procesado...`);
    await saveProcessedList(processed);

    console.log("✅ Proceso finalizado exitosamente.");
    return res.status(200).send(`✅ Archivo ${fileName} procesado y enviado`);
  } catch (error) {
    console.error("❌ Error durante el proceso:", error);
    return res.status(500).send("Error procesando archivo.");
  }
}
