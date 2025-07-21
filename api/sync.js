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

  const processed = await readProcessedList();
  if (processed.includes(fileName)) {
    return res.status(200).send(`🟡 Ya fue procesado: ${fileName}`);
  }

  try {
    const contacts = await fetchCSVFromS3(fileName);

    if (contacts.length === 0) {
      return res.status(200).send("⚠️ El archivo está vacío.");
    }

    await sendToHubspot(contacts);
    processed.push(fileName);
    await saveProcessedList(processed);

    return res.status(200).send(`✅ Archivo ${fileName} procesado y enviado`);
  } catch (error) {
    console.error("❌ Error:", error.message);
    return res.status(500).send("Error procesando archivo.");
  }
}
