// utils/hubspot.js
import fetch from "node-fetch";

const HUBSPOT_BASE = "https://api.hubapi.com";
const BATCH_SIZE = 100;

export async function sendToHubspot(contactos) {
  const apiKey = process.env.HUBSPOT_API_KEY;

  const inputs = contactos
    .filter((c) => c.properties?.contact_id1)
    .map((c) => ({
      id: c.properties.contact_id1,
      properties: c.properties,
    }));

  for (let i = 0; i < inputs.length; i += BATCH_SIZE) {
    const batch = inputs.slice(i, i + BATCH_SIZE);

    try {
      const res = await fetch(
        `${HUBSPOT_BASE}/crm/v3/objects/contacts/batch/upsert`,
        {
          method: "POST",
          headers: {
            Authorization: `Bearer ${apiKey}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            idProperty: "contact_id1",
            inputs: batch,
          }),
        }
      );

      if (!res.ok) {
        const error = await res.text();
        console.error(`❌ Error en batch ${i}-${i + batch.length - 1}:`, error);
      } else {
        console.log(
          `✅ Batch ${i}-${i + batch.length - 1} procesado correctamente.`
        );
      }
    } catch (err) {
      console.error(
        `❌ Excepción al enviar batch ${i}-${i + batch.length - 1}:`,
        err
      );
    }
  }
}
