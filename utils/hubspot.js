// ./utils/hubspot.js
import fetch from "node-fetch";

export async function sendToHubspot(contactos) {
  const apiKey = process.env.HUBSPOT_API_KEY;
  const url = `https://api.hubapi.com/crm/v3/objects/contacts/batch/create`;
  const batchSize = 100;

  for (let i = 0; i < contactos.length; i += batchSize) {
    const batch = contactos.slice(i, i + batchSize);
    const res = await fetch(url, {
      method: "POST",
      headers: { 
        "Content-Type": "application/json",
        "Authorization": `Bearer ${apiKey}`
      },
      body: JSON.stringify({ inputs: batch }),
    });

    if (!res.ok) {
      console.error("❌ Error en batch:", await res.text());
    } else {
      console.log(`✅ Batch enviado: ${i} - ${i + batch.length - 1}`);
    }
  }
}