import fetch from "node-fetch";

const HUBSPOT_BASE = "https://api.hubapi.com";

export async function sendToHubspot(contactos) {
  const apiKey = process.env.HUBSPOT_API_KEY;
  const batchSize = 100;

  for (let i = 0; i < contactos.length; i += batchSize) {
    const batch = contactos.slice(i, i + batchSize);

    for (const contact of batch) {
      const props = contact.properties;
      const contactIdValue = props.contact_id;

      if (!contactIdValue) {
        console.warn("⚠️ Contacto sin contact_id. Se omite.");
        continue;
      }

      try {
        const existing = await buscarPorContactId(contactIdValue, apiKey);

        if (existing) {
          await actualizarContacto(existing.id, props, apiKey);
        } else {
          await crearContacto(props, apiKey);
        }
      } catch (err) {
        console.error("❌ Error procesando contacto:", err.message);
      }
    }

    console.log(`✅ Batch procesado: ${i} - ${i + batch.length - 1}`);
  }
}

async function buscarPorContactId(contactId, apiKey) {
  const res = await fetch(`${HUBSPOT_BASE}/crm/v3/objects/contacts/search`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      filterGroups: [
        {
          filters: [
            {
              propertyName: "contact_id",
              operator: "EQ",
              value: contactId,
            },
          ],
        },
      ],
      properties: ["contact_id"],
    }),
  });

  const data = await res.json();

  if (!res.ok) {
    console.error(`❌ Error buscando contact_id=${contactId}:`, data);
    return null;
  }

  return data.results && data.results.length > 0 ? data.results[0] : null;
}

async function actualizarContacto(id, properties, apiKey) {
  const res = await fetch(`${HUBSPOT_BASE}/crm/v3/objects/contacts/${id}`, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ properties }),
  });

  if (!res.ok) {
    const error = await res.text();
    console.error(`❌ Error actualizando contacto ${id}:`, error);
  } else {
    console.log(`🔄 Contacto actualizado: ${id}`);
  }
}

async function crearContacto(properties, apiKey) {
  const res = await fetch(`${HUBSPOT_BASE}/crm/v3/objects/contacts`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ properties }),
  });

  if (!res.ok) {
    const error = await res.text();
    console.error("❌ Error creando contacto:", error);
  } else {
    console.log("🆕 Contacto creado");
  }
}
