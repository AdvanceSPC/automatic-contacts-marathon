//Contactos
// utils/hubspot.js
import fetch from "node-fetch";
import { saveReportToS3, savePartialProgress } from "./s3Helpers.js";

const HUBSPOT_BASE = "https://api.hubapi.com";
const BATCH_SIZE = 100;
const MAX_CONCURRENT_REQUESTS = 3;

export async function sendToHubspot(contacts, fileName, maxExecutionTime = 240000) {
  const startTime = Date.now();
  
  const apiKey = process.env.HUBSPOT_API_KEY;
  let totalCreados = 0;
  let totalActualizados = 0;
  let totalFallidos = 0;

  if (contacts.length === 0) {
    console.log("⚠️ No hay contactos válidos para procesar.");
    return { totalCreados, totalActualizados, totalFallidos };
  }

  console.log(`🚀 Enviando ${contacts.length} contactos a HubSpot con ${Math.round(maxExecutionTime/1000)}s disponibles...`);

  const inputs = contacts
    .filter((c) => c.properties?.contact_id)
    .map((c) => ({
      id: c.properties.contact_id,         
      idProperty: "contact_id",           
      properties: c.properties,
    }));

  console.log(`📊 Contactos válidos para procesar: ${inputs.length} de ${contacts.length}`);

  for (let i = 0; i < inputs.length; i += BATCH_SIZE) {
    const elapsed = Date.now() - startTime;
    if (elapsed > maxExecutionTime * 0.9) { 
      console.log(`⏰ Timeout preventivo: procesados ${totalCreados + totalActualizados} de ${inputs.length} contactos (${Math.round(elapsed/1000)}s transcurridos)`);
      await savePartialProgress(fileName, totalCreados + totalActualizados, inputs.length);
      break;
    }

    const batch = inputs.slice(i, i + BATCH_SIZE);

    try {
      const res = await fetch(`${HUBSPOT_BASE}/crm/v3/objects/contacts/batch/upsert`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ inputs: batch }),
      });

      if (!res.ok) {
        const error = await res.text();
        console.error(`❌ Error al procesar batch ${i}-${i + batch.length - 1}:`, error);
        totalFallidos += batch.length;
        continue;
      }

      const result = await res.json();
      
      if (result.results) {
        result.results.forEach(contact => {
          const createdAt = new Date(contact.createdAt);
          const updatedAt = new Date(contact.updatedAt);
          const isRecent = (Date.now() - createdAt.getTime()) < 10000;
          
          if (isRecent && Math.abs(createdAt.getTime() - updatedAt.getTime()) < 1000) {
            totalCreados++;
          } else {
            totalActualizados++;
          }
        });
      } else {
        totalActualizados += batch.length;
      }

      console.log(`✅ Procesado batch ${i}-${i + batch.length - 1} (${batch.length} contactos)`);

      // Guardar progreso cada 5 batches
      if ((i / BATCH_SIZE) % 5 === 0) {
        await savePartialProgress(fileName, totalCreados + totalActualizados, inputs.length);
      }

    } catch (err) {
      console.error(`❌ Excepción al procesar batch ${i}-${i + batch.length - 1}:`, err);
      totalFallidos += batch.length;
    }

    await wait(200);
  }

  await generateFinalReport(contacts, { totalCreados, totalActualizados, totalFallidos }, fileName);
  
  return { totalCreados, totalActualizados, totalFallidos };
}

async function generateFinalReport(contacts, result, fileName) {
  const { totalCreados, totalActualizados, totalFallidos } = result;
  const totalOriginal = contacts.length;
  const totalProcesados = totalCreados + totalActualizados;

  console.log(`\n🎯 ================== RESUMEN FINAL CONTACTOS ==================`);
  console.log(`📄 Total contactos en archivo: ${totalOriginal}`);
  console.log(`🆕 Contactos creados: ${totalCreados}`);
  console.log(`🔄 Contactos actualizados: ${totalActualizados}`);
  console.log(`✅ Total procesados exitosamente: ${totalProcesados}`);
  console.log(`❌ Fallidos en envío: ${totalFallidos}`);
  console.log(`📊 Tasa de éxito: ${((totalProcesados / totalOriginal) * 100).toFixed(1)}%`);
  console.log(`===============================================================\n`);

  const now = new Date();
  const reportString = `📄 Procesado archivo de contactos: ${fileName || "Desconocido"}

📊 Total contactos en archivo: ${totalOriginal}
🆕 Contactos creados: ${totalCreados}
🔄 Contactos actualizados: ${totalActualizados}
✅ Total procesados exitosamente: ${totalProcesados}
❌ Fallidos en envío: ${totalFallidos}

📈 Tasa de éxito: ${((totalProcesados / totalOriginal) * 100).toFixed(1)}%

🕒 Fecha de ejecución: ${now.toLocaleDateString("es-EC")} ${now.toLocaleTimeString("es-EC")}
`.trim();

  const baseFileName = (fileName || `archivo_contactos_${now.getTime()}`).replace(".csv", "");
  await saveReportToS3(reportString, `reporte_contactos_${baseFileName}.txt`);
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
