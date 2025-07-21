// ./utils/s3Helpers.js
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  ListObjectsV2Command,
} from "@aws-sdk/client-s3";
import csv from "csv-parser";
import { Readable } from "stream";

// Cliente S3 para la cuenta Marathon (lectura del CSV)
const s3Read = new S3Client({
  region: process.env.AWS1_REGION,
  credentials: {
    accessKeyId: process.env.AWS1_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS1_SECRET_ACCESS_KEY,
  },
});

// Cliente S3 para la cuenta Advance (guardar historial)
const s3Hist = new S3Client({
  region: process.env.AWS2_REGION,
  credentials: {
    accessKeyId: process.env.AWS2_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS2_SECRET_ACCESS_KEY,
  },
});

// Leer CSV del bucket de datos
export async function fetchCSVFromS3(fileName) {
  const command = new GetObjectCommand({
    Bucket: process.env.AWS1_BUCKET,
    Key: fileName,
  });

  const data = await s3Read.send(command);
  const stream = Readable.from(data.Body);
  const contacts = [];

  await new Promise((resolve, reject) => {
    stream
      .pipe(csv({ separator: ";" }))
      .on("data", (row) => {
        contacts.push({
          properties: {
            email_principal: row.email || null,
            tipo_de_identificacion: row.tipo_de_identificacion || null,
            contact_id: row.contact_id || null,
            estado_empleado: row.estado_empleado || null,
            firstname: row.firstname || null,
            lastname: row.lastname || null,
            genero_cliente: row.genero_cliente || null,
            rango_edad: row.rango_edad || null,
            fecha_nacimiento: row.fecha_nacimiento || null,
            mes_cumpleanios: row.mes_cumpleanios || null,
            correo_secundario: row.correo_secundario || null,
            phone: row.phone || null,
            contactable: row.contactable || null,
            fecha_registro_newsletter: row.fecha_registro_newsletter || null,
            campana_newsletter: row.campana_newsletter || null,
            convenio: row.convenio || null,
            fecha_inicio_convenio: row.fecha_inicio_convenio || null,
            fecha_fin_convenio: row.fecha_fin_convenio || null,
            // ✅ Asegurar que sean SOLO contactos (no marketing ni deals)
            lifecyclestage: "other", // Esto los marca como contactos normales
          },
        });
      })
      .on("end", resolve)
      .on("error", reject);
  });

  return contacts;
}

// Leer historial de archivos procesados
export async function readProcessedList() {
  try {
    const command = new GetObjectCommand({
      Bucket: process.env.AWS2_BUCKET,
      Key: process.env.PROCESSED_KEY,
    });

    const response = await s3Hist.send(command);
    const stream = await response.Body.transformToString();
    return JSON.parse(stream);
  } catch {
    return [];
  }
}

// Guardar historial actualizado
export async function saveProcessedList(list) {
  const command = new PutObjectCommand({
    Bucket: process.env.AWS2_BUCKET,
    Key: process.env.PROCESSED_KEY,
    Body: JSON.stringify(list, null, 2),
    ContentType: "application/json",
  });

  await s3Hist.send(command);
}

// Verificar conexión a los buckets S3
export async function testS3Connections() {
  try {
    // Probar conexión al bucket de lectura
    await s3Read.send(
      new ListObjectsV2Command({
        Bucket: process.env.AWS1_BUCKET,
        MaxKeys: 1,
      })
    );
    console.log("✅ Conexión exitosa a bucket de lectura (AWS1)");

    // Probar conexión al bucket de historial
    await s3Hist.send(
      new ListObjectsV2Command({
        Bucket: process.env.AWS2_BUCKET,
        MaxKeys: 1,
      })
    );
    console.log("✅ Conexión exitosa a bucket de historial (AWS2)");

    return true;
  } catch (err) {
    console.error("❌ Fallo en conexión a uno o ambos buckets S3:", err);
    return false;
  }
}