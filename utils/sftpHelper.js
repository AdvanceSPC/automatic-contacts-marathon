// utils/sftpHelper.js
import Client from 'ssh2-sftp-client';
import csv from 'csv-parser';
import { Readable } from 'stream';


const sftpConfig = {
  host: process.env.SFTP_HOST,
  port: parseInt(process.env.SFTP_PORT || '22'),
  username: process.env.SFTP_USER,
  password: process.env.SFTP_PASSWORD,
};


// Función para convertir fecha YYYY-MM-DD a timestamp de medianoche UTC
function convertDateToHubSpotFormat(dateString) {
  if (!dateString || dateString.trim() === '') return undefined; // undefined = no enviar la propiedad
  
  try {
    // Parsear la fecha
    const [year, month, day] = dateString.split('-').map(Number);
    
    // Crear fecha a medianoche UTC (00:00:00) - lo que HubSpot requiere
    const utcDate = new Date(Date.UTC(year, month - 1, day, 0, 0, 0, 0));
    
    // Verificar si la fecha es válida
    if (isNaN(utcDate.getTime())) {
      console.warn(`⚠️  Fecha inválida: ${dateString}`);
      return undefined;
    }
    
    // Retornar timestamp en milisegundos (medianoche UTC)
    return utcDate.getTime();
  } catch (error) {
    console.warn(`⚠️  Error convirtiendo fecha ${dateString}:`, error);
    return undefined;
  }
}


export async function listSFTPFiles(remotePath = '/') {
  const sftp = new Client();
  try {
    console.log('🔌 Conectando a SFTP Marathon...');
    await sftp.connect(sftpConfig);
    console.log('✅ Conectado a SFTP Marathon');
    
    const fileList = await sftp.list(remotePath);
    
    const csvFiles = fileList
      .filter(file => file.type === '-') 
      .filter(file => file.name.startsWith('delta_contacto_') && file.name.endsWith('.csv'))
      .map(file => ({
        name: file.name,
        size: file.size,
        modifyTime: file.modifyTime,
        path: `${remotePath}${file.name}`.replace('//', '/')
      }));
    
    console.log(`📂 Archivos CSV encontrados en SFTP: ${csvFiles.length}`);
    
    await sftp.end();
    return csvFiles;
  } catch (error) {
    console.error('❌ Error listando archivos SFTP:', error);
    await sftp.end();
    throw error;
  }
}


export async function fetchCSVFromSFTP(fileName) {
  const sftp = new Client();
  try {
    console.log(`📥 Conectando a SFTP para descargar: ${fileName}`);
    await sftp.connect(sftpConfig);
    
    const remotePath = process.env.SFTP_REMOTE_PATH || '/';
    const fullPath = `${remotePath}${fileName}`.replace('//', '/');
    
    console.log(`📥 Descargando archivo desde: ${fullPath}`);
    
    // Descargar el archivo como buffer
    const buffer = await sftp.get(fullPath);
    
    await sftp.end();
    console.log(`✅ Archivo descargado exitosamente: ${fileName}`);
    
    // Parsear CSV desde el buffer
    return await parseCSVBuffer(buffer);
  } catch (error) {
    console.error(`❌ Error descargando archivo SFTP ${fileName}:`, error);
    await sftp.end();
    throw error;
  }
}


async function parseCSVBuffer(buffer) {
  const contacts = [];
  const stream = Readable.from(buffer);

  await new Promise((resolve, reject) => {
    stream
      .pipe(csv({ separator: ";" }))
      .on("data", (row) => {
        if (!row.contact_id) return;
        
        // Construir objeto de propiedades
        const properties = {
          contact_id: row.contact_id || null,
          email_principal: row.email || null,
          tipo_de_identificacion: row.tipo_de_identificacion || null,
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
          lifecyclestage: "other",
        };
        
        // Agregar fechas solo si existen (undefined = no se envía a HubSpot)
        const fechaPrimeraCompra = convertDateToHubSpotFormat(row.fecha_primera_compra);
        if (fechaPrimeraCompra !== undefined) {
          properties.fecha_primera_compra = fechaPrimeraCompra;
        }
        
        const fechaRegistroWeb = convertDateToHubSpotFormat(row.fecha_registro_web);
        if (fechaRegistroWeb !== undefined) {
          properties.fecha_registro_web = fechaRegistroWeb;
        }
        
        contacts.push({ properties });
      })
      .on("end", resolve)
      .on("error", reject);
  });

  console.log(`📊 Contactos extraídos del CSV: ${contacts.length}`);
  return contacts;
}


export async function testSFTPConnection() {
  const sftp = new Client();
  try {
    console.log('🔌 Probando conexión SFTP Marathon...');
    await sftp.connect(sftpConfig);
    console.log('✅ Conexión SFTP Marathon exitosa');
    await sftp.end();
    return true;
  } catch (error) {
    console.error('❌ Error en conexión SFTP Marathon:', error);
    await sftp.end();
    return false;
  }
}
