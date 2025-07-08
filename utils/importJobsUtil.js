// utils/importJobsUtil.js
const xml2js = require('xml2js');
const iconv = require('iconv-lite');
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

async function importJobsFromXmlBuffer(buffer) {
  try {
    const xmlContent = iconv.decode(buffer, 'windows-1251');
    const parser = new xml2js.Parser({ explicitArray: false });
    const parsed = await parser.parseStringPromise(xmlContent);

    const allRows = parsed?.DECLARBODY?.ROW || [];
    const rows = Array.isArray(allRows) ? allRows : [allRows];
    const jobRows = rows.filter(row => row.SCHETNOMER || row.VAKNAZV);

    const formatted = jobRows.map(row => {
      // Get responsibilities, requirements, and conditions directly from their tags
      const responsibilities = row.DOPINFORMS_OBYZANOSTI;
      const requirements = row.DOPINFORMS_TREBOVANIY;
      const conditions = row.DOPINFORMS_USLOVIY;

      // Collect all address blocks and combine them
      const addresses = [];
      if (row.ADRESSORABOTI) { // Original tag
          addresses.push(extractAddress(row.ADRESSORABOTI));
      }
      // Iterate for numbered ADRESSORABOTI tags (assuming up to 8 based on sample)
      for (let i = 1; i <= 8; i++) {
          if (row[`ADRESSORABOTI${i}`]) {
              addresses.push(extractAddress(row[`ADRESSORABOTI${i}`]));
          }
      }
      const combinedAddress = addresses.filter(Boolean).join('; ');

      return {
        job_title: row.VAKNAZV || null,
        publication_date: toISODate(row.PUBLON),
        depub_date: toISODate(row.PUBLOFF),
        account_number: row.SCHETNOMER || null,
        account_date: parseDate(row.SCHETDATA),
        company_inn: row.INNKOMPAN || null,
        company_name: row.NAZVKOMPAN || null,
        phone: extractPhone(row.TELEF),
        email: row.ELPOCHTA || null,
        address: combinedAddress,
        conditions: cleanHtmlToText(conditions),
        responsibilities: cleanHtmlToText(responsibilities),
        requirements: cleanHtmlToText(requirements),
        // Prioritize new schedule structure (GAFIK_RABOTI1), fallback to old (GAFIK_RABOTI)
        schedule: extractSchedule(row.GAFIK_RABOTI1 || row.GAFIK_RABOTI),
        salary: row.ZARPL || null,
        contact_person: row.KOGOSPROSITJ || null,
        // Changed 'extra_info' to 'description' as requested
        extra_info: row.VAKOPISANIYE || '',
      };
    });

    if (formatted.length === 0) {
      return { success: true, message: 'Нет новых вакансий', count: 0, data: [] };
    }

    const { data, error } = await supabase.from('jobs').insert(formatted).select('*');

    if (error) {
    console.error('❌ Ошибка Supabase при вставке:', error);
    return { success: false, error: 'Ошибка Supabase', details: error };
    }

    console.log('✅ Вставка прошла успешно:', data);

    // ✅ RETURN inserted rows WITH ID
    return { success: true, message: 'Вакансии импортированы', count: data.length, jobs: data };

  } catch (err) {
    return { success: false, error: 'Ошибка обработки XML', details: err };
  }
}

// Helper to get address components dynamically
function getAddressComponent(addressBlock, componentName) {
  if (!addressBlock) return null;
  for (const key in addressBlock) {
    if (key.endsWith(`-${componentName.toUpperCase()}`)) {
      return addressBlock[key];
    }
  }
  return null;
}

function extractAddress(addressBlock) {
  if (!addressBlock) return null;
  const region = getAddressComponent(addressBlock, 'OBLAST');
  const city = getAddressComponent(addressBlock, 'GOROD');
  const street = getAddressComponent(addressBlock, 'ULICA');
  const dom = getAddressComponent(addressBlock, 'DOM');
  return [region, city, street, dom].filter(Boolean).join(', ');
}

function extractPhone(phoneObj) {
  if (!phoneObj) return null;
  const phones = phoneObj?.TELEF_NOMER;
  if (Array.isArray(phones)) return phones.map(p => (typeof p === 'object' ? p._ : p)).join(', ');
  if (typeof phones === 'object' && phones._) return phones._;
  if (typeof phones === 'string') return phones;
  return null;
}

function extractSchedule(scheduleObj) {
  if (!scheduleObj) return null;

  const scheduleComponents = [];
  // Check for the new nested structure within GAFIK_RABOTI1 or GAFIK_RABOTI
  const gafik = scheduleObj.GAFIK;

  if (gafik) {
    if (gafik.SMENA1) {
        scheduleComponents.push(`смена: ${gafik.SMENA1}`);
    }
    if (gafik.DNI_RABOTI1) {
        scheduleComponents.push(`дни работы: ${gafik.DNI_RABOTI1}`);
    }
    if (gafik.VREMYARABOTY1) {
        scheduleComponents.push(`время работы: ${gafik.VREMYARABOTY1}`);
    }
  } else if (Array.isArray(scheduleObj.GAFIK)) { // Fallback for a potential array of schedules
      return scheduleObj.GAFIK.map(item => item._ || item).join(', ');
  } else if (typeof scheduleObj.GAFIK === 'object' && scheduleObj.GAFIK._) { // Fallback for a simple object with '_'
      return scheduleObj.GAFIK._;
  } else if (typeof scheduleObj === 'string') { // Fallback for a direct string
      return scheduleObj;
  }

  return scheduleComponents.filter(Boolean).join(', ') || null;
}

function extractExtraInfo(field) {
  if (!field) return null;
  if (typeof field === 'string') return field;
  if (typeof field === 'object' && field._) return field._;
  return JSON.stringify(field);
}

// This function is no longer used for conditions, responsibilities, requirements
// but might be kept for historical reasons or if DOPINFORMS is used elsewhere for generic parsing.
function extractDopinformsFields(field) {
  if (!field) return { conditions: null, responsibilities: null, requirements: null };

  const text = typeof field === 'string' ? field : typeof field === 'object' && field._ ? field._ : JSON.stringify(field);

  const result = { conditions: null, responsibilities: null, requirements: null };
  const pattern = /(?:Условия|Обязанности|Требования):/gi;
  const parts = text.split(pattern).map(s => s.trim());
  const markers = [...text.matchAll(pattern)].map(m => m[0].toLowerCase());

  markers.forEach((marker, idx) => {
    const value = parts[idx + 1]?.trim();
    if (!value) return;
    if (marker.includes('условия')) result.conditions = value;
    if (marker.includes('обязанности')) result.responsibilities = value;
    if (marker.includes('требования')) result.requirements = value;
  });

  return result;
}

function cleanHtmlToText(htmlString) {
  if (!htmlString) return null;

  const liMatches = [...htmlString.matchAll(/<li[^>]*>(.*?)<\/li>/gi)];
  if (liMatches.length > 0) {
    const listItems = liMatches.map(m => m[1].trim());
    return listItems.join(', ');
  }

  let text = htmlString
    .replace(/<br\s*\/?>/gi, ', ')
    .replace(/<\/?[^>]+(>|$)/g, '')
    .replace(/\s+/g, ' ')
    .trim();

  return text || null;
}


function parseDate(dateStr) {
  if (!dateStr) return null;
  const parts = dateStr.split('.');
  if (parts.length !== 3) return null;
  const [day, month, year] = parts;
  const fullYear = year.length === 2 ? '20' + year : year;
  return `${fullYear}-${month}-${day}`;
}

function toISODate(timestamp) {
  if (!timestamp) return null;
  const ts = parseInt(timestamp, 10);
  if (isNaN(ts)) return null;
  return new Date(ts * 1000).toISOString().split('T')[0];
}

module.exports = { importJobsFromXmlBuffer };