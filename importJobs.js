// importJobs.js
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
      const dopFields = extractDopinformsFields(row.DOPINFORMS);

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
        address: extractAddress(row.ADRESSORABOTI),
        conditions: dopFields.conditions,
        responsibilities: dopFields.responsibilities,
        requirements: dopFields.requirements,
        schedule: extractSchedule(row.GAFIK_RABOTI),
        salary: row.ZARPL || null,
        contact_person: row.KOGOSPROSITJ || null,
        extra_info: extractExtraInfo(row.DOPINFORMS),
      };
    });

    console.log('Всего <ROW> блоков:', rows.length);
    console.log('Найдено вакансий:', jobRows.length);
    console.log('Пример вакансии:', formatted[0]);

    if (formatted.length === 0) {
      return { success: true, message: 'Нет новых вакансий', count: 0 };
    }

    const { data, error } = await supabase.from('jobs').insert(formatted);

    if (error) {
      console.error('❌ Ошибка Supabase при вставке:', error);
      return { success: false, error: 'Ошибка Supabase', details: error };
    }

    console.log('✅ Вставка прошла успешно:', data);
    return { success: true, message: 'Вакансии импортированы', count: formatted.length };
  } catch (err) {
    console.error('❌ Ошибка обработки XML:', err);
    return { success: false, error: 'Ошибка обработки XML', details: err };
  }
}

// 🧩 Вспомогательные функции

function extractAddress(addressBlock) {
  if (!addressBlock) return null;
  const region = addressBlock['ADRESSORABOTI-OBLAST'] || '';
  const city = addressBlock['ADRESSORABOTI-GOROD'] || '';
  const street = addressBlock['ADRESSORABOTI-ULICA'] || '';
  const dom = addressBlock['ADRESSORABOTI-DOM'] || '';
  return [region, city, street, dom].filter(Boolean).join(', ');
}

function extractPhone(phoneObj) {
  if (!phoneObj) return null;
  const phones = phoneObj?.TELEF_NOMER;

  if (Array.isArray(phones)) {
    return phones.map(p => (typeof p === 'object' ? p._ : p)).join(', ');
  }

  if (typeof phones === 'object' && phones._) return phones._;
  if (typeof phones === 'string') return phones;

  return null;
}

function extractSchedule(scheduleObj) {
  if (!scheduleObj) return null;
  const schedule = scheduleObj?.GAFIK;

  if (Array.isArray(schedule)) {
    return schedule.map(item => item._ || item).join(', ');
  }

  if (typeof schedule === 'object' && schedule._) return schedule._;
  if (typeof schedule === 'string') return schedule;

  return null;
}

function extractExtraInfo(field) {
  if (!field) return null;
  if (typeof field === 'string') return field;
  if (typeof field === 'object' && field._) return field._;
  return JSON.stringify(field);
}

function extractDopinformsFields(field) {
  if (!field) {
    return {
      conditions: null,
      responsibilities: null,
      requirements: null,
    };
  }

  const text = typeof field === 'string'
    ? field
    : typeof field === 'object' && field._
    ? field._
    : JSON.stringify(field);

  const result = {
    conditions: null,
    responsibilities: null,
    requirements: null,
  };

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
