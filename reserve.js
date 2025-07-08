const express = require('express');
const multer = require('multer');
const xml2js = require('xml2js');
const iconv = require('iconv-lite');
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config(); // Ensure your .env file has SUPABASE_URL and SUPABASE_KEY

const app = express();
const port = 3000;

// Middleware for parsing JSON request bodies
app.use(express.json());

// Multer: Use memory storage so nothing is saved to disk
const upload = multer({ storage: multer.memoryStorage() });

// Supabase client
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

// Helper: Extract address from nested XML structure (Enhanced)
function extractAddress(addressBlock) {
  if (!addressBlock) return null;

  const region = addressBlock['ADRESSORABOTI-OBLAST'] || '';
  const regionDistrict = addressBlock['ADRESSORABOTI-OBLAST-RAION'] ? `${addressBlock['ADRESSORABOTI-OBLAST-RAION']} район` : '';
  const villageType = addressBlock['ADRESSORABOTI-OBLAST-TIPDERVNY'] || '';
  const villageName = addressBlock['ADRESSORABOTI-GOROD-DERVNY'] || '';
  const city = addressBlock['ADRESSORABOTI-GOROD'] ? `город ${addressBlock['ADRESSORABOTI-GOROD']}` : '';
  const street = addressBlock['ADRESSORABOTI-ULICA'] || '';
  const dom = addressBlock['ADRESSORABOTI-DOM'] || '';

  return [region, regionDistrict, villageType, villageName, city, street, dom].filter(Boolean).join(', ');
}

// Helper: Extract and format work schedule from nested GAFIK_RABOTI
function extractSchedule(scheduleBlock) {
  if (!scheduleBlock || !scheduleBlock.GAFIK) return null;

  const gaflikArray = Array.isArray(scheduleBlock.GAFIK) ? scheduleBlock.GAFIK : [scheduleBlock.GAFIK];
  const schedules = [];
  const timeRegex = /^(0?\d|1\d|2[0-3]):([0-5]\d)-(0?\d|1\d|2[0-3]):([0-5]\d)$/;

  let currentDays = null;
  let currentTime = null;

  for (const item of gaflikArray) {
    const content = item._ || item; 
    if (!content) continue;

    if (content.includes('/')) { 
      currentDays = content;
    } else if (timeRegex.test(content)) { 
      if (!currentTime || (content.startsWith('0') && !currentTime.startsWith('0'))) {
         currentTime = content;
      }
    }

    if (currentDays && currentTime) {
        schedules.push(`График работы: ${currentDays}, время работы: ${currentTime}`);
        currentDays = null; 
        currentTime = null;
    }
  }

  if (schedules.length > 0) {
    return schedules.join(', есть ещё '); 
  }

  return null;
}

// Helper: Format phone numbers as per rules
function formatPhoneNumber(phoneNumber) {
  if (!phoneNumber) return null;

  // Convert phoneNumber to string to safely use string methods
  let cleaned = String(phoneNumber).replace(/[\s()-]/g, '');

  if (cleaned.startsWith('+7')) {
    const regionalCodes = [
      '30', '36', '38', '39', '40', '43', '44', '45', '47', '48', '49', '50', '51', '52', '53', '54',
      '55', '56', '57', '58', '59', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '70',
      '71', '72', '73', '74', '75', '76', '77', '78', '79', '90', '91', '92', '93', '94', '95', '96'
    ];
    if (cleaned.startsWith('+7831')) {
      const remainingDigits = cleaned.substring(5); 
      if (remainingDigits.length >= 2 && regionalCodes.includes(remainingDigits.substring(0, 2))) {
        return `8${cleaned.substring(1)}`; 
      } else {
        return remainingDigits; 
      }
    }
    return `8${cleaned.substring(1)}`; 
  } else if (cleaned.startsWith('8') && cleaned.length === 11) {
    return cleaned; 
  } else if (cleaned.length === 10) {
    return `8${cleaned}`; 
  }
  return cleaned; 
}

// Helper: Apply import rules (duplicate prevention, status logic)
async function applyImportRules(jobData) {
  if (jobData.depub_date) {
    const depubDate = new Date(jobData.depub_date);
    const today = new Date();
    today.setHours(0, 0, 0, 0); 
    if (depubDate <= today) {
      console.log(`Skipping vacancy ${jobData.job_title} due to depub_date (${jobData.depub_date}) being current or less.`);
      return { shouldInsert: false, data: null };
    }
  }

  const { data: existingJobs, error: fetchError } = await supabase
    .from('jobs')
    .select('id, account_date, depub_date')
    .eq('company_inn', jobData.company_inn)
    .eq('job_title', jobData.job_title);

  if (fetchError) {
    console.error('Error checking for existing job:', fetchError);
    return { shouldInsert: true, data: jobData }; 
  }

  if (existingJobs && existingJobs.length > 0) {
    const existingJob = existingJobs[0]; 
    const existingAccountDate = new Date(existingJob.account_date);
    const newAccountDate = new Date(jobData.account_date);

    if (newAccountDate <= existingAccountDate) {
      console.log(`Skipping older/same duplicate for ${jobData.job_title} (INN: ${jobData.company_inn})`);
      return { shouldInsert: false, data: null }; 
    } else {
      console.log(`Data for ${jobData.job_title} (INN: ${jobData.company_inn}) is newer. Will attempt update.`);
      return { shouldInsert: true, data: jobData }; 
    }
  }
  
  return { shouldInsert: true, data: jobData };
}

// POST endpoint to handle XML import (Create)
app.post('/upload', upload.single('xmlfile'), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'No XML file uploaded' });
  }

  let xmlContent;
  let parsed;
  let allRows;
  let jobRows;

  try {
    // --- Step 1: Decode XML Content ---
    try {
      xmlContent = iconv.decode(req.file.buffer, 'windows-1251');
      console.log('--- XML DECODING SUCCESS ---');
      console.log('Decoded XML content snippet (first 500 chars):', xmlContent.substring(0, 500) + '...');
    } catch (decodeErr) {
      console.error('--- XML DECODING ERROR ---');
      console.error('Error during XML decoding:', decodeErr);
      return res.status(500).json({ error: 'Failed to decode XML content' });
    }

    const parser = new xml2js.Parser({ explicitArray: false, ignoreAttrs: true });

    // --- Step 2: Parse XML Content ---
    try {
      parsed = await parser.parseStringPromise(xmlContent);
      console.log('--- XML PARSING SUCCESS ---');
      console.log('Parsed XML root keys:', Object.keys(parsed || {}));
      
      if (!parsed || !parsed.DECLARBODY) {
        console.error('Parsed XML does not contain expected <DECLARBODY> root element.');
        return res.status(400).json({ error: 'XML structure error: Missing or unexpected root tag' });
      }
    } catch (parseErr) {
      console.error('--- XML PARSING ERROR ---');
      console.error('Error during XML parsing:', parseErr);
      return res.status(500).json({ error: 'Failed to parse XML content' });
    }

    // --- Step 3: Extract and Filter Job Rows ---
    allRows = parsed.DECLARBODY.ROW || [];
    allRows = Array.isArray(allRows) ? allRows : (allRows ? [allRows] : []);
    console.log('Total <ROW> blocks found directly under <DECLARBODY>:', allRows.length);

    jobRows = allRows.filter(row => row.VAKNAZV || row.SCHETNOMER);
    console.log('Job listings identified after filtering (those with VAKNAZV or SCHETNOMER):', jobRows.length);
    if (jobRows.length > 0) {
      console.log('Sample of first identified job row:', JSON.stringify(jobRows[0]).substring(0, 500) + '...');
    }

    const vacanciesToInsert = [];

    // --- Step 4: Process and Format Each Job Row ---
    for (const row of jobRows) {
        try {
            const jobData = {
                job_title: row.VAKNAZV || null,
                publication_date: row.PUBLON ? new Date(row.PUBLON) : null,
                depub_date: row.PUBLOFF ? new Date(row.PUBLOFF) : null,
                account_number: row.SCHETNOMER || null,
                account_date: row.SCHETDATA ? new Date(row.SCHETDATA) : null,
                company_inn: row.INNKOMPAN || null,
                company_name: row.NAZVKOMPAN || null,
                phone: formatPhoneNumber(row.TELEF), // FIX APPLIED HERE
                email: row.ELPOCHTA || null,
                address: extractAddress(row.ADRESSORABOTI),
                conditions: row.DOPINFORMSUSLOVIY || null,
                responsibilities: row.DOPINFORMSOBYZANOSTI || null,
                requirements: row.DOPINFORMSTREBOVANIY || null,
                schedule: extractSchedule(row.GAFIK_RABOTI),
                salary: row.ZARPL || null,
                contact_person: row.KOGOSPROSITJ || null,
                rubr_atryb: row.RUBR_ATRYB || null,
                status: 'No Status',
            };

            const { shouldInsert, data: processedData } = await applyImportRules(jobData);
            
            if (shouldInsert && processedData) {
                vacanciesToInsert.push(processedData);
            }
        } catch (rowProcessErr) {
            console.error('--- JOB ROW PROCESSING ERROR ---');
            console.error('Error processing a single job row:', rowProcessErr);
            console.error('Problematic row data (snippet):', JSON.stringify(row).substring(0, 500) + '...');
        }
    }

    if (vacanciesToInsert.length === 0) {
      console.log('No new or updated valid job records to import after processing and filtering.');
      return res.json({ message: 'No new or updated valid job records to import.', count: 0 });
    }

    // --- Step 5: Upsert into Supabase ---
    console.log(`Attempting to upsert ${vacanciesToInsert.length} vacancies into Supabase.`);
    const { data, error } = await supabase
      .from('jobs')
      .upsert(vacanciesToInsert, { onConflict: ['company_inn', 'job_title'], ignoreDuplicates: false });

    if (error) {
      console.error('--- SUPABASE INSERT/UPSERT ERROR ---');
      console.error('Supabase upsert error:', error);
      return res.status(500).json({ error: 'Failed to insert/update records into Supabase' });
    }

    console.log(`Successfully upserted ${data.length} job listings.`);
    res.json({ message: `Successfully imported and/or updated ${data.length} job listings`, count: data.length });

  } catch (err) {
    console.error('--- UNHANDLED OVERALL IMPORT ERROR ---');
    console.error('An unhandled error occurred during the XML import process:', err);
    res.status(500).json({ error: 'Failed to process XML' });
  }
});

// --- Read (Retrieve) Vacancies ---
app.get('/vacancies', async (req, res) => {
  const { page = 1, limit = 500, status, date_from, date_to, search_term, sort_by, sort_order = 'desc' } = req.query;
  const offset = (parseInt(page) - 1) * parseInt(limit);
  let query = supabase.from('jobs').select('*', { count: 'exact' });

  if (status) {
    query = query.eq('status', status);
  }
  if (date_from) {
    query = query.gte('publication_date', date_from);
  }
  if (date_to) {
    query = query.lte('publication_date', date_to);
  }
  if (search_term) {
    query = query.or(`job_title.ilike.%${search_term}%,company_name.ilike.%${search_term}%,address.ilike.%${search_term}%`);
  }

  if (sort_by) {
    query = query.order(sort_by, { ascending: sort_order === 'asc' });
  } else {
    query = query.order('publication_date', { ascending: false });
  }

  query = query.range(offset, offset + parseInt(limit) - 1);

  const { data, error, count } = await query;

  if (error) {
    console.error('Supabase fetch error:', error);
    return res.status(500).json({ error: 'Failed to fetch vacancies' });
  }

  res.json({
    totalCount: count,
    page: parseInt(page),
    limit: parseInt(limit),
    vacancies: data,
  });
});

app.get('/vacancies/:id', async (req, res) => {
  const { id } = req.params;
  const { data, error } = await supabase.from('jobs').select('*').eq('id', id).single();

  if (error && error.code === 'PGRST116') {
    return res.status(404).json({ error: 'Vacancy not found' });
  }
  if (error) {
    console.error('Supabase fetch single error:', error);
    return res.status(500).json({ error: 'Failed to fetch vacancy' });
  }

  res.json(data);
});

// --- Update Vacancies ---
app.patch('/vacancies/:id', async (req, res) => {
  const { id } = req.params;
  const updateData = req.body;

  if (updateData.publication_date) updateData.publication_date = new Date(updateData.publication_date);
  if (updateData.depub_date) updateData.depub_date = new Date(updateData.depub_date);
  if (updateData.account_date) updateData.account_date = new Date(updateData.account_date);
  if (updateData.phone) updateData.phone = formatPhoneNumber(updateData.phone);
  if (updateData.ADRESSORABOTI) updateData.address = extractAddress(updateData.ADRESSORABOTI);
  if (updateData.GAFIK_RABOTI) updateData.schedule = extractSchedule(updateData.GAFIK_RABOTI);

  const { data, error } = await supabase
    .from('jobs')
    .update(updateData)
    .eq('id', id)
    .select();

  if (error) {
    console.error('Supabase update error:', error);
    return res.status(500).json({ error: 'Failed to update vacancy' });
  }
  if (!data || data.length === 0) {
    return res.status(404).json({ error: 'Vacancy not found for update' });
  }

  res.json({ message: 'Vacancy updated successfully', updatedVacancy: data[0] });
});

// --- Delete Vacancies ---
app.delete('/vacancies/:id', async (req, res) => {
  const { id } = req.params;
  const { error } = await supabase.from('jobs').delete().eq('id', id);

  if (error) {
    console.error('Supabase delete error:', error);
    return res.status(500).json({ error: 'Failed to delete vacancy' });
  }

  res.json({ message: 'Vacancy deleted successfully' });
});

app.delete('/vacancies/batch', async (req, res) => {
  const { ids } = req.body; 

  if (!Array.isArray(ids) || ids.length === 0) {
    return res.status(400).json({ error: 'No IDs provided for batch deletion' });
  }

  const { error } = await supabase.from('jobs').delete().in('id', ids);

  if (error) {
    console.error('Supabase batch delete error:', error);
    return res.status(500).json({ error: 'Failed to delete vacancies in batch' });
  }

  res.json({ message: `Successfully deleted ${ids.length} vacancies` });
});

app.delete('/vacancies/by-date-range', async (req, res) => {
  const { date_from, date_to } = req.query; 

  if (!date_from || !date_to) {
    return res.status(400).json({ error: 'Please provide both date_from and date_to parameters' });
  }

  const { error } = await supabase
    .from('jobs')
    .delete()
    .gte('publication_date', date_from) 
    .lte('publication_date', date_to);

  if (error) {
    console.error('Supabase delete by date range error:', error);
    return res.status(500).json({ error: 'Failed to delete vacancies by date range' });
  }

  res.json({ message: `Successfully deleted vacancies from ${date_from} to ${date_to}` });
});

app.listen(port, () => {
  console.log(`🚀 Server running on http://localhost:${port}`);
  console.log('Ensure Supabase table "jobs" exists and has necessary columns and a unique constraint for upsert.');
  console.log('For duplicate handling, consider a unique constraint on (company_inn, job_title) in Supabase.');
});