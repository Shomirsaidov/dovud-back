const express = require('express');
const router = express.Router();
const multer = require('multer');
const axios = require('axios');
const stringSimilarity = require('string-similarity');
const { createClient } = require('@supabase/supabase-js');
const { importJobsFromXmlBuffer } = require('../utils/importJobsUtil');

require('dotenv').config();

const upload = multer({ storage: multer.memoryStorage() });

const API_VERSION = '5.131';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// 🆕 Get credentials from Supabase
async function getVkCredentials() {
  const { data, error } = await supabase
    .from('access_tokens')
    .select('service_key, owner_id')
    .order('created_at', { ascending: false })
    .limit(1)
    .single();

  if (error || !data) {
    throw new Error(`❌ Не удалось получить VK токены: ${error?.message || 'Нет данных'}`);
  }

  return {
    accessToken: data.service_key,
    ownerId: data.owner_id
  };
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function groupJobsByVacancyName(jobs) {
  const groups = [];
  const threshold = 0.4;

  jobs.forEach(job => {
    let foundGroup = false;

    for (const group of groups) {
      const similarity = stringSimilarity.compareTwoStrings(
        job.job_title.toLowerCase(),
        group[0].job_title.toLowerCase()
      );

      if (similarity >= threshold) {
        group.push(job);
        foundGroup = true;
        break;
      }
    }

    if (!foundGroup) {
      groups.push([job]);
    }
  });

  return groups;
}

function groupJobsByAccountNumber(jobs) {
  const groupsMap = new Map();

  jobs.forEach(job => {
    const accNum = job.account_number || 'no_account';
    if (!groupsMap.has(accNum)) groupsMap.set(accNum, []);
    groupsMap.get(accNum).push(job);
  });

  return Array.from(groupsMap.values());
}

function createPostMessage(jobsGroup, options) {
  const {
    hideCompanyName,
    salaryThreshold,
    includeConditions,
    includeResponsibilities,
    includeRequirements,
    hideAddress,
    hideEmail
  } = options;

  let message = '';

  jobsGroup.forEach((job, idx) => {
    let salaryNum = 0;
    if (job.salary) {
      const digits = job.salary.replace(/\D/g, '');
      salaryNum = digits ? parseInt(digits, 10) : 0;
    }

    const salaryText = (salaryNum && salaryNum < salaryThreshold)
      ? 'По договоренности'
      : job.salary || '—';

    const companyText = hideCompanyName ? '' : `Компания: ${job.company_name || '—'}`;

    const contactsArr = [];
    if (job.phone) contactsArr.push(job.phone);
    if (!hideEmail && job.email) contactsArr.push(job.email);
    const contactsText = contactsArr.length ? `Контакты: ${contactsArr.join(', ')}` : 'Контакты: —';

    const addressText = hideAddress ? '' : (job.address ? `Адрес: ${job.address}` : '');

    message += `📌 Вакансия ${jobsGroup.length > 1 ? `${idx + 1}` : ''}: ${job.job_title || 'Вакансия'}\n`;

    if (companyText) message += `${companyText}\n`;
    message += `Зарплата: ${salaryText}\n`;

    if (job.schedule) message += `График: ${job.schedule}\n`;

    message += `${contactsText}\n`;

    if (includeResponsibilities && job.responsibilities) {
      message += `Обязанности: ${job.responsibilities}\n`;
    }

    if (includeConditions && job.conditions) {
      message += `Условия: ${job.conditions}\n`;
    }

    if (includeRequirements && job.requirements) {
      message += `Требования: ${job.requirements}\n`;
    }

    if (addressText) message += `${addressText}\n`;

    if (idx < jobsGroup.length - 1) {
      message += '\n---\n\n';
    }
  });

  message += '\n#работа #вакансия';

  return message;
}

async function postToVkWall(message, accessToken, ownerId) {
  const url = 'https://api.vk.com/method/wall.post';

  const data = new URLSearchParams();
  data.append('owner_id', ownerId);
  data.append('message', message);
  data.append('from_group', '1');
  data.append('access_token', accessToken);
  data.append('v', API_VERSION);

  try {
    const res = await axios.post(url, data.toString(), {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded'
      }
    });

    if (res.data.response) {
      const link = `https://vk.com/wall${ownerId}_${res.data.response.post_id}`;
      return {
        success: true,
        postId: res.data.response.post_id,
        link
      };
    } else {
      return { success: false, error: res.data.error?.error_msg || 'Unknown VK API error' };
    }
  } catch (error) {
    return { success: false, error: error.message };
  }
}


async function markJobAsPublished(jobId, vkLink) {
  const today = new Date().toISOString().split('T')[0]; // YYYY-MM-DD

  const { error } = await supabase
    .from('jobs')
    .update({
      publication_date: today,
      status: 'Активные',
      vk_link: vkLink
    })
    .eq('id', jobId);

  if (error) {
    console.error(`🔴 Ошибка при обновлении вакансии ID ${jobId}:`, error.message);
  }
}

router.post('/', upload.single('file'), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ success: false, error: 'XML файл не был загружен' });
  }

  try {
    const {
      groupByVacancyName,
      groupByAccountNumber,
      hideCompanyName,
      salaryThreshold,
      includeConditions,
      includeResponsibilities,
      includeRequirements,
      hideAddress,
      hideEmail
    } = req.body;

    const options = {
      groupByVacancyName: groupByVacancyName === 'true',
      groupByAccountNumber: groupByAccountNumber === 'true',
      hideCompanyName: hideCompanyName === 'true',
      salaryThreshold: Number(salaryThreshold) || 0,
      includeConditions: includeConditions === 'true',
      includeResponsibilities: includeResponsibilities === 'true',
      includeRequirements: includeRequirements === 'true',
      hideAddress: hideAddress === 'true',
      hideEmail: hideEmail === 'true'
    };

    // 🆕 get VK credentials from Supabase
    const credentials = await getVkCredentials();

    const importResult = await importJobsFromXmlBuffer(req.file.buffer);

    if (!importResult.success) {
      return res.status(500).json({ success: false, error: 'Импорт не удался', details: importResult.details });
    }

    let jobs = importResult.jobs || [];

    let groupedJobs = [];

    if (options.groupByVacancyName) {
      groupedJobs = groupJobsByVacancyName(jobs);
    } else if (options.groupByAccountNumber) {
      groupedJobs = groupJobsByAccountNumber(jobs);
    } else {
      groupedJobs = jobs.map(job => [job]);
    }

    const results = [];

    for (const group of groupedJobs) {
      const postMsg = createPostMessage(group, options);
      const vkResult = await postToVkWall(postMsg, credentials.accessToken, credentials.ownerId);

      if (vkResult.success) {
        for (const job of group) {
          if (job.id) {
            await markJobAsPublished(job.id, vkResult.link);
          }
        }
      }

      results.push({
        job_titles: group.map(j => j.job_title),
        ...vkResult
      });

      await sleep(1200); // VK flood control
    }

    return res.json({
      success: true,
      message: `Импортировано и опубликовано ${results.length} постов с вакансиями`,
      results
    });

  } catch (err) {
    console.error('❌ Ошибка при загрузке и публикации:', err);
    return res.status(500).json({ success: false, error: 'Внутренняя ошибка сервера' });
  }
});

module.exports = router;
