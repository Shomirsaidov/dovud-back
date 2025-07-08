const express = require('express');
const multer = require('multer');
const { importJobsFromXmlBuffer } = require('./utils/importJobsUtil.js');
const uploadAndPublishRoute = require('./routes/uploadAndPublish.js'); // ✅ Import new route
require('dotenv').config(); // ✅ Load environment variables

const app = express();
const port = 3000;
const upload = multer({ storage: multer.memoryStorage() });

app.use(express.json());

// ✅ Mount the new route
app.use('/upload-and-publish', uploadAndPublishRoute);

// ✅ Existing XML upload route
app.post('/upload', upload.single('xmlfile'), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'Файл XML не был загружен' });
  }

  const result = await importJobsFromXmlBuffer(req.file.buffer);

  if (!result.success) {
    return res.status(500).json({ error: result.error, details: result.details });
  }

  res.json({ message: result.message, count: result.count });
});

// ✅ Start the server
app.listen(port, () => {
  console.log(`🚀 Сервер запущен на http://localhost:${port}`);
});
