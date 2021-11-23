const express = require('express');
const router = express.Router();
const cors = require('cors');

const PdfController = require('../controllers/pdf/PdfController')
const userData = require('../users.json')


const corsOptions = {
  origin: ['https://aca.am', 'https://www.aca.am'],
  optionsSuccessStatus: 200
}


router.post('/generateCVs', cors(corsOptions), function(req, res, next) {
  PdfController.start();

  for (let i = 0; i < userData.length; i++) {
    PdfController.publish("", "jobs", new Buffer.from(JSON.stringify(userData[i])));
  }

  res.send('processing');
});

router.get('/ping', cors(corsOptions), function(req, res, next) {
  res.send('pong');
});

module.exports = router;
