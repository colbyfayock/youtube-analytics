const parse = require('csv-parse/lib/sync');
const fetch = require('node-fetch');
const fs = require('fs').promises;

const RAW_PATH = './raw';
const OUTPUT_PATH = './data';
const THUMBS_PATH = './thumbs';
const THUMB_ID_DELIMITER = '||';

const columnMap = {
  Video: 'id',
  'Video title': 'title',
  'Video publish time': 'publishDate',
  Views: 'views',
  'Watch time (hours)': 'watchTimeHours',
  Subscribers: 'subscribers',
  'Your estimated revenue (USD)': 'estRevenueUsd',
  Impressions: 'impressions',
  'Impressions click-through rate (%)': 'impressionsCtrPercentage'
}

async function run() {
  const dateNow = new Date().toISOString();

  let files = await fs.readdir(RAW_PATH);

  console.log(`Found ${files.length} files, reading content...`);

  files = await Promise.all(files.map(async file => {
    const data = await fs.readFile(`${RAW_PATH}/${file}`, 'utf8');
    return {
      name: file,
      data
    }
  }));

  console.log('Normalizing records...');

  const sets = await Promise.all(files.map(async ({ name, data }) => {
    const records = await parse(data);
    const firstRow = records.shift();
    return {
      name,
      data: records.map(record => {
        const video = {};

        firstRow.forEach((column, i) => {
          const key = columnMap[column] || column;
          video[key] = record[i];
        });

        video.thumbId = `${video.id}${THUMB_ID_DELIMITER}${dateNow}`;
        video.thumbUrl = `https://img.youtube.com/vi/${video.id}/maxresdefault.jpg`;

        return video;
      })
    };
  }));

  console.log('Saving output...');

  await fs.writeFile(`${OUTPUT_PATH}/${dateNow}.json`, JSON.stringify(sets), 'utf8');

  console.log('Downloading current thumbnails...');

  await Promise.all(sets.map(({ data }) => data).flat().flatMap(async record => {
    const { thumbId, thumbUrl } = record;

    const response = await fetch(thumbUrl);

    const buffer = await response.buffer();

    await fs.writeFile(`${THUMBS_PATH}/${thumbId}.jpg`, buffer, 'utf8');
  }));

  console.log('Done.');
}

run();