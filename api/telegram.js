import { bot } from '../lib/bot.js';
import { ensureSchema } from '../lib/db.js';

// قراءة جسم الطلب
function readBody(req){
  return new Promise((resolve,reject)=>{
    let data=''; req.on('data',c=>data+=c);
    req.on('end',()=>resolve(data)); req.on('error',reject);
  });
}

export default async function handler(req, res){
  if (req.method !== 'POST'){ res.status(200).send('OK'); return; }
  try{
    await ensureSchema(); // تأكد من الجداول
    const raw = await readBody(req);
    const update = raw? JSON.parse(raw) : {};
    await bot.handleUpdate(update);
    res.status(200).send('OK');
  }catch(e){
    console.error('webhook error', e);
    // نعيد 200 لتجنّب إعادة الإرسال بكثرة من تيليجرام
    res.status(200).send('OK');
  }
}
