import { bot, postNextBlock } from '../lib/bot.js';
import { getSetting, setSetting } from '../lib/db.js';

/* حساب الوقت داخل منطقة زمنية بدون مكتبات خارجية */
function nowParts(tz){
  const d = new Date();
  const fmt = new Intl.DateTimeFormat('en-GB', { timeZone: tz, hour12:false,
    year:'numeric', month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit' });
  const [{value:day},{value:month},{value:year},{value:hour},{value:minute}] = fmt.formatToParts(d).filter(p=>['day','month','year','hour','minute'].includes(p.type));
  const wstr = new Intl.DateTimeFormat('en-US',{ timeZone: tz, weekday:'short' }).format(d); // Sun..Sat
  const map = {Sun:0,Mon:1,Tue:2,Wed:3,Thu:4,Fri:5,Sat:6};
  return { Y:+year, M:+month, D:+day, h:+hour, m:+minute, dow: map[wstr] };
}

function shouldFire(mode, daysCSV, timeHM, everyH, everyM, parts){
  const {Y,M,D,h,m,dow} = parts;
  if (mode==='every_minutes'){
    const N=Math.max(1, Math.min(parseInt(everyM||'1',10),60));
    if (m % N === 0) return { ok:true, key:`${Y}-${M}-${D} ${h}:${m}-everym-${N}` };
    return { ok:false };
  }
  if (mode==='every_hours'){
    const N=Math.max(1, Math.min(parseInt(everyH||'1',10),24));
    if (m===0 && (h % N)===0) return { ok:true, key:`${Y}-${M}-${D} ${h}:00-everyh-${N}` };
    return { ok:false };
  }
  const [HH,MM]=(timeHM||'09:00').split(':').map(x=>parseInt(x,10));
  if (mode==='daily'){
    if (h===HH && m===MM) return { ok:true, key:`${Y}-${M}-${D} ${HH}:${MM}-daily` };
    return { ok:false };
  }
  // weekly_days
  const set=new Set(String(daysCSV||'').split(',').filter(Boolean).map(x=>parseInt(x,10)));
  if ((set.size===0 || set.has(dow)) && h===HH && m===MM) return { ok:true, key:`${Y}-${M}-${D} ${HH}:${MM}-weekly` };
  return { ok:false };
}

export default async function handler(req, res){
  try{
    const enabled = (await getSetting('sched_enabled'))==='true';
    if (!enabled) return res.status(200).json({ ok:true, skip:'disabled' });

    const mode   = (await getSetting('sched_mode')) || 'weekly_days';
    const days   = await getSetting('sched_days_csv');
    const timeHM = await getSetting('sched_time') || '09:00';
    const everyH = await getSetting('sched_every_hours') || '1';
    const everyM = await getSetting('sched_every_minutes') || '1';
    const tz     = process.env.SCHED_TZ || 'Asia/Baghdad';

    const parts  = nowParts(tz);
    const fire   = shouldFire(mode, days, timeHM, everyH, everyM, parts);
    if (!fire.ok) return res.status(200).json({ ok:true, skip:'not time' });

    const lastKey = await getSetting('last_fire_key');
    if (lastKey === fire.key) return res.status(200).json({ ok:true, skip:'dupe minute' });

    const result = await postNextBlock({ telegram: bot.telegram });
    await setSetting('last_fire_key', fire.key);
    return res.status(200).json(result);
  }catch(e){
    console.error('cron error', e);
    return res.status(200).json({ ok:false, error: e.message });
  }
}
