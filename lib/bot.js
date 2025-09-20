import { Telegraf, Markup } from 'telegraf';
import * as xlsx from 'xlsx';
import {
  setSetting, getSetting, upsertChat,
  clearRoster, insertRoster, rosterStats, rosterRange, minDate, nextDateOnOrAfter,
  setUserState, getUserState, clearUserState, listGroupsPage
} from './db.js';

/* ========= إعدادات عامة ========= */
const BOT_TOKEN = process.env.BOT_TOKEN;
const OWNER_ID = Number(process.env.OWNER_ID || 0);
const ALLOW_ANYONE_UPLOAD = String(process.env.ALLOW_ANYONE_UPLOAD || 'false').toLowerCase()==='true';
const SCHED_TZ = process.env.SCHED_TZ || 'Asia/Baghdad';
if (!BOT_TOKEN) throw new Error('BOT_TOKEN is required');
export const bot = new Telegraf(BOT_TOKEN);

/* ========= أدوات مساعدة ========= */
const AR_DAYS = ['الأحد','الإثنين','الثلاثاء','الأربعاء','الخميس','الجمعة','السبت'];
function pad2(n){ return n<10? '0'+n : ''+n; }
function fmtDMY(d){ return `${pad2(d.getDate())}/${pad2(d.getMonth()+1)}/${d.getFullYear()}`; }
function ymd(d){ return `${d.getFullYear()}-${pad2(d.getMonth()+1)}-${pad2(d.getDate())}`; }
function parseYMD(s){ const [Y,M,D]=s.split('-').map(Number); return new Date(Y, M-1, D); }
function arDayName(d){ return AR_DAYS[d.getDay()]; }
function addDays(d, n){ const x=new Date(d); x.setDate(x.getDate()+n); return x; }
async function notifyOwner(txt){ if (OWNER_ID) { try { await bot.telegram.sendMessage(OWNER_ID, txt); } catch {} } }

async function sendChunkedText(telegram, chatId, text, chunkSize=4000, extra={}){
  const s=String(text??'');
  if (s.length<=chunkSize) return telegram.sendMessage(chatId, s, { disable_web_page_preview:true, ...extra });
  let i=0; while(i<s.length){
    let len=Math.min(chunkSize, s.length-i);
    if (i+len<s.length){ const cut=s.lastIndexOf('\n', i+len); if (cut>i) len=(cut-i)+1; }
    await telegram.sendMessage(chatId, s.slice(i,i+len), { disable_web_page_preview:true, ...extra });
    i+=len;
  }
}

/* ========= رسالة "انشر الآن" ========= */
const DEFAULT_NOW_MESSAGE = `📄 جدول خفارات السلامة والدفاع المدني
(نص افتراضي — عدّله بالرد على رسالة ثم /setnow)`;
async function getNowMessage(){ return (await getSetting('now_message')) || DEFAULT_NOW_MESSAGE; }
async function setNowMessageTxt(txt){ await setSetting('now_message', txt||''); }

/* ========= اكتشاف أعمدة الإكسل ========= */
function norm(s=''){ return String(s).normalize('NFKD').replace(/[\u064B-\u065F]/g,'').replace(/[^\p{L}\p{N}\s_]/gu,'').replace(/\s+/g,' ').trim().toLowerCase(); }
function normalizeCollege(val=''){
  const s = String(val||'').trim();
  if (/صيدلة/i.test(s) || s.includes('الصيدل')) return 'كلية الصيدلة';
  if (/علوم\s*طبي/i.test(s) || s.includes('العلوم الطبية')) return 'كلية العلوم الطبية';
  return s || 'غير محدد';
}
function robustFindColumns(rows2D){
  const headers = rows2D[0] || [];
  const H = headers.map(h=>norm(h));
  const NAME_KEYS=['الاسم الرباعي','اسم الرباعي','الاسم','اسم','الرباعي','fullname','full name','name'];
  const DATE_KEYS=['تاريخ الخفارة','تاريخ','التاريخ','date'];
  const COLLEGE_KEYS=['مكان العمل','مكان_العمل','الكلية','الموقع','workplace','college','location','site'];

  let nameIdx=-1, dateIdx=-1, collegeIdx=-1;
  const hasAny=(h,arr)=>arr.some(k=>h.includes(norm(k)));
  H.forEach((h,i)=>{ if(dateIdx===-1&&hasAny(h,DATE_KEYS))dateIdx=i; if(collegeIdx===-1&&hasAny(h,COLLEGE_KEYS))collegeIdx=i; if(nameIdx===-1&&hasAny(h,NAME_KEYS))nameIdx=i; });

  const rows = rows2D.slice(1);
  const cols = Math.max(...rows.map(r=>Array.isArray(r)?r.length:0), headers.length);

  const isDateLike = (v)=>{
    if (v instanceof Date && !isNaN(v)) return true;
    const s=String(v||'').trim().replace(/-/g,'/').replace(/\./g,'/');
    if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(s)) return true;
    if (/^\d{4}\/\d{1,2}\/\d{1,2}$/.test(s)) return true;
    const n=Number(s); if (Number.isFinite(n) && n>20000 && n<60000) return true;
    return false;
  };

  if (dateIdx===-1){
    let best=-1,bestScore=0;
    for(let c=0;c<cols;c++){
      let score=0,seen=0;
      for(let r=0;r<Math.min(rows.length,60);r++){
        const v=rows[r]?.[c]; if(v==null||v==='')continue;
        seen++; if(isDateLike(v))score++;
      }
      if (seen>0 && score/seen>=0.6 && score>bestScore){ best=c; bestScore=score; }
    }
    if (best!==-1) dateIdx=best;
  }

  if (nameIdx===-1){
    let best=-1,bestScore=0;
    for(let c=0;c<cols;c++){
      let score=0,seen=0;
      for(let r=0;r<Math.min(rows.length,120);r++){
        const v=rows[r]?.[c]; if(!v)continue;
        const s=String(v).trim(); if(/^\d+(\.\d+)?$/.test(s))continue;
        const words=s.split(/\s+/).length; score+=Math.min(words,5); seen++;
      }
      if (seen>0 && score/seen>bestScore){ best=c; bestScore=score/seen; }
    }
    if (best!==-1) nameIdx=best;
  }

  if (collegeIdx===-1){
    let best=-1,bestRatio=Infinity;
    for(let c=0;c<cols;c++){
      const set=new Set(); let seen=0;
      for(let r=0;r<Math.min(rows.length,200);r++){
        const v=rows[r]?.[c]; if(v==null||String(v).trim()==='')continue;
        set.add(String(v).trim()); seen++;
      }
      if (seen===0) continue;
      const ratio=set.size/seen; if (ratio<bestRatio){ best=c; bestRatio=ratio; }
    }
    if (best!==-1) collegeIdx=best;
  }
  return { dateIdx, collegeIdx, nameIdx };
}
function robustParseDate(v){
  if (v instanceof Date && !isNaN(v)) return v;
  const s=String(v||'').trim().replace(/-/g,'/').replace(/\./g,'/');
  let m=s.match(/^(\d{4})\/(\d{1,2})\/(\d{1,2})$/);
  if (m) return new Date(+m[1], +m[2]-1, +m[3]);
  m=s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) return new Date(+m[3], +m[2]-1, +m[1]);
  const t=new Date(s); if (!isNaN(t)) return t;
  return null;
}
export async function importRosterFromBuffer(buf){
  const wb = xlsx.read(buf, { type:'buffer', cellDates:true, WTF:false });
  const ws = wb.Sheets[wb.SheetNames[0]];
  const rows2D = xlsx.utils.sheet_to_json(ws, { header:1, blankrows:false, defval:null, raw:false });
  if (!rows2D.length) throw new Error('الشيت فارغ.');
  const {dateIdx, collegeIdx, nameIdx} = robustFindColumns(rows2D);
  if (dateIdx<0 || nameIdx<0) throw new Error('تعذّر اكتشاف أعمدة (التاريخ/الاسم).');
  await clearRoster();
  for(let i=1;i<rows2D.length;i++){
    const row=rows2D[i]||[];
    const d = robustParseDate(row[dateIdx]); if(!d) continue;
    const name = String(row[nameIdx]||'').trim(); if(!name) continue;
    const college = normalizeCollege(row[collegeIdx]);
    await insertRoster(ymd(d), college, name);
  }
}

/* ========= بناء رسالة حسب مدة ========= */
export async function buildPeriodMessage(orderNo, orderDate, startDate, spanDays){
  const endDate = addDays(startDate, spanDays-1);
  const rows = await rosterRange(ymd(startDate), ymd(endDate));
  const byDate = new Map();
  for (const r of rows){
    const map = byDate.get(r.d) || new Map();
    const list = map.get(r.college||'غير محدد') || [];
    list.push(r.name); map.set(r.college||'غير محدد', list); byDate.set(r.d, map);
  }

  const header = (spanDays===1)
    ? `📄 جدول خفارات السلامة والدفاع المدني
ليوم ${arDayName(startDate)} ${fmtDMY(startDate)}

السلام عليكم ورحمة الله وبركاته،
السادة منتسبو جامعة جابر بن حيان للعلوم الطبية والصيدلانية المحترمون،
تحية طيبة وبعد...

استنادًا إلى الأمر الجامعي المرقم (${orderNo}) والصادر بتاريخ ${orderDate}، نرفق لكم فيما يلي جدول خفارات السلامة والدفاع المدني:`
    : `📄 جدول خفارات السلامة والدفاع المدني
للفترة من يوم ${arDayName(startDate)} ${fmtDMY(startDate)} ولغاية ${arDayName(endDate)} ${fmtDMY(endDate)}

السلام عليكم ورحمة الله وبركاته،
السادة منتسبو جامعة جابر بن حيان للعلوم الطبية والصيدلانية المحترمون،
تحية طيبة وبعد...

استنادًا إلى الأمر الجامعي المرقم (${orderNo}) والصادر بتاريخ ${orderDate}، نرفق لكم فيما يلي جدول خفارات السلامة والدفاع المدني لكلية العلوم الطبية والصيدلة:`;

  const lines=[];
  for (let i=0;i<spanDays;i++){
    const d=addDays(startDate,i); const key=ymd(d); const map=byDate.get(key); if(!map) continue;
    for (const [college,names] of map.entries()){
      lines.push(`\n🔹 ${arDayName(d)} ${fmtDMY(d)} – ${college}\n${names.join('\n')}\n`);
    }
  }

  const notes = `
🛑 ملاحظات وتعليمات مهمة:
1. الالتزام التام بأوقات الخفارة والتواجد في المواقع المحددة دون تأخير.
2. التوقيع في سجل الخفارات (استعلامات رقم 1) يُعد إجراءً رسميًا ملزمًا.
3. تُمنح إجازة تعويضية لمن يُكلف بالخفارة بعد تقديم طلب رسمي إلى الجهة الإدارية المختصة.

مع خالص التقدير والاحترام،
م. أحمد رحيم صاحب
مسؤول شعبة الدفاع المدني
جامعة جابر بن حيان للعلوم الطبية والصيدلانية`;
  return header + (lines.length? lines.join('') : '\n(لا توجد أسماء ضمن هذه الفترة)') + notes;
}

/* ========= تشغيل دفعة (يستعمله cron) ========= */
export async function postNextBlock({ telegram }){
  const enabled = (await getSetting('sched_enabled'))==='true';
  if (!enabled) return { ok:true, skipped:'disabled' };

  const targetId = Number(await getSetting('target_chat_id'));
  if (!targetId) return { ok:false, error:'no target_chat_id' };

  const orderNo = await getSetting('order_number');
  const orderDate = await getSetting('order_date');
  if (!orderNo || !orderDate) return { ok:false, error:'missing order info' };

  const spanDays = Math.max(1, Math.min(parseInt(await getSetting('post_span_days')||'7',10), 30));
  const stats = await rosterStats();
  if (!stats.min_d) return { ok:false, error:'empty roster' };

  let cursor = await getSetting('cursor_iso');
  if (!cursor) cursor = stats.min_d;
  if (cursor < stats.min_d) cursor = stats.min_d;
  if (cursor > stats.max_d) cursor = stats.min_d;

  // اقفز لأقرب تاريخ يحتوي بيانات
  const next = await nextDateOnOrAfter(cursor);
  if (!next){
    await setSetting('sched_enabled','false');
    await notifyOwner('✅ انتهت السجلات، أوقفتُ الجدولة.');
    return { ok:true, done:'exhausted' };
  }

  const start = parseYMD(next);
  const msg = await buildPeriodMessage(orderNo, orderDate, start, spanDays);
  const topicId = Number(await getSetting('target_topic_id') || 0);
  const extra = topicId ? { message_thread_id: topicId } : {};
  await sendChunkedText(telegram, targetId, msg, 4000, extra);

  await setSetting('cursor_iso', ymd(addDays(start, spanDays)));
  return { ok:true, posted:true };
}

/* ========= القائمة الرئيسية ========= */
function mainMenu(ctx){
  const rows=[];
  const inGroup = ctx.chat?.type==='group' || ctx.chat?.type==='supergroup';
  if (inGroup) rows.push([Markup.button.callback('📍 اجعل هذا الكروب هدفًا','t_set_current')]);
  rows.push([Markup.button.callback('🎯 اختيار هدف','t_choose'), Markup.button.callback('🎯 عرض الهدف','t_show')]);
  rows.push([Markup.button.callback('⏱️ إعداد/تعديل الجدولة','sched_setup')]);
  rows.push([Markup.button.callback('▶️ تشغيل الجدولة','sched_enable'), Markup.button.callback('⏹️ إيقاف الجدولة','sched_disable')]);
  rows.push([Markup.button.callback('🔔 انشر الآن (اختبار)','sched_run_now')]);
  if (inGroup) rows.push([Markup.button.callback('🔧 اضبط هذا الموضوع (Topics)','topic_set_here')]);
  rows.push([Markup.button.callback('📂 تحميل ملف جدول','file_help'), Markup.button.callback('🧾 معاينة شيت (نصي)','sheet_help')]);
  rows.push([Markup.button.callback('📚 قائمة المجموعات','groups_list')]);
  return { text:'✨ <b>القائمة الرئيسية</b>\nاختر ما تريد:', keyboard: Markup.inlineKeyboard(rows) };
}

/* ========= أوامر وبوت ========= */
bot.start(async (ctx)=>{ try{ await upsertChat(ctx.chat,'active'); }catch{}; const {text,keyboard}=mainMenu(ctx); await ctx.reply(text,{parse_mode:'HTML',...keyboard}); });
bot.command('menu', async (ctx)=>{ const {text,keyboard}=mainMenu(ctx); await ctx.reply(text,{parse_mode:'HTML',...keyboard}); });

/* الهدف */
bot.action('t_set_current', async (ctx)=>{ await ctx.answerCbQuery().catch(()=>{}); if(ctx.from?.id!==OWNER_ID) return ctx.reply('للمالك فقط.');
  const chat=ctx.chat; if(!(chat?.type==='group'||chat?.type==='supergroup')) return ctx.reply('استخدم هذا الزر داخل الكروب.');
  await upsertChat(chat,'active'); await setSetting('target_chat_id', chat.id);
  await ctx.reply('✅ تم تعيين هذا الكروب هدفًا للنشر.');
});
bot.action('t_show', async (ctx)=>{ await ctx.answerCbQuery().catch(()=>{}); if (ctx.from?.id!==OWNER_ID) return ctx.reply('للمالك فقط.');
  const id=Number(await getSetting('target_chat_id')); if(!id) return ctx.reply('لا يوجد هدف معيّن بعد.');
  const topicId = await getSetting('target_topic_id');
  await ctx.reply(`🎯 الهدف: ID=${id}\n🧵 Topic: ${topicId? topicId : '(غير محدد)'}`);
});

/* Topics */
bot.action('topic_set_here', async (ctx)=>{ await ctx.answerCbQuery().catch(()=>{}); if(ctx.from?.id!==OWNER_ID) return ctx.reply('للمالك فقط.');
  if (ctx.chat?.type!=='supergroup') return ctx.reply('يعمل داخل المجموعات فقط.');
  const tid = ctx.update?.callback_query?.message?.message_thread_id;
  if (!tid) return ctx.reply('لا يوجد Topic هنا (أنت في العام).');
  await setSetting('target_topic_id', String(tid));
  await ctx.reply(`✅ تم ضبط موضوع النشر على ID=${tid}`);
});
bot.command('settopic', async (ctx)=>{
  if (ctx.chat?.type!=='supergroup') return ctx.reply('هذا الأمر داخل المجموعات فقط.');
  if (ctx.from?.id!==OWNER_ID) return ctx.reply('للمالك فقط.');
  const tid = ctx.message?.message_thread_id;
  if (!tid) return ctx.reply('أنت في الموضوع العام.');
  await setSetting('target_topic_id', String(tid));
  await ctx.reply(`✅ تم ضبط موضوع النشر على ID=${tid}`);
});
bot.command('cleartopic', async (ctx)=>{ if(ctx.from?.id!==OWNER_ID) return ctx.reply('للمالك فقط.'); await setSetting('target_topic_id',''); await ctx.reply('✅ سيتم النشر في العام.'); });

/* تشغيل/إيقاف */
bot.action('sched_enable', async (ctx)=>{ await ctx.answerCbQuery().catch(()=>{}); if(ctx.from?.id!==OWNER_ID) return ctx.reply('للمالك فقط.');
  await setSetting('sched_enabled','true'); await ctx.reply('▶️ تم تشغيل الجدولة.');
});
bot.action('sched_disable', async (ctx)=>{ await ctx.answerCbQuery().catch(()=>{}); if(ctx.from?.id!==OWNER_ID) return ctx.reply('للمالك فقط.');
  await setSetting('sched_enabled','false'); await ctx.reply('⏹️ تم إيقاف الجدولة.');
});
bot.command('resume', async (ctx)=>{ if(ctx.from?.id!==OWNER_ID) return ctx.reply('للمالك فقط.'); await setSetting('sched_enabled','true'); await ctx.reply('▶️ تم تشغيل الجدولة.'); });
bot.command('stop', async (ctx)=>{ if(ctx.from?.id!==OWNER_ID) return ctx.reply('للمالك فقط.'); await setSetting('sched_enabled','false'); await ctx.reply('⏹️ تم إيقاف الجدولة.'); });
bot.command('status', async (ctx)=>{ if(ctx.from?.id!==OWNER_ID) return ctx.reply('للمالك فقط.');
  const enabled=(await getSetting('sched_enabled'))==='true'; await ctx.reply(`حالة الجدولة: ${enabled? 'تشغيل ✅':'متوقفة ⏸️'}`);
});

/* قائمة المجموعات + اختيار هدف مع صفحات من DB */
const PAGE=8;
function groupsKeyboard(page, total, rows){
  const pages=Math.max(1, Math.ceil(total/PAGE));
  const kb = rows.map(r=>[Markup.button.callback(`${r.title||'(بدون عنوان)'} — ${r.type}`, `pick:${r.chat_id}`)]);
  const nav=[]; if (page>0) nav.push(Markup.button.callback('◀️ السابق', `pg:${page-1}`));
  if (page<pages-1) nav.push(Markup.button.callback('التالي ▶️', `pg:${page+1}`));
  if (nav.length) kb.push(nav);
  kb.push([Markup.button.callback('◀️ القائمة','go_menu')]);
  return Markup.inlineKeyboard(kb);
}
bot.action('groups_list', async (ctx)=>{ await ctx.answerCbQuery().catch(()=>{}); if(ctx.from?.id!==OWNER_ID) return ctx.reply('للمالك فقط.');
  const { rows, total } = await listGroupsPage(0,PAGE); if(!total) return ctx.reply('لا توجد مجموعات محفوظة.');
  await ctx.reply('📚 المجموعات:', groupsKeyboard(0,total,rows));
});
bot.action('t_choose', async (ctx)=>{ await ctx.answerCbQuery().catch(()=>{}); if(ctx.chat?.type!=='private'||ctx.from?.id!==OWNER_ID) return ctx.reply('نفّذ من الخاص ومن حساب المالك.');
  const { rows, total } = await listGroupsPage(0,PAGE); if(!total) return ctx.reply('لا توجد مجموعات نشطة بعد.');
  await ctx.reply('اختر الهدف:', groupsKeyboard(0,total,rows));
});
bot.action(/^pg:(\d+)$/, async (ctx)=>{ await ctx.answerCbQuery().catch(()=>{}); const p=Number(ctx.match[1]||0);
  const { rows, total } = await listGroupsPage(p,PAGE); try{ await ctx.editMessageReplyMarkup(groupsKeyboard(p,total,rows).reply_markup); }
  catch{ await ctx.reply('اختر الهدف:', groupsKeyboard(p,total,rows)); }
});
bot.action(/^pick:(-?\d+)$/, async (ctx)=>{ await ctx.answerCbQuery().catch(()=>{}); if(ctx.from?.id!==OWNER_ID) return ctx.reply('للمالك فقط.');
  await setSetting('target_chat_id', String(Number(ctx.match[1]))); await ctx.reply('✅ تم اختيار الهدف.');
});
bot.action('go_menu', async (ctx)=>{ await ctx.answerCbQuery().catch(()=>{}); const {text,keyboard}=mainMenu(ctx); await ctx.reply(text,{parse_mode:'HTML',...keyboard}); });

/* تعليمات ملفات */
bot.action('file_help', async (ctx)=>{ await ctx.answerCbQuery().catch(()=>{}); await ctx.reply('📂 أرسل ملف XLSX/XLS/CSV/TSV/JSON (يفضّل بالخاص). سيتم استخدام أول شيت.'); });
bot.action('sheet_help', async (ctx)=>{ await ctx.answerCbQuery().catch(()=>{}); await ctx.reply('🧾 المعاينة النصية المتقدمة غير مفعّلة هنا، لكن المعالجة عند التحميل كاملة.'); });

/* ويزارد إعداد الجدولة — الحالة محفوظة في DB */
function modeKeyboard(){
  return Markup.inlineKeyboard([
    [Markup.button.callback('🗓️ حسب أيام الأسبوع','sm_weekly')],
    [Markup.button.callback('📅 مرة يوميًا','sm_daily')],
    [Markup.button.callback('⏳ كل N ساعة','sm_every')],
    [Markup.button.callback('⏱️ كل N دقيقة (اختبار)','sm_every_min')],
    [Markup.button.callback('◀️ القائمة','go_menu')],
  ]);
}
function daysKeyboard(daysSet){
  const dayButtons=[
    {t:'الأحد',v:0},{t:'الإثنين',v:1},{t:'الثلاثاء',v:2},{t:'الأربعاء',v:3},
    {t:'الخميس',v:4},{t:'الجمعة',v:5},{t:'السبت',v:6}
  ].map(d=>Markup.button.callback((daysSet.has(d.v)?'✅ ':'⬜ ')+d.t,`dsel:${d.v}`));
  return Markup.inlineKeyboard([
    dayButtons.slice(0,4),
    dayButtons.slice(4),
    [Markup.button.callback('تم الاختيار ▶️','days_done')],
  ]);
}
bot.action('sched_setup', async (ctx)=>{ await ctx.answerCbQuery().catch(()=>{}); if(ctx.from?.id!==OWNER_ID) return ctx.reply('للمالك فقط.');
  await setUserState(ctx.from.id, { mode:null, days:[], time:'09:00', every:1, minutes:1, span:7, step:'mode' });
  await ctx.reply('اختر نمط الجدولة:', modeKeyboard());
});
bot.action('sm_weekly', async (ctx)=>{ await ctx.answerCbQuery().catch(()=>{}); const st=await getUserState(ctx.from.id); if(!st) return;
  st.mode='weekly_days'; st.step='days'; await setUserState(ctx.from.id,st);
  const set=new Set(st.days); await ctx.reply('اختر أيام الأسبوع للنشر:', daysKeyboard(set));
});
bot.action('sm_daily', async (ctx)=>{ await ctx.answerCbQuery().catch(()=>{}); const st=await getUserState(ctx.from.id); if(!st) return;
  st.mode='daily'; st.step='time'; await setUserState(ctx.from.id,st);
  await ctx.reply('⏰ اكتب وقت النشر HH:MM (مثال 09:00):');
});
bot.action('sm_every', async (ctx)=>{ await ctx.answerCbQuery().catch(()=>{}); const st=await getUserState(ctx.from.id); if(!st) return;
  st.mode='every_hours'; st.step='every'; await setUserState(ctx.from.id,st);
  await ctx.reply('⏳ اكتب كل كم ساعة تريد النشر؟ (1–24) مثلاً: 1');
});
bot.action('sm_every_min', async (ctx)=>{ await ctx.answerCbQuery().catch(()=>{}); const st=await getUserState(ctx.from.id); if(!st) return;
  st.mode='every_minutes'; st.step='every_min'; await setUserState(ctx.from.id,st);
  await ctx.reply('⏱️ اكتب كل كم دقيقة تريد النشر؟ (1–60) مثلاً: 1');
});
bot.action(/^dsel:(\d)$/, async (ctx)=>{ await ctx.answerCbQuery().catch(()=>{}); const st=await getUserState(ctx.from.id); if(!st||st.step!=='days') return;
  const v=Number(ctx.match[1]); const set=new Set(st.days||[]); if(set.has(v)) set.delete(v); else set.add(v);
  st.days=[...set.values()]; await setUserState(ctx.from.id,st);
  await ctx.editMessageReplyMarkup(daysKeyboard(set).reply_markup).catch(async()=>{ await ctx.reply('اختر أيام الأسبوع:', daysKeyboard(set)); });
});
bot.action('days_done', async (ctx)=>{ await ctx.answerCbQuery().catch(()=>{}); const st=await getUserState(ctx.from.id); if(!st||st.step!=='days') return;
  if (!st.days?.length) return ctx.reply('اختر يومًا واحدًا على الأقل.');
  st.step='time'; await setUserState(ctx.from.id,st);
  await ctx.reply('⏰ اكتب وقت النشر HH:MM (مثال 09:00):');
});

/* إدخال نص للويزارد */
bot.on('text', async (ctx)=>{
  const st=await getUserState(ctx.from.id); if(!st) return; const txt=(ctx.message.text||'').trim();
  if (st.step==='time'){
    if (!/^\d{1,2}:\d{2}$/.test(txt)) return ctx.reply('صيغة غير صحيحة. اكتب مثل 09:00');
    st.time=txt; st.step='span'; await setUserState(ctx.from.id,st);
    return ctx.reply('🧩 كم يوم تريد تضمينه في كل رسالة؟ (1–14):');
  }
  if (st.step==='every'){
    const n=parseInt(txt,10); if(!Number.isFinite(n)||n<1||n>24) return ctx.reply('اكتب رقم بين 1 و 24.');
    st.every=n; st.step='span'; await setUserState(ctx.from.id,st);
    return ctx.reply('🧩 كم يوم تريد تضمينه في كل رسالة؟ (1–14):');
  }
  if (st.step==='every_min'){
    const n=parseInt(txt,10); if(!Number.isFinite(n)||n<1||n>60) return ctx.reply('اكتب رقم بين 1 و 60.');
    st.minutes=n; st.span=Math.min(st.span||7,7); st.step='span'; await setUserState(ctx.from.id,st);
    return ctx.reply('🧩 كم يوم تريد تضمينه في كل رسالة؟ (1–14):');
  }
  if (st.step==='span'){
    const n=parseInt(txt,10); if(!Number.isFinite(n)||n<1||n>14) return ctx.reply('اكتب رقم 1–14.');
    st.span=n; st.step='orderNo'; await setUserState(ctx.from.id,st);
    return ctx.reply('🧾 اكتب رقم الأمر الجامعي (مثال 2971):');
  }
  if (st.step==='orderNo'){ st.orderNo=txt; st.step='orderDate'; await setUserState(ctx.from.id,st); return ctx.reply('📅 اكتب تاريخ الأمر بصيغة YYYY/MM/DD (مثال 2025/04/30):'); }
  if (st.step==='orderDate'){
    if(!/^\d{4}\/\d{1,2}\/\d{1,2}$/.test(txt)) return ctx.reply('صيغة التاريخ غير صحيحة. مثل 2025/04/30');
    st.orderDate=txt; st.step='file'; await setUserState(ctx.from.id,st);
    return ctx.reply('📂 أرسل الآن ملف الإكسل. سأحلّله وأعرض معاينة.');
  }
});

/* استقبال الملف ضمن مسار الإعداد */
function detectExt(name='', mime=''){
  const n=name.toLowerCase();
  if (n.endsWith('.xlsx')) return 'xlsx';
  if (n.endsWith('.xls')) return 'xls';
  if (n.endsWith('.csv')) return 'csv';
  if (n.endsWith('.tsv')) return 'tsv';
  if (n.endsWith('.json')) return 'json';
  if ((mime||'').includes('spreadsheetml')) return 'xlsx';
  if ((mime||'').includes('csv')) return 'csv';
  if ((mime||'').includes('tsv')) return 'tsv';
  if ((mime||'').includes('json')) return 'json';
  return 'unknown';
}
bot.on('document', async (ctx)=>{
  const st=await getUserState(ctx.from?.id);
  if (st && st.step==='file'){
    if (!ALLOW_ANYONE_UPLOAD && ctx.from?.id!==OWNER_ID) return ctx.reply('للأمان: القراءة للمالك فقط.');
    const doc=ctx.message.document; const ext=detectExt(doc.file_name, doc.mime_type||'');
    if (!['xlsx','xls','csv','tsv','json'].includes(ext)) return ctx.reply('ادعم: XLSX/XLS/CSV/TSV/JSON');
    await ctx.reply('📥 جارٍ تحميل الملف...');
    try{
      const link = await ctx.telegram.getFileLink(doc.file_id);
      const res = await fetch(link.href);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const buf = Buffer.from(await res.arrayBuffer());

      if (ext==='json'){
        const arr = JSON.parse(buf.toString('utf8'));
        if (!Array.isArray(arr)) throw new Error('JSON ليس مصفوفة.');
        await clearRoster();
        for (const o of arr){
          const dStr=o.d||o.date||o['تاريخ_الخفارة']||o['التاريخ'];
          const nm=o.name||o['الاسم']||o['اسم الرباعي']||o['اسم_الرباعي'];
          const col=normalizeCollege(o.college||o['الكلية']||o['مكان العمل']||o['مكان_العمل']);
          if (!dStr || !nm) continue;
          const dt=new Date(dStr); if (isNaN(dt)) continue;
          await insertRoster(ymd(dt), col, String(nm).trim());
        }
      }else{
        await importRosterFromBuffer(buf);
      }

      /* حفظ إعدادات الجدولة */
      await setSetting('sched_mode', st.mode);
      await setSetting('sched_days_csv', (st.days||[]).sort((a,b)=>a-b).join(','));
      await setSetting('sched_time', st.time);
      await setSetting('sched_every_hours', String(st.every||1));
      await setSetting('sched_every_minutes', String(st.minutes||1));
      await setSetting('post_span_days', String(st.span));
      await setSetting('order_number', st.orderNo);
      await setSetting('order_date', st.orderDate);
      await setSetting('cursor_iso', null);

      const md = await minDate();
      if (!md){ await ctx.reply('لم أجد سجلات صالحة بعد التحميل.'); await clearUserState(ctx.from.id); return; }
      const preview = await buildPeriodMessage(st.orderNo, st.orderDate, parseYMD(md), st.span);
      await ctx.reply('🧾 هذه معاينة لأول رسالة سيتم نشرها:');
      await sendChunkedText(ctx.telegram, ctx.chat.id, preview, 3500);
      await ctx.reply('هل تريد بدء الجدولة؟', Markup.inlineKeyboard([
        [Markup.button.callback('✅ ابدأ الجدولة','sched_start')],
        [Markup.button.callback('❌ إلغاء','sched_cancel')]
      ]));
    }catch(e){
      console.error('schedule file load error:', e);
      await ctx.reply('❌ فشل التحميل/التحويل. تأكد من الأعمدة والتواريخ.');
      await clearUserState(ctx.from.id);
    }
    return;
  }

  // خارج المسار
  if ((ctx.chat?.type==='group'||ctx.chat?.type==='supergroup') && ctx.from?.id!==OWNER_ID){
    try{ await ctx.reply('⚠️ أرسل الملف في الخاص مع البوت.'); }catch{}
  }
});
bot.action('sched_start', async (ctx)=>{ await ctx.answerCbQuery().catch(()=>{}); if(ctx.from?.id!==OWNER_ID) return ctx.reply('للمالك فقط.');
  const target=await getSetting('target_chat_id'); if(!target) return ctx.reply('عيّن هدف النشر أولًا.');
  await setSetting('sched_enabled','true');
  const md = await minDate(); if (md) await setSetting('cursor_iso', md);
  await clearUserState(ctx.from.id);
  await ctx.reply('✅ تم تفعيل الجدولة. سيتم النشر تلقائيًا حسب الإعدادات.');
});
bot.action('sched_cancel', async (ctx)=>{ await ctx.answerCbQuery().catch(()=>{}); await clearUserState(ctx.from.id); await ctx.reply('أُلغي الإعداد.'); });

/* انشر الآن (رسالة ثابتة) */
bot.action('sched_run_now', async (ctx)=>{ await ctx.answerCbQuery().catch(()=>{}); if(ctx.from?.id!==OWNER_ID) return ctx.reply('للمالك فقط.');
  const targetId=Number(await getSetting('target_chat_id')); if(!targetId) return ctx.reply('عيّن الهدف أولًا.');
  const topicId=Number(await getSetting('target_topic_id')||0); const extra=topicId?{message_thread_id:topicId}:{};
  const message=await getNowMessage();
  try{ await sendChunkedText(bot.telegram, targetId, message, 3500, extra); await notifyOwner(`✅ أُرسلت الرسالة الثابتة إلى ${targetId}${topicId? ' (Topic '+topicId+')':''}.`); }
  catch(e){ const desc=e?.response?.description||e?.message||String(e); await notifyOwner(`❌ فشل الإرسال: ${desc}`); }
});

/* setnow/shownow */
bot.command('setnow', async (ctx)=>{ if(ctx.from?.id!==OWNER_ID) return ctx.reply('للمالك فقط.');
  const replyText=ctx.message?.reply_to_message?.text; if(!replyText) return ctx.reply('ارسل /setnow بالرد على رسالة تحتوي النص المطلوب.');
  await setNowMessageTxt(replyText); await ctx.reply('✅ تم حفظ الرسالة الثابتة.');
});
bot.command('shownow', async (ctx)=>{ if(ctx.from?.id!==OWNER_ID) return ctx.reply('للمالك فقط.'); const m=await getNowMessage(); await sendChunkedText(ctx.telegram, ctx.chat.id, m, 3500); });

/* settarget/registerhere/whoami/setorder/diag/resetcursor */
bot.command('registerhere', async (ctx)=>{ try{ await upsertChat(ctx.chat,'active'); }catch{}; await ctx.reply('✅ تم تسجيلي هنا. ارجع للخاص واضغط (🎯 اختيار هدف).'); });
bot.command('settarget', async (ctx)=>{ if(ctx.chat?.type!=='private'||ctx.from?.id!==OWNER_ID) return ctx.reply('نفّذ من الخاص ومن حساب المالك.');
  const parts=(ctx.message.text||'').trim().split(/\s+/); const id=Number(parts[1]); if(!id) return ctx.reply('الاستخدام: /settarget <chat_id>');
  await setSetting('target_chat_id', String(id)); await ctx.reply(`✅ تم تعيين الهدف إلى: ${id}`);
});
bot.command('setorder', async (ctx)=>{ if(ctx.from?.id!==OWNER_ID) return ctx.reply('للمالك فقط.');
  const parts=(ctx.message.text||'').split(/\s+/).slice(1); if(parts.length<2) return ctx.reply('الاستخدام: /setorder <رقم> <YYYY/MM/DD>');
  await setSetting('order_number', parts[0]); await setSetting('order_date', parts[1]); await ctx.reply('✅ تم تحديث الأمر الجامعي.');
});
bot.command('diag', async (ctx)=>{ if(ctx.from?.id!==OWNER_ID) return ctx.reply('للمالك فقط.');
  const targetId=await getSetting('target_chat_id'); const enabled=await getSetting('sched_enabled'); const mode=await getSetting('sched_mode');
  const days=await getSetting('sched_days_csv'); const timeHM=await getSetting('sched_time'); const every=await getSetting('sched_every_hours');
  const everyM=await getSetting('sched_every_minutes'); const span=await getSetting('post_span_days'); const orderNo=await getSetting('order_number'); const orderDt=await getSetting('order_date'); const cursor=await getSetting('cursor_iso');
  const stats=await rosterStats();
  await ctx.reply(
    '🔍 التشخيص:\n'+
    `• target: ${targetId||'-'}\n`+
    `• enabled: ${enabled}\n`+
    `• mode: ${mode||'-'}, days: ${days||'-'}, time: ${timeHM||'-'}, every_h: ${every||'-'}, every_m: ${everyM||'-'}\n`+
    `• span: ${span||'-'}\n`+
    `• order: ${orderNo||'-'} / ${orderDt||'-'}\n`+
    `• cursor: ${cursor||'-'}\n`+
    `• roster: count=${stats?.cnt||0}, min=${stats?.min_d||'-'}, max=${stats?.max_d||'-'}`
  );
});
bot.command('resetcursor', async (ctx)=>{ if(ctx.from?.id!==OWNER_ID) return ctx.reply('للمالك فقط.');
  const md=await minDate(); if(!md) return ctx.reply('لا توجد بيانات.'); await setSetting('cursor_iso', md); await ctx.reply(`✅ المؤشر = ${md}`);
});
bot.command('whoami', async (ctx)=>{ await ctx.reply(`👤 user_id: ${ctx.from?.id}\nchat_id: ${ctx.chat?.id}`); });

/* تسجيل دخول/خروج البوت من المجموعات */
bot.on('my_chat_member', async (ctx)=>{
  try{
    const me=ctx.update?.my_chat_member?.new_chat_member?.user;
    const status=ctx.update?.my_chat_member?.new_chat_member?.status;
    if (me?.id!==ctx.botInfo.id) return;
    if (status==='member'||status==='administrator'){
      await upsertChat(ctx.chat,'active');
      try{ const {text,keyboard}=mainMenu(ctx); await ctx.reply('✅ تم تسجيلي هنا.\n'+text,{parse_mode:'HTML',...keyboard}); }catch{}
    } else if (status==='left'||status==='kicked'){ await upsertChat(ctx.chat,'left'); }
    else { await upsertChat(ctx.chat,'active'); }
  }catch(e){ console.error('my_chat_member error:', e); }
});
