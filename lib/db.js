import { sql } from '@vercel/postgres';

let _init;
export async function ensureSchema(){
  if (_init) return _init;
  _init = (async()=>{
    await sql`CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)`;
    await sql`CREATE TABLE IF NOT EXISTS chats (
      chat_id BIGINT PRIMARY KEY,
      type TEXT, title TEXT, username TEXT,
      status TEXT,
      added_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )`;
    await sql`CREATE TABLE IF NOT EXISTS roster (
      id BIGSERIAL PRIMARY KEY,
      d DATE NOT NULL,
      college TEXT,
      name TEXT
    )`;
    await sql`CREATE INDEX IF NOT EXISTS roster_d_idx ON roster(d)`;
    await sql`CREATE TABLE IF NOT EXISTS session_state (
      user_id BIGINT PRIMARY KEY,
      json JSONB,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )`;
  })();
  return _init;
}

/* settings (عامّة) */
export async function setSetting(key, value){
  await ensureSchema();
  await sql`INSERT INTO settings(key,value) VALUES(${key}, ${String(value)})
            ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value`;
}
export async function getSetting(key){
  await ensureSchema();
  const { rows } = await sql`SELECT value FROM settings WHERE key=${key}`;
  return rows[0]?.value ?? null;
}

/* session_state (حالة الويزارد لكل مستخدم) */
export async function setUserState(userId, obj){
  await ensureSchema();
  await sql`INSERT INTO session_state(user_id, json) VALUES(${userId}, ${obj}::jsonb)
            ON CONFLICT(user_id) DO UPDATE SET json=EXCLUDED.json, updated_at=NOW()`;
}
export async function getUserState(userId){
  await ensureSchema();
  const { rows } = await sql`SELECT json FROM session_state WHERE user_id=${userId}`;
  return rows[0]?.json || null;
}
export async function clearUserState(userId){
  await ensureSchema();
  await sql`DELETE FROM session_state WHERE user_id=${userId}`;
}

/* chats */
export async function upsertChat(chat, status='active'){
  await ensureSchema();
  const { id, type, title, username } = chat;
  await sql`INSERT INTO chats(chat_id,type,title,username,status)
            VALUES(${id},${type},${title||null},${username||null},${status})
            ON CONFLICT(chat_id) DO UPDATE SET
              type=EXCLUDED.type, title=EXCLUDED.title, username=EXCLUDED.username,
              status=EXCLUDED.status, updated_at=NOW()`;
}
export async function listGroupsPage(page=0, pageSize=8){
  await ensureSchema();
  const { rows: rowsCnt } = await sql`
    SELECT COUNT(*)::int AS c FROM chats
    WHERE status='active' AND (type='group' OR type='supergroup')`;
  const total = rowsCnt[0]?.c || 0;
  const { rows } = await sql`
    SELECT chat_id,title,type FROM chats
    WHERE status='active' AND (type='group' OR type='supergroup')
    ORDER BY title NULLS LAST
    OFFSET ${page*pageSize} LIMIT ${pageSize}`;
  return { rows, total };
}

/* roster */
export async function clearRoster(){ await ensureSchema(); await sql`DELETE FROM roster`; }
export async function insertRoster(dIso, college, name){
  await ensureSchema();
  await sql`INSERT INTO roster(d,college,name) VALUES(${dIso}, ${college}, ${name})`;
}
export async function rosterStats(){
  await ensureSchema();
  const { rows } = await sql`SELECT MIN(d)::text AS min_d, MAX(d)::text AS max_d, COUNT(*)::int AS cnt FROM roster`;
  return rows[0] || {};
}
export async function rosterRange(fromIso, toIso){
  await ensureSchema();
  const { rows } = await sql`
    SELECT d::text AS d, college, name FROM roster
    WHERE d BETWEEN ${fromIso} AND ${toIso}
    ORDER BY d, college, name`;
  return rows;
}
export async function minDate(){
  await ensureSchema();
  const { rows } = await sql`SELECT MIN(d)::text AS md FROM roster`;
  return rows[0]?.md || null;
}
export async function nextDateOnOrAfter(iso){
  await ensureSchema();
  const { rows } = await sql`SELECT MIN(d)::text AS md FROM roster WHERE d >= ${iso}`;
  return rows[0]?.md || null;
}
