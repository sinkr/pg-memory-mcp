#!/usr/bin/env node
// One-shot migration: union many memory-graph JSONL files into Postgres.
// Two passes (entities before relations for the FK); ON CONFLICT dedupes, so
// loading every backup yields the union — the most complete recovery.
// Newest file first so its entity_type wins on conflict.
import fs from "fs";
import pg from "pg";

const files = process.argv.slice(2);
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 1 });

function* rows(file) {
  for (const ln of fs.readFileSync(file, "utf8").split("\n")) {
    const s = ln.trim();
    if (!s) continue;
    try { yield JSON.parse(s); } catch { /* skip junk */ }
  }
}

let ents = 0, obs = 0, rels = 0, relSkip = 0;

// pass 1: entities + observations
for (const f of files) {
  for (const o of rows(f)) {
    if (o.type !== "entity" || !o.name) continue;
    const r = await pool.query(
      `INSERT INTO entities(name, entity_type) VALUES ($1,$2)
         ON CONFLICT (name) DO NOTHING`,
      [o.name, o.entityType || "unknown"]
    );
    ents += r.rowCount;
    for (const c of o.observations || []) {
      const r2 = await pool.query(
        `INSERT INTO observations(entity_name, content) VALUES ($1,$2)
           ON CONFLICT (entity_name, content) DO NOTHING`,
        [o.name, c]
      );
      obs += r2.rowCount;
    }
  }
}

// pass 2: relations (endpoints now exist; skip dangling)
for (const f of files) {
  for (const o of rows(f)) {
    if (o.type !== "relation" || !o.from || !o.to) continue;
    try {
      const r = await pool.query(
        `INSERT INTO relations(from_name, to_name, relation_type) VALUES ($1,$2,$3)
           ON CONFLICT DO NOTHING`,
        [o.from, o.to, o.relationType]
      );
      rels += r.rowCount;
    } catch { relSkip++; } // FK violation = endpoint never defined; drop it
  }
}

const c = async (sql) => (await pool.query(sql)).rows[0].n;
console.log(JSON.stringify({
  inserted: { entities: ents, observations: obs, relations: rels, relations_skipped_dangling: relSkip },
  totals: {
    entities: await c("SELECT count(*) n FROM entities"),
    observations: await c("SELECT count(*) n FROM observations"),
    relations: await c("SELECT count(*) n FROM relations"),
  },
}, null, 2));
await pool.end();
