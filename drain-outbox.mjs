// Manually drain the pg-memory-mcp offline outbox into Postgres.
// Safe by the server's own design note: every queued op is idempotent on the PG
// side (ON CONFLICT DO NOTHING), so replaying here cannot conflict with a stuck
// instance replaying later.
import pg from "pg";
import { DatabaseSync } from "node:sqlite";
import os from "node:os";
import path from "node:path";

const mirrorPath = path.join(os.homedir(), ".config", "pg-memory", "mirror.sqlite");
const mdb = new DatabaseSync(mirrorPath);
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
  max: 2,
  connectionTimeoutMillis: 8000,
  application_name: "pg-memory-manual-drain",
});
const q = (t, p) => pool.query(t, p);

async function upsertEntity(name, entityType) {
  await q(
    `INSERT INTO entities(name, entity_type) VALUES ($1,$2) ON CONFLICT (name) DO NOTHING`,
    [name, entityType || "unknown"],
  );
}
async function addObs(entityName, contents) {
  let added = 0;
  for (const c of contents || []) {
    const r = await q(
      `INSERT INTO observations(entity_name, content) VALUES ($1,$2)
         ON CONFLICT (entity_name, content) DO NOTHING RETURNING id`,
      [entityName, c],
    );
    added += r.rowCount;
  }
  return added;
}

const ops = {
  async create_entities(a) {
    let n = 0;
    for (const e of a.entities || []) {
      await upsertEntity(e.name, e.entityType);
      n += await addObs(e.name, e.observations);
    }
    return `entities=${(a.entities || []).length} obs+=${n}`;
  },
  async add_observations(a) {
    let n = 0;
    for (const o of a.observations || []) {
      const ent = await q(`SELECT 1 FROM entities WHERE name=$1`, [o.entityName]);
      if (!ent.rowCount) throw new Error(`entity not found in PG: ${o.entityName}`);
      n += await addObs(o.entityName, o.contents);
    }
    return `obs+=${n}`;
  },
  async create_relations(a) {
    let n = 0;
    for (const r of a.relations || []) {
      const res = await q(
        `INSERT INTO relations(from_name, to_name, relation_type) VALUES ($1,$2,$3)
           ON CONFLICT DO NOTHING RETURNING from_name`,
        [r.from, r.to, r.relationType],
      );
      n += res.rowCount;
    }
    return `relations+=${n}`;
  },
};

const probe = await q("SELECT current_user, current_database()");
console.log("connected as", probe.rows[0].current_user, "->", probe.rows[0].current_database);

const rows = mdb.prepare(`SELECT id, tool, args FROM outbox ORDER BY id`).all();
console.log(`outbox rows: ${rows.length}`);
let drained = 0;
for (const row of rows) {
  try {
    const summary = await ops[row.tool](JSON.parse(row.args));
    mdb.prepare(`DELETE FROM outbox WHERE id=?`).run(row.id);
    drained++;
    console.log(`  OK   id=${row.id} ${row.tool}  ${summary}`);
  } catch (err) {
    console.log(`  FAIL id=${row.id} ${row.tool}  ${err.message}`);
  }
}
const left = mdb.prepare(`SELECT COUNT(*) AS n FROM outbox`).get().n;
console.log(`drained ${drained}/${rows.length}; remaining in outbox: ${left}`);
await pool.end();
