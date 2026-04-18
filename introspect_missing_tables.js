// Run this in Render Web Shell to discover missing table names and columns
// node introspect_missing_tables.js

const { Pool } = require('pg');
const pool = new Pool({
  host: process.env.DB_HOST,
  port: parseInt(process.env.DB_PORT || '5432'),
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  ssl: { rejectUnauthorized: false },
  family: 4,
});

async function q(sql, label) {
  try {
    const r = await pool.query(sql);
    console.log(`\n=== ${label} ===`);
    if (r.rows.length === 0) console.log('(no rows)');
    else r.rows.forEach(row => console.log(JSON.stringify(row)));
  } catch (e) {
    console.log(`\n=== ${label} === ERROR: ${e.message}`);
  }
}

async function main() {
  // 1. Find geologia tables
  await q(`
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema IN ('geologia', 'geologia_litologia', 'cprm', 'sgb')
      AND table_type = 'BASE TABLE'
    ORDER BY table_schema, table_name
  `, 'Geologia schemas - all tables');

  // 2. UC table columns
  await q(`
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'unidade_conservacao' AND table_name = 'unidade_conserv'
    ORDER BY ordinal_position
  `, 'UC table columns');

  // 3. UC sample row
  await q(`SELECT * FROM unidade_conservacao.unidade_conserv LIMIT 1`, 'UC sample row');

  // 4. Solo table columns
  await q(`
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'solo' AND table_name = 'pedo_area'
    ORDER BY ordinal_position
  `, 'Solo pedo_area columns');

  // 5. Solo sample row
  await q(`SELECT * FROM solo.pedo_area LIMIT 1`, 'Solo sample row');

  // 6. Aquiferos tables
  await q(`
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema IN ('hidrogeologia', 'sgb', 'hidrogeo', 'aquiferos', 'hidrogeol')
      AND table_type = 'BASE TABLE'
    ORDER BY table_schema, table_name
  `, 'Aquiferos schemas - all tables');

  // 7. Hidrografia schemas
  await q(`
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema IN ('hidrografia', 'hidro', 'ana', 'bho')
      AND table_type = 'BASE TABLE'
    ORDER BY table_schema, table_name
  `, 'Hidrografia schemas - all tables');

  // 8. Serventias table columns
  await q(`
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'serventias' AND table_name = 'serventias_brasil'
    ORDER BY ordinal_position
  `, 'Serventias columns');

  // 9. ANM table sample (confirm column names)
  await q(`SELECT * FROM anm.anm_mt LIMIT 0`, 'ANM mt columns');

  // 10. Carbono table check
  await q(`SELECT * FROM carbono_solo.carbono_2024 LIMIT 0`, 'Carbono table columns');

  pool.end();
}

main();
