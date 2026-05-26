require('dotenv').config() // init // xx
   
const express = require('express');
const cors = require('cors');
const { Pool, Client } = require('pg'); // v2

const app = express();
const port = process.env.PORT || 3000; 

// Middleware
app.use(cors());
app.use(express.json({ limit: '50mb' }));

// Database connection pool — family:4 forces IPv4 (avoids ENETUNREACH on Render)
const pool = new Pool({
  host: process.env.DB_HOST,
  port: parseInt(process.env.DB_PORT || '5432'),
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  ssl: process.env.DB_SSL === 'false' ? false : { rejectUnauthorized: false },
  statement_timeout: 15000,
  query_timeout: 16000,
  family: 4,
});

const SRID = 4674; // SIRGAS 2000

// PERF: cache em memória LRU simples para /api/analises (TTL 1h, max 50 entries)
const _crypto = require('crypto');
const analiseCache = new Map();
const ANALISE_CACHE_TTL_MS = 60 * 60 * 1000;
const ANALISE_CACHE_MAX = 50;
function _cacheKey(geojson) {
  return _crypto.createHash('sha256').update(JSON.stringify(geojson || {})).digest('hex');
}
function _cacheGet(key) {
  const v = analiseCache.get(key);
  if (!v) return null;
  if (Date.now() - v.t > ANALISE_CACHE_TTL_MS) { analiseCache.delete(key); return null; }
  return v.data;
}
function _cacheSet(key, data) {
  analiseCache.set(key, { t: Date.now(), data });
  if (analiseCache.size > ANALISE_CACHE_MAX) {
    let oldestKey = null, oldestT = Infinity;
    for (const [k, v] of analiseCache) if (v.t < oldestT) { oldestT = v.t; oldestKey = k; }
    if (oldestKey) analiseCache.delete(oldestKey);
  }
}


// --- Startup: indices GIST para queries espaciais ---
async function ensureLogisticaTable() {
  try {
    await pool.query(`CREATE SCHEMA IF NOT EXISTS infra`);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS infra.terminais_logisticos (
        id SERIAL PRIMARY KEY,
        nome TEXT NOT NULL,
        tipo TEXT NOT NULL CHECK (tipo IN ('porto_maritimo', 'porto_fluvial', 'porto_seco', 'terminal_ferroviario')),
        municipio TEXT,
        uf TEXT,
        latitude DOUBLE PRECISION NOT NULL,
        longitude DOUBLE PRECISION NOT NULL,
        geom geometry(Point, 4674) GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(longitude, latitude), 4674)) STORED,
        UNIQUE (nome, tipo)
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_terminais_geom ON infra.terminais_logisticos USING GIST (geom)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_terminais_tipo ON infra.terminais_logisticos (tipo)`);
    // Seed idempotente (ON CONFLICT DO NOTHING)
    const seedSql = `
      INSERT INTO infra.terminais_logisticos (nome, tipo, municipio, uf, latitude, longitude)
      VALUES
-- Portos Marítimos
('Porto de Santos', 'porto_maritimo', 'Santos', 'SP', -23.9608, -46.3331),
('Porto de Paranaguá', 'porto_maritimo', 'Paranaguá', 'PR', -25.5163, -48.5224),
('Porto de Itaqui', 'porto_maritimo', 'São Luís', 'MA', -2.5757, -44.3635),
('Porto de Vitória', 'porto_maritimo', 'Vitória', 'ES', -20.3194, -40.3128),
('Porto de Tubarão', 'porto_maritimo', 'Vitória', 'ES', -20.2986, -40.2447),
('Porto de Rio Grande', 'porto_maritimo', 'Rio Grande', 'RS', -32.0356, -52.0986),
('Porto de Itapoá', 'porto_maritimo', 'Itapoá', 'SC', -26.1108, -48.6178),
('Porto de Itajaí', 'porto_maritimo', 'Itajaí', 'SC', -26.9078, -48.6614),
('Porto de São Francisco do Sul', 'porto_maritimo', 'São Francisco do Sul', 'SC', -26.2435, -48.6383),
('Porto de Suape', 'porto_maritimo', 'Ipojuca', 'PE', -8.3939, -34.9627),
('Porto do Pecém', 'porto_maritimo', 'São Gonçalo do Amarante', 'CE', -3.5475, -38.8089),
('Porto de Fortaleza/Mucuripe', 'porto_maritimo', 'Fortaleza', 'CE', -3.7062, -38.4763),
('Porto de Salvador', 'porto_maritimo', 'Salvador', 'BA', -12.9655, -38.5117),
('Porto de Aratu', 'porto_maritimo', 'Candeias', 'BA', -12.82, -38.53),
('Porto de Itaguaí (Sepetiba)', 'porto_maritimo', 'Itaguaí', 'RJ', -22.9389, -43.8350),
('Porto do Rio de Janeiro', 'porto_maritimo', 'Rio de Janeiro', 'RJ', -22.8995, -43.1872),
('Porto de Natal', 'porto_maritimo', 'Natal', 'RN', -5.7649, -35.2056),
('Porto de Cabedelo', 'porto_maritimo', 'Cabedelo', 'PB', -6.9683, -34.8294),
('Porto de Maceió', 'porto_maritimo', 'Maceió', 'AL', -9.6712, -35.7240),
('Porto de São Sebastião', 'porto_maritimo', 'São Sebastião', 'SP', -23.8094, -45.4031),
-- Portos Fluviais
('Porto de Manaus', 'porto_fluvial', 'Manaus', 'AM', -3.1322, -60.0153),
('Porto de Santarém', 'porto_fluvial', 'Santarém', 'PA', -2.4378, -54.7081),
('Terminal Hidroviário de Itacoatiara', 'porto_fluvial', 'Itacoatiara', 'AM', -3.1419, -58.4444),
('Porto de Vila do Conde', 'porto_fluvial', 'Barcarena', 'PA', -1.5444, -48.6500),
('Terminal Miritituba (TUP Cargill)', 'porto_fluvial', 'Itaituba', 'PA', -4.2789, -55.9794),
('Porto de Cáceres', 'porto_fluvial', 'Cáceres', 'MT', -16.0656, -57.6839),
('Porto de Ladário', 'porto_fluvial', 'Ladário', 'MS', -19.0078, -57.6056),
('Porto Velho', 'porto_fluvial', 'Porto Velho', 'RO', -8.7619, -63.9039),
('Porto de Estrela', 'porto_fluvial', 'Estrela', 'RS', -29.5006, -51.9647),
('Porto de Pirapora', 'porto_fluvial', 'Pirapora', 'MG', -17.3447, -44.9436),
('Porto de Belém', 'porto_fluvial', 'Belém', 'PA', -1.4558, -48.5044),
('Porto de Macapá', 'porto_fluvial', 'Macapá', 'AP', -0.0356, -51.0500),
('Porto de Trombetas (Porto Trombetas)', 'porto_fluvial', 'Oriximiná', 'PA', -1.4628, -56.3897),
('Porto de Marabá', 'porto_fluvial', 'Marabá', 'PA', -5.3683, -49.1175),
-- Portos Secos / EADIs / Recintos Alfandegados
('EADI Salvador', 'porto_seco', 'Salvador', 'BA', -12.97, -38.5),
('EADI São Bernardo do Campo', 'porto_seco', 'São Bernardo do Campo', 'SP', -23.6914, -46.5650),
('EADI Anápolis', 'porto_seco', 'Anápolis', 'GO', -16.3267, -48.9531),
('EADI Vitória da Conquista', 'porto_seco', 'Vitória da Conquista', 'BA', -14.8619, -40.8444),
('EADI Uberlândia', 'porto_seco', 'Uberlândia', 'MG', -18.9189, -48.2767),
('EADI Caxias do Sul', 'porto_seco', 'Caxias do Sul', 'RS', -29.1678, -51.1789),
('EADI Cuiabá', 'porto_seco', 'Cuiabá', 'MT', -15.5989, -56.0950),
('EADI Foz do Iguaçu', 'porto_seco', 'Foz do Iguaçu', 'PR', -25.5168, -54.5853),
('EADI Cascavel', 'porto_seco', 'Cascavel', 'PR', -24.9550, -53.4555),
('EADI Várzea Grande', 'porto_seco', 'Várzea Grande', 'MT', -15.6467, -56.1325),
('EADI Barueri', 'porto_seco', 'Barueri', 'SP', -23.5111, -46.8761),
('EADI Itaboraí', 'porto_seco', 'Itaboraí', 'RJ', -22.7456, -42.8597),
('EADI Suzano', 'porto_seco', 'Suzano', 'SP', -23.5425, -46.3106),
('EADI Maringá', 'porto_seco', 'Maringá', 'PR', -23.4205, -51.9331),
('EADI Sorocaba', 'porto_seco', 'Sorocaba', 'SP', -23.5018, -47.4581),
('EADI Resende', 'porto_seco', 'Resende', 'RJ', -22.4683, -44.4471),
('EADI Manaus', 'porto_seco', 'Manaus', 'AM', -3.0936, -59.9986),
-- Terminais Ferroviários
('Terminal Ferroviário Rondonópolis (Rumo)', 'terminal_ferroviario', 'Rondonópolis', 'MT', -16.4708, -54.6394),
('Terminal Alto Taquari (Rumo)', 'terminal_ferroviario', 'Alto Taquari', 'MT', -17.8333, -53.2833),
('Terminal Itiquira', 'terminal_ferroviario', 'Itiquira', 'MT', -17.2117, -54.1481),
('Terminal Aparecida do Taboado', 'terminal_ferroviario', 'Aparecida do Taboado', 'MS', -20.0867, -51.0917),
('Terminal Pederneiras (VLI)', 'terminal_ferroviario', 'Pederneiras', 'SP', -22.3514, -48.7747),
('Terminal Itirapina', 'terminal_ferroviario', 'Itirapina', 'SP', -22.2542, -47.8233),
('Terminal Estrela d''Oeste', 'terminal_ferroviario', 'Estrela d''Oeste', 'SP', -20.2872, -50.4083),
('Terminal Bauru', 'terminal_ferroviario', 'Bauru', 'SP', -22.3147, -49.0606),
('Terminal Anápolis (VLI)', 'terminal_ferroviario', 'Anápolis', 'GO', -16.3267, -48.9531),
('Terminal Catalão', 'terminal_ferroviario', 'Catalão', 'GO', -18.1714, -47.9461),
('Terminal Uberlândia', 'terminal_ferroviario', 'Uberlândia', 'MG', -18.9189, -48.2767),
('Terminal Araguari', 'terminal_ferroviario', 'Araguari', 'MG', -18.6457, -48.1869),
('Terminal Patrocínio', 'terminal_ferroviario', 'Patrocínio', 'MG', -18.9438, -46.9925),
('Terminal Porto Nacional', 'terminal_ferroviario', 'Porto Nacional', 'TO', -10.7058, -48.4178),
('Terminal Açailândia (Carajás)', 'terminal_ferroviario', 'Açailândia', 'MA', -4.9469, -47.5083),
('Terminal Cariri', 'terminal_ferroviario', 'Crato', 'CE', -7.2298, -39.4128),
('Terminal Paulínia (VLI)', 'terminal_ferroviario', 'Paulínia', 'SP', -22.7614, -47.1542),
('Terminal Lucas do Rio Verde (Ferrogrão proj.)', 'terminal_ferroviario', 'Lucas do Rio Verde', 'MT', -13.0436, -55.9136),
('Terminal Sorriso (Ferrogrão proj.)', 'terminal_ferroviario', 'Sorriso', 'MT', -12.5481, -55.7167),
('Terminal Sinop (Ferrogrão proj.)', 'terminal_ferroviario', 'Sinop', 'MT', -11.8642, -55.5103)
      ON CONFLICT (nome, tipo) DO NOTHING
    `;
    const seedRes = await pool.query(seedSql);
    const total = (await pool.query(`SELECT COUNT(*) FROM infra.terminais_logisticos`)).rows[0].count;
    console.log(`[startup] terminais logisticos OK — ${total} registros (seed inseriu ${seedRes.rowCount} novos)`);
  } catch (err) {
    console.error('[startup] ensureLogisticaTable err:', err.message);
  }
}

async function ensureRodoviasTable() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS infra.rodovias_principais (
        id SERIAL PRIMARY KEY,
        codigo TEXT NOT NULL UNIQUE,
        nome TEXT NOT NULL,
        jurisdicao TEXT,
        pavimentacao TEXT,
        geom geometry(LineString, 4674) NOT NULL
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_rodovias_geom ON infra.rodovias_principais USING GIST (geom)`);
    const seedSql = `
      INSERT INTO infra.rodovias_principais (codigo, nome, jurisdicao, pavimentacao, geom)
      VALUES
      ('BR-101', 'BR-101 (Litoral)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-35.2 -5.78, -35.71 -7.12, -34.83 -6.97, -34.85 -9.67, -37.07 -10.95, -38.51 -12.97, -40.31 -20.32, -43.18 -22.9, -46.31 -23.96, -48.66 -26.91, -48.62 -26.24, -49.27 -25.43, -50.07 -29.68, -52.1 -32.03)', 4674)),
      ('BR-116', 'BR-116 (eixo leste)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-38.54 -3.72, -38.51 -12.97, -40.84 -14.86, -43.94 -19.92, -46.63 -23.55, -49.27 -25.43, -51.1 -30.04, -52.1 -32.03)', 4674)),
      ('BR-040', 'BR-040 (RJ-DF)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-43.18 -22.9, -43.1 -22.51, -43.94 -19.92, -44.94 -17.34, -47.93 -15.83)', 4674)),
      ('BR-153', 'BR-153 (Belém-Brasília)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-48.5 -1.46, -48.36 -3.79, -48.42 -10.71, -49.27 -16.69, -49.39 -20.81, -48.27 -18.92, -47.93 -15.83)', 4674)),
      ('BR-163', 'BR-163 (Cuiabá-Santarém)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-56.1 -15.6, -55.71 -12.55, -55.92 -13.04, -55.51 -11.86, -55.97 -4.28, -54.71 -2.44)', 4674)),
      ('BR-364', 'BR-364 (Cuiabá-Rio Branco)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-56.1 -15.6, -57.68 -16.07, -58.27 -15.5, -63.9 -8.76, -67.81 -9.97, -72.66 -7.97)', 4674)),
      ('BR-262', 'BR-262 (Vitória-Cuiabá)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-40.31 -20.32, -43.94 -19.92, -44.99 -19.46, -46.99 -19.74, -49.37 -20.4, -54.62 -20.46, -57.68 -16.07, -56.1 -15.6)', 4674)),
      ('BR-070', 'BR-070 (Brasília-Cáceres)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-47.93 -15.83, -50.93 -16.33, -52.27 -15.89, -54.78 -15.55, -57.68 -16.07)', 4674)),
      ('BR-381', 'BR-381 (Fernão Dias)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-43.94 -19.92, -45.42 -21.99, -46.63 -23.55)', 4674)),
      ('BR-242', 'BR-242 (Salvador-Sorriso)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-38.51 -12.97, -41.07 -13.41, -44.52 -13.2, -45.43 -12.2, -47.95 -11.71, -50.16 -12.83, -52.55 -12.83, -55.72 -12.55)', 4674)),
      ('BR-158', 'BR-158 (Pelotas-Marabá)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-52.34 -31.77, -53.81 -29.86, -54.05 -25.96, -53.45 -24.96, -52.61 -19.99, -52.39 -15.86, -51.13 -12.96, -50.05 -7.46, -49.12 -5.37)', 4674)),
      ('BR-080', 'BR-080 (Brasília-Aragarças)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-47.93 -15.83, -50.34 -15.18, -52.24 -15.9)', 4674)),
      ('BR-060', 'BR-060 (Brasília-Bela Vista)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-47.93 -15.83, -50.93 -16.33, -54.62 -20.46, -56.45 -22.1)', 4674)),
      ('BR-369', 'BR-369 (Paraná-Bahia)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-53.46 -23.41, -51.93 -23.42, -50.31 -25.3, -49.27 -25.43)', 4674)),
      ('BR-376', 'BR-376 (Paraná-SC)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-51.93 -23.42, -50.31 -25.3, -49.27 -25.43, -48.66 -26.91)', 4674)),
      ('BR-282', 'BR-282 (SC oeste-leste)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-48.55 -27.59, -50.13 -27.21, -52.13 -26.78, -53.5 -26.49)', 4674)),
      ('BR-290', 'BR-290 (Free Way RS)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-51.23 -30.03, -52.43 -30.04, -53.46 -30.04, -55.78 -30.86)', 4674)),
      ('BR-285', 'BR-285 (RS norte)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-51.18 -28.27, -52.41 -28.26, -54.91 -27.66, -55.78 -28.66)', 4674)),
      ('BR-414', 'BR-414 (GO)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-49.25 -16.68, -49.1 -15.94, -49.95 -14.13)', 4674)),
      ('BR-251', 'BR-251 (Janaúba-MG)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-47.93 -15.83, -44.36 -15.05, -42.86 -13.51)', 4674)),
      ('BR-135', 'BR-135 (MG-MA)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-43.94 -19.92, -43.92 -16.73, -44.36 -12.3, -44.3 -9.62, -45.93 -7.1, -44.3 -2.53)', 4674)),
      ('BR-230', 'BR-230 (Transamazônica)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-34.83 -6.97, -37.32 -7.1, -41.78 -8.05, -44.99 -5.51, -49.12 -5.37, -55.99 -4.27, -60.02 -3.13, -65.74 -4.42, -69.95 -4.1)', 4674)),
      ('BR-174', 'BR-174 (Manaus-Boa Vista)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-60.02 -3.13, -60.71 -1.41, -60.66 2.81)', 4674)),
      ('BR-101N', 'BR-101 Norte (BA-RN)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-38.51 -12.97, -37.07 -10.95, -34.85 -9.67, -35.74 -9.65, -34.85 -8.05, -34.83 -6.97, -35.2 -5.78)', 4674)),
      ('BR-369-MS', 'BR-369 MS', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-54.62 -20.46, -52.41 -20.18, -50.31 -20.08)', 4674)),
      ('BR-470', 'BR-470 (SC)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-48.66 -26.91, -49.07 -26.91, -50.5 -27.1)', 4674)),
      ('BR-280', 'BR-280 (SC norte)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-48.61 -26.24, -49.07 -26.27, -50.36 -26.42, -52.36 -26.41)', 4674)),
      ('BR-422', 'BR-422 (RO)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-63.9 -8.76, -62.06 -9.3)', 4674)),
      ('BR-156', 'BR-156 (AP)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-51.05 -0.04, -50.34 3.83)', 4674)),
      ('BR-104', 'BR-104 (PE)', 'federal', 'pavimentada', ST_GeomFromText('LINESTRING(-34.85 -9.67, -35.74 -9.65, -36.69 -8.89)', 4674))
      ON CONFLICT (codigo) DO NOTHING
    `;
    const res = await pool.query(seedSql);
    const total = (await pool.query(`SELECT COUNT(*) FROM infra.rodovias_principais`)).rows[0].count;
    console.log(`[startup] rodovias principais OK — ${total} BRs (seed inseriu ${res.rowCount} novos)`);
  } catch (err) {
    console.error('[startup] ensureRodoviasTable err:', err.message);
  }
}

async function ensureGistIndexes() {
  const client = await pool.connect();
  try {
    const schemas = ['public', 'incra', 'car', 'anm'];
    const gc = await client.query(
      "SELECT f_table_schema s, f_table_name t, f_geometry_column c " +
      "FROM geometry_columns " +
      "WHERE f_table_schema = ANY($1) AND type NOT ILIKE $2",
      [schemas, '%RASTER%']
    );
    for (const row of gc.rows) {
      const tbl = row.s === 'public' ? row.t : (row.s + '.' + row.t);
      const idx = ('gist_' + row.s + '_' + row.t + '_' + row.c).substring(0, 63);
      try {
        await client.query(
          "CREATE INDEX IF NOT EXISTS " + idx + " ON " + tbl + " USING GIST (" + row.c + ")"
        );
      } catch (ie) { /* tabela pode nao existir ainda */ }
    }
    console.log('[startup] GIST indices OK:', gc.rows.length);

    // BTREE indices em cod_imovel — necessários p/ queries WHERE cod_imovel = X
    // sem isso, sequential scan em tabelas CAR (300k+ linhas) timeout
    const carPrefixes = ['area_imovel', 'reserva_legal', 'vegetacao_nativa', 'area_consolidada', 'apps'];
    const ufsAll = ['ac','al','am','ap','ba','ce','df','es','go','ma','mg','ms','mt','pa','pb','pe','pi','pr','rj','rn','ro','rr','rs','sc','se','sp','to'];
    let btreeOk = 0;
    for (const prefix of carPrefixes) {
      for (const uf of ufsAll) {
        const tbl = `car.${prefix}_${uf}`;
        const idx = `btree_car_${prefix}_${uf}_cod_imovel`.substring(0, 63);
        try {
          await client.query(`CREATE INDEX IF NOT EXISTS ${idx} ON ${tbl} (cod_imovel)`);
          btreeOk++;
        } catch (ie) { /* tabela pode nao existir */ }
      }
    }
    console.log('[startup] BTREE indices cod_imovel CAR OK:', btreeOk);
  } catch (err) {
    console.error('[startup] GIST err:', err.message);
  } finally { client.release(); }
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/**
 * Builds a geometry filter that handles SRID=0 with ST_SetSRID pattern
 * @param {string} geomColumn - Column name (e.g. 'geom')
 * @param {number} expectedSrid - Expected SRID (default 4674)
 * @returns {string} - SQL CASE expression
 */
function buildGeomFilter(geomColumn, expectedSrid = 4674) {
  return `CASE WHEN ST_SRID(${geomColumn}) != ${expectedSrid} THEN ST_SetSRID(${geomColumn}, ${expectedSrid}) ELSE ${geomColumn} END`;
}

// Approximate bounding boxes [minLng, minLat, maxLng, maxLat] — module-level for reuse
const STATE_BBOX = {
  ac: [-74.0,-11.15,-66.5,-7.1],   al: [-38.25,-10.5,-35.15,-8.8],
  am: [-73.8,-9.9,-56.1,2.3],      ap: [-52.0,1.0,-49.9,4.45],
  ba: [-46.6,-18.35,-37.3,-8.5],   ce: [-41.4,-7.85,-37.25,-2.75],
  df: [-48.3,-16.05,-47.3,-15.5],  es: [-41.9,-21.3,-39.6,-17.9],
  go: [-53.3,-19.5,-45.9,-12.4],   ma: [-48.7,-10.25,-41.8,-1.05],
  mg: [-51.05,-22.95,-39.85,-14.25],ms: [-58.2,-24.1,-50.95,-17.2],
  mt: [-61.6,-18.05,-50.2,-7.35],  pa: [-58.5,-9.85,-46.0,2.6],
  pb: [-38.8,-8.35,-34.8,-6.0],    pe: [-41.35,-9.5,-34.8,-7.15],
  pi: [-45.95,-10.95,-40.35,-2.75],pr: [-54.65,-26.75,-48.05,-22.5],
  rj: [-44.9,-23.4,-40.95,-20.75], rn: [-38.6,-6.99,-35.0,-4.85],
  ro: [-66.85,-13.7,-59.85,-7.95], rr: [-64.8,1.25,-59.8,5.3],
  rs: [-57.65,-33.75,-49.7,-27.1], sc: [-53.85,-29.4,-48.35,-25.95],
  se: [-38.25,-11.6,-36.4,-9.5],   sp: [-53.15,-25.35,-44.15,-19.8],
  to: [-50.75,-13.5,-45.7,-5.15],
};

/**
 * Determines UF from lat/lng using hardcoded bounding boxes — no DB required.
 * Returns the UF whose bbox contains the point (smallest area wins for overlaps).
 */
function getUFFromPointFallback(lat, lng) {
  const matches = ALL_UFS.filter(uf => {
    const b = STATE_BBOX[uf];
    if (!b) return false;
    return lng >= b[0] && lng <= b[2] && lat >= b[1] && lat <= b[3];
  });
  if (matches.length === 0) return null;
  // Pick the state with the smallest bounding box area (most specific match)
  return matches.sort((a, b) => {
    const ba = STATE_BBOX[a], bb = STATE_BBOX[b];
    return ((ba[2]-ba[0]) * (ba[3]-ba[1])) - ((bb[2]-bb[0]) * (bb[3]-bb[1]));
  })[0];
}

/**
 * Determines the UF (state abbreviation, lowercase) from a lat/lng point
 * Uses municipios.municipios_2024 table, falls back to bbox matching
 */
async function getUFFromPoint(lat, lng) {
  try {
    // Use ST_SetSRID + ST_MakePoint (avoids EWKT parsing issues with ST_GeomFromText)
    const result = await pool.query(`
      SELECT LOWER("SIGLA_UF") as uf_lower
      FROM municipios.municipios_2024
      WHERE ST_Contains(geom, ST_SetSRID(ST_MakePoint($1, $2), ${SRID}))
      LIMIT 1
    `, [lng, lat]);
    return result.rows[0]?.uf_lower || null;
  } catch (e) {
    console.warn('getUFFromPoint DB query failed:', e.message);
    return null;
  }
}

/**
 * Resolves UF from DB first, falls back to bbox matching — never defaults to 'mt'
 */
async function resolveUF(lat, lng) {
  const ufFromDb = await getUFFromPoint(lat, lng);
  if (ufFromDb) return ufFromDb;
  const ufFromBbox = getUFFromPointFallback(lat, lng);
  if (ufFromBbox) {
    console.warn(`UF resolved via bbox fallback: ${ufFromBbox} for lat=${lat} lng=${lng}`);
    return ufFromBbox;
  }
  console.warn(`UF not resolved, defaulting to mt for lat=${lat} lng=${lng}`);
  return 'mt';
}

/**
 * Determines the UF from a GeoJSON feature (uses centroid)
 */
async function getUFFromGeoJSON(geojsonStr) {
  try {
    const result = await pool.query(`
      SELECT LOWER(m."SIGLA_UF") as uf_lower
      FROM municipios.municipios_2024 m
      WHERE m.geom && ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AND ST_Intersects(CASE WHEN ST_SRID(m.geom) != ${SRID} THEN ST_SetSRID(m.geom, ${SRID}) ELSE m.geom END, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))
      LIMIT 1
    `, [geojsonStr]);
    return result.rows[0]?.uf_lower || null;
  } catch (e) {
    return null;
  }
}

// All 27 Brazilian state codes (lowercase) — used for state-partitioned layer queries
const ALL_UFS = [
  'ac','al','am','ap','ba','ce','df','es','go',
  'ma','mg','ms','mt','pa','pb','pe','pi','pr',
  'rj','rn','ro','rr','rs','sc','se','sp','to'
];

/**
 * Returns UF codes whose approximate bounding box overlaps the given bbox.
 * Uses hardcoded state bboxes — no ibge dependency.
 */
function getUFsFromBbox(minLng, minLat, maxLng, maxLat) {
  const overlapping = ALL_UFS.filter(uf => {
    const b = STATE_BBOX[uf];
    if (!b) return true; // include if unknown
    // Check overlap: not (maxLng < b[0] || minLng > b[2] || maxLat < b[1] || minLat > b[3])
    return !(maxLng < b[0] || minLng > b[2] || maxLat < b[1] || minLat > b[3]);
  });

  return overlapping.length > 0 ? overlapping : ALL_UFS;
}

/**
 * Fetch PRODES/DETER do WFS TerraBrasilis pra um polígono (com timeout curto + fallback gracioso)
 */
async function fetchWFS(typeName, bbox, timeoutMs = 8000) {
  const url = `https://terrabrasilis.dpi.inpe.br/geoserver/ows?service=WFS&version=2.0.0&request=GetFeature&typeName=${encodeURIComponent(typeName)}&outputFormat=application/json&bbox=${bbox.join(',')},EPSG:4326&count=500`;
  const ctrl = new AbortController();
  const tid = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const r = await fetch(url, { signal: ctrl.signal });
    clearTimeout(tid);
    if (!r.ok) return { features: [], error: `HTTP ${r.status}` };
    const j = await r.json();
    return j;
  } catch (e) {
    clearTimeout(tid);
    return { features: [], error: e.message || 'fetch err' };
  }
}

/**
 * Computa intersect de cada feature do WFS com o polígono da parcela via PostGIS
 * Recebe features GeoJSON + geojsonStr da parcela
 * Retorna lista filtrada com area_ha real da interseção
 */
async function intersectFeaturesWithParcel(features, geojsonStr, propsToKeep) {
  if (!features || !features.length) return [];
  // Mapeia cada feature p/ {props, geomJson}
  const itens = features.map(f => ({ props: f.properties || {}, geom: f.geometry }));
  // Faz UMA query batched: passa array de geoms via UNNEST
  const geoms = itens.map(i => JSON.stringify(i.geom));
  try {
    const r = await pool.query(`
      WITH parcel AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AS g),
      feats AS (
        SELECT idx, ST_SetSRID(ST_GeomFromGeoJSON(geom_str), 4674) AS geom
        FROM unnest($2::text[]) WITH ORDINALITY AS u(geom_str, idx)
      )
      SELECT f.idx::int AS idx,
             ST_Intersects(f.geom, p.g) AS hits,
             CASE WHEN ST_Intersects(f.geom, p.g)
               THEN ROUND(CAST(ST_Area(ST_Transform(ST_Intersection(f.geom, p.g), 32721))/10000 AS numeric), 4)
               ELSE 0 END AS area_ha,
             -- Bbox da intersecção (parte da feature dentro da parcela) p/ buscar imagem do trecho relevante
             ST_XMin(ST_Intersection(f.geom, p.g))::float8 AS minx,
             ST_YMin(ST_Intersection(f.geom, p.g))::float8 AS miny,
             ST_XMax(ST_Intersection(f.geom, p.g))::float8 AS maxx,
             ST_YMax(ST_Intersection(f.geom, p.g))::float8 AS maxy,
             ST_X(ST_Centroid(ST_Intersection(f.geom, p.g)))::float8 AS clng,
             ST_Y(ST_Centroid(ST_Intersection(f.geom, p.g)))::float8 AS clat
      FROM feats f, parcel p
      ORDER BY f.idx
    `, [geojsonStr, geoms]);
    const out = [];
    for (const row of r.rows) {
      if (!row.hits) continue;
      const item = itens[row.idx - 1];
      const filtered = {};
      for (const k of propsToKeep) if (item.props[k] !== undefined) filtered[k] = item.props[k];
      out.push({
        ...filtered,
        area_ha: parseFloat(row.area_ha),
        bbox: [parseFloat(row.minx), parseFloat(row.miny), parseFloat(row.maxx), parseFloat(row.maxy)],
        centroid: { lng: parseFloat(row.clng), lat: parseFloat(row.clat) },
      });
    }
    return out;
  } catch (e) {
    console.warn('[intersect-features]', e.message);
    return [];
  }
}

/**
 * Safe query that returns empty array on error (for state-specific tables that may not exist)
 */
async function safeQuery(sql, params) {
  try {
    const result = await pool.query(sql, params);
    return result;
  } catch (e) {
    console.warn('Safe query failed:', e.message);
    return { rows: [] };
  }
}

/**
 * Builds area percentage query
 * @param {string} table - Table name
 * @param {string} geomColumn - Geometry column name
 * @param {number} expectedSrid - Expected SRID
 * @returns {string} - SQL for area percentages
 */
function buildAreaPercentQuery(table, geomColumn, expectedSrid = 4674) {
  const safeGeom = buildGeomFilter(geomColumn, expectedSrid);
  return `
    SELECT
      nome,
      ROUND(CAST(SUM(ST_Area(ST_Intersection(${safeGeom}, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)))) /
                     ST_Area(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)) * 100 AS numeric), 2) as percentual
    FROM ${table}
    WHERE ${safeGeom} && ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AND ST_Intersects(${safeGeom}, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))
    GROUP BY nome
    ORDER BY percentual DESC
  `;
}

// ============================================================================
// ANA HidroWebservice Integration
// Token auth with 55-min cache to avoid IP-block from excessive auth requests
// ============================================================================
const ANA_BASE = 'https://www.ana.gov.br/hidrowebservice/EstacoesTelemetricas';
const ANA_IDENTIFICADOR = process.env.ANA_IDENTIFICADOR || '07639595000179';
const ANA_SENHA = process.env.ANA_SENHA || 'erk1gqsk';
const _anaToken = { value: null, expiresAt: 0 };

async function getAnaToken() {
  if (_anaToken.value && Date.now() < _anaToken.expiresAt) return _anaToken.value;
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 10000);
  try {
    const resp = await fetch(`${ANA_BASE}/OAUth/v1`, {
      headers: { 'Identificador': ANA_IDENTIFICADOR, 'Senha': ANA_SENHA, 'Accept': 'application/json' },
      signal: ctrl.signal,
    });
    clearTimeout(timer);
    if (!resp.ok) throw new Error(`ANA auth HTTP ${resp.status}`);
    const data = await resp.json();
    const itm   = data?.items;
    const token = data?.tokenautenticacao
      || data?.token || data?.access_token
      || (typeof itm === 'string' && itm.length > 10 ? itm : null)
      || itm?.tokenautenticacao || itm?.token || itm?.access_token
      || (Array.isArray(itm)  && itm[0]?.tokenautenticacao)
      || (Array.isArray(data) && data[0]?.tokenautenticacao);
    if (!token) throw new Error(`ANA token field not found — keys: ${Object.keys(data || {}).join(',')}`);
    _anaToken.value = token;
    _anaToken.expiresAt = Date.now() + 55 * 60 * 1000;
    console.log('[ANA] Token obtained, valid 55 min');
    return token;
  } catch (e) {
    clearTimeout(timer);
    throw e;
  }
}

async function anaGet(path, params = {}) {
  const token = await getAnaToken();
  const qs = new URLSearchParams(params).toString();
  const url = `${ANA_BASE}/${path}${qs ? '?' + qs : ''}`;
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 15000);
  try {
    console.log('[ANA] GET', url, 'token[:20]:', token ? String(token).slice(0,20) : 'NONE');
    const resp = await fetch(url, {
      headers: { 'Authorization': `Bearer ${token}`, 'Accept': '*/*' },
      signal: ctrl.signal,
    });
    clearTimeout(timer);
    if (!resp.ok) {
      let body406 = '';
      try { body406 = await resp.text(); } catch(_) {}
      console.log('[ANA] HTTP', resp.status, 'body[:200]:', body406.slice(0, 200));
      throw new Error(`ANA ${path} HTTP ${resp.status}`);
    }
    const ct = resp.headers.get('content-type') || '';
    return ct.includes('json') ? resp.json() : resp.json();
  } catch (e) {
    clearTimeout(timer);
    throw e;
  }
}

/** Haversine distance in km */
function haversineKm(lat1, lon1, lat2, lon2) {
  const R = 6371, toRad = d => d * Math.PI / 180;
  const dLat = toRad(lat2 - lat1), dLon = toRad(lon2 - lon1);
  const a = Math.sin(dLat/2)**2 + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon/2)**2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}


// ─── Pluviometria PostGIS (PMA Brasil 1977-2006 do schema pluviometria) ───
async function fetchPluviometriaPostGIS(lat, lng, geojson) {
  console.log('[pluvio-postgis] CALLED lat=' + lat + ' lng=' + lng + ' has_geojson=' + (geojson ? typeof geojson : 'null'));
  function extractGeometry(g) {
    if (!g) return null;
    if (typeof g === 'string') {
      try { g = JSON.parse(g); } catch (e) { console.log('[pluvio-postgis] JSON.parse falhou:', e.message); return null; }
    }
    if (g.type === 'Feature') return extractGeometry(g.geometry);
    if (g.type === 'FeatureCollection' && g.features?.length) return extractGeometry(g.features[0].geometry);
    if (['Polygon', 'MultiPolygon', 'Point', 'MultiPoint', 'LineString', 'MultiLineString'].includes(g.type)) return g;
    return null;
  }
  try {
    let pma = null;
    const geom = extractGeometry(geojson);
    console.log('[pluvio-postgis] extracted geometry type:', geom?.type || 'null');
    if (geom && (geom.type === 'Polygon' || geom.type === 'MultiPolygon')) {
      try {
        const r1 = await pool.query(`
          WITH parcel AS (
            SELECT ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON($1::text), 4326), 4674) AS geom
          )
          SELECT AVG((ST_SummaryStats(ST_Clip(r.rast, parcel.geom, true), 1, true)).mean)::float AS pma_mm
          FROM pluviometria.precipitacao_brasil_1977_2006 r, parcel
          WHERE ST_Intersects(r.rast, parcel.geom)
        `, [JSON.stringify(geom)]);
        pma = r1.rows[0]?.pma_mm;
        console.log('[pluvio-postgis] poligono retornou pma=' + pma + ' (rows=' + r1.rows.length + ')');
      } catch (ePoly) {
        console.warn('[pluvio-postgis] poligono falhou, fallback para ponto:', ePoly?.message);
      }
    }
    if (pma == null || pma < 0 || isNaN(pma)) {
      const r2 = await pool.query(`
        SELECT ST_Value(r.rast, ST_Transform(ST_SetSRID(ST_MakePoint($1, $2), 4326), 4674))::float AS pma_mm
        FROM pluviometria.precipitacao_brasil_1977_2006 r
        WHERE ST_Intersects(r.rast, ST_Transform(ST_SetSRID(ST_MakePoint($1, $2), 4326), 4674))
        LIMIT 1
      `, [lng, lat]);
      pma = r2.rows[0]?.pma_mm;
      console.log('[pluvio-postgis] ponto retornou pma=' + pma + ' (rows=' + r2.rows.length + ')');
    }
    if (pma == null || pma < 0 || isNaN(pma)) {
      console.log('[pluvio-postgis] retornando null (pma invalido):', pma);
      return null;
    }
    const result = Math.round(pma);
    console.log('[pluvio-postgis] OK retornando ' + result + ' mm');
    return result;
  } catch (e) {
    console.warn('[pluvio-postgis] excecao:', e.message, e.stack?.substring(0, 300));
    return null;
  }
}

async function fetchPluviometriaANA(lat, lng, uf = 'MT') {
  const MONTHS_PT = ['Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez'];
  const MAX_DIST_KM  = 600;

  // ── helper: fetch series for one station, return monthly averages ──
  async function fetchStationData(stationCode, stationName, dist) {
    const now = new Date();
    const tenYearsAgo = new Date(now);
    tenYearsAgo.setFullYear(tenYearsAgo.getFullYear() - 10);
    const chunks = [];
    let cur = new Date(tenYearsAgo);
    while (cur < now) {
      const end = new Date(cur); end.setDate(end.getDate() + 364);
      if (end > now) end.setTime(now.getTime());
      chunks.push({ start: cur.toISOString().split('T')[0], end: end.toISOString().split('T')[0] });
      cur = new Date(end); cur.setDate(cur.getDate() + 1);
    }
    const allRows = [];
    for (const chunk of chunks) {
      try {
        const series = await anaGet('HidroSerieChuva/v1', {
          codigoEstacao: stationCode, dataInicio: chunk.start, dataFim: chunk.end,
        });
        const rows = series?.items
          || series?.chuvas
          || series?.value
          || series?.Registro
          || series?.registro
          || series?.HidroSerieChuva?.Registro
          || series?.HidroSerieChuva?.registro
          || (series?.HidroSerieChuva && Object.values(series.HidroSerieChuva).find(v => Array.isArray(v)))
          || series;
        if (Array.isArray(rows)) allRows.push(...rows);
      } catch (e) {
        console.warn(`[ANA] ${stationCode} bloco ${chunk.start} falhou: ${e.message}`);
      }
    }
    if (allRows.length === 0) return null;
    // aggregate to year-month totals
    const ym = {};
    for (const r of allRows) {
      const ds = r.dataHora || r.data || r.date || r.DT_MEDICAO || r.DataHora || '';
      const v  = parseFloat(r.chuvaTotal || r.chuva || r.value || r.CHUVA || r.Chuva || 0);
      const d  = new Date(ds);
      if (isNaN(d.getTime()) || d.getFullYear() < 2000) continue;
      const k = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`;
      ym[k] = (ym[k] || 0) + (isNaN(v) ? 0 : v);
    }
    // monthly averages (1-12)
    const buckets = {};
    for (const [k, t] of Object.entries(ym)) {
      const m = parseInt(k.split('-')[1]);
      buckets[m] = buckets[m] || []; buckets[m].push(t);
    }
    const monthAvg = Array.from({length:12}, (_,i) => {
      const vals = buckets[i+1] || [];
      return vals.length > 0 ? vals.reduce((s,v)=>s+v,0)/vals.length : null;
    });
    // annual totals
    const ytMap = {};
    for (const [k,t] of Object.entries(ym)) {
      const y = k.split('-')[0]; ytMap[y] = (ytMap[y]||0) + t;
    }
    return { stationCode, stationName, dist, monthAvg, ytMap };
  }

  try {
    // ── 1. Inventário de estações pluviométricas próximas ──
    const delta = 6.0;
    // ── 1. Buscar inventário de estações pluviométricas por UF ──
    try {
      const ufCode = (uf || 'MT').toUpperCase();
      const inv = await anaGet('HidroInventarioEstacoes/v1', {
        tipoEstacao: '2',
        uf: ufCode,
      });
      console.log('[ANA] inv keys:', inv && typeof inv === 'object' ? Object.keys(inv).join(',') : String(inv).slice(0,100));
      const raw = inv?.items
        || inv?.value
        || inv?.estacoes
        || inv?.Estacao
        || inv?.estacao
        || inv?.HidroInventarioEstacoes?.Estacao
        || inv?.HidroInventarioEstacoes?.estacao
        || (inv?.HidroInventarioEstacoes && Object.values(inv.HidroInventarioEstacoes).find(v => Array.isArray(v)))
        || inv;
      if (Array.isArray(raw)) stations = raw;
      else if (raw && typeof raw === 'object') stations = Object.values(raw);
    } catch (e) {
      console.warn('[ANA] Inventário falhou:', e.message);
    }
    if (stations.length === 0) {
      return { pendente: false, erro: 'Nenhuma estação pluviométrica ANA encontrada', resumo: null, media_mensal: [], total_anual: [] };
    }

    // ── 2. Selecionar todas as estações dentro de MAX_DIST_KM ──
    const candidates = stations
      .map(s => {
        const sLat = parseFloat(s.latitude  || s.lat  || s.Latitude  || s.LAT || 0);
        const sLng = parseFloat(s.longitude || s.lon  || s.Longitude || s.LON || 0);
        const code = String(s.codigoEstacao || s.codigo || s.Codigo || s.code || '');
        const name = s.nomeEstacao || s.nome || s.Nome || code;
        return { code, name, lat: sLat, lng: sLng, dist: haversineKm(lat, lng, sLat, sLng) };
      })
      .filter(s => s.dist <= MAX_DIST_KM && s.code)
      .sort((a, b) => a.dist - b.dist);

    if (candidates.length === 0) {
      return { pendente: false, erro: `Nenhuma estação ANA dentro de ${MAX_DIST_KM} km`, resumo: null, media_mensal: [], total_anual: [] };
    }

    console.log(`[ANA] Usando ${candidates.length} estações para triangulação IDW`);
    candidates.forEach(c => console.log(`  • ${c.code} (${c.name}) — ${c.dist.toFixed(1)} km`));

    // ── 3. Buscar dados de todas as estações em paralelo ──
    const results = await Promise.all(
      candidates.map(c => fetchStationData(c.code, c.name, c.dist))
    );
    const valid = results.filter(r => r !== null);

    if (valid.length === 0) {
      return { pendente: false, erro: 'Nenhuma estação retornou dados de chuva nos últimos 10 anos', resumo: null, media_mensal: [], total_anual: [] };
    }

    // ── 4. IDW: peso = 1/dist² para interpolação no ponto da fazenda ──
    const weights = valid.map(r => r.dist <= 0.1 ? 1e9 : 1 / (r.dist * r.dist));
    const totalW  = weights.reduce((s, w) => s + w, 0);

    // Média mensal IDW
    const media_mensal = Array.from({length:12}, (_, i) => {
      let num = 0, den = 0;
      valid.forEach((r, j) => {
        if (r.monthAvg[i] !== null) { num += weights[j] * r.monthAvg[i]; den += weights[j]; }
      });
      return { mes: MONTHS_PT[i], media_mm: den > 0 ? Math.round(num / den) : 0 };
    });

    // Total anual IDW (por ano)
    const allYears = [...new Set(valid.flatMap(r => Object.keys(r.ytMap)))].sort();
    const total_anual = allYears.map(ano => {
      let num = 0, den = 0;
      valid.forEach((r, j) => {
        if (r.ytMap[ano] != null) { num += weights[j] * r.ytMap[ano]; den += weights[j]; }
      });
      return { ano, total_mm: den > 0 ? Math.round(num / den) : 0 };
    });

    const mediaAnualMm   = total_anual.length > 0
      ? Math.round(total_anual.reduce((s,a) => s + a.total_mm, 0) / total_anual.length) : 0;
    const mesMaisChuvoso = [...media_mensal].sort((a,b) => b.media_mm - a.media_mm)[0] || null;
    const mesMaisSeco    = [...media_mensal].sort((a,b) => a.media_mm - b.media_mm)[0] || null;

    const stationsDesc = valid
      .map(r => `${r.stationName} (${r.dist.toFixed(0)} km)`)
      .join(', ');

    return {
      pendente: false, erro: null,
      resumo: {
        media_anual_mm:   mediaAnualMm,
        mes_mais_chuvoso: mesMaisChuvoso,
        mes_mais_seco:    mesMaisSeco,
        fonte:            `IDW ${valid.length} estações ANA: ${stationsDesc}`,
        estacoes_usadas:  valid.length,
        latitude:         lat.toFixed(4),
        longitude:        lng.toFixed(4),
      },
      media_mensal,
      total_anual,
    };

  } catch (e) {
    console.error('[ANA] fetchPluviometriaANA error:', e.message);
    return { pendente: false, erro: `Erro ANA HidroWeb: ${e.message}`, resumo: null, media_mensal: [], total_anual: [] };
  }
}

// ============================================================================
// ____________________________________________________________________________
// NASA POWER — precipitação histórica mensal (MERRA-2 PRECTOTCORR)
// ____________________________________________________________________________
async function fetchPluviometriaCHIRPS(lat, lng) {
  const MESES = ['Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez'];
  function mkBuckets(rows, dateKey, valKey) {
    const mo = {}, yr = {};
    for (const r of rows) {
      const d = new Date(r[dateKey] || '');
      if (isNaN(d.getTime())) continue;
      const val = parseFloat(valKey ? r[valKey] : (r.value?.avg ?? r.value?.sum ?? r.value ?? 0));
      if (isNaN(val) || val < 0) continue;
      const m = d.getMonth() + 1, y = String(d.getFullYear());
      (mo[m] = mo[m] || []).push(val);
      yr[y] = (yr[y] || 0) + val;
    }
    return { mo, yr };
  }
  function mkResult(mo, yr, fonte) {
    const mm = Array.from({ length: 12 }, (_, i) => {
      const vals = mo[i+1] || [];
      return { mes: MESES[i], media_mm: vals.length ? Math.round(vals.reduce((s,v)=>s+v,0)/vals.length) : 0 };
    });
    const ta = Object.entries(yr).map(([ano,total_mm])=>({ ano, total_mm: Math.round(total_mm) })).sort((a,b)=>a.ano.localeCompare(b.ano));
    const med = ta.length ? Math.round(ta.reduce((s,a)=>s+a.total_mm,0)/ta.length) : 0;
    return { pendente: false, erro: null,
      resumo: { media_anual_mm: med,
        mes_mais_chuvoso: [...mm].sort((a,b)=>b.media_mm-a.media_mm)[0]||null,
        mes_mais_seco: [...mm].sort((a,b)=>a.media_mm-b.media_mm)[0]||null,
        fonte, latitude: lat.toFixed(4), longitude: lng.toFixed(4) },
      media_mensal: mm, total_anual: ta };
  }
  function cFetch(url, opts, ms) {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), ms);
    return fetch(url, { ...opts, signal: ctrl.signal }).finally(() => clearTimeout(t));
  }

  // PRIMARY: CHIRPS v2.0 via ClimateSERV (0.05 deg / 5 km, 1994-2024)
  try {
    const body = new URLSearchParams({
      datatype: '26', begintime: '01/01/2020', endtime: '12/31/2024',
      intervaltype: '0', operationtype: '5', callback: '', isZip_FILE: 'false',
      geometry: JSON.stringify({ type: 'Point', coordinates: [+lng.toFixed(5), +lat.toFixed(5)] })
    });
    const sr = await cFetch('https://climateserv.servirglobal.net/api/submitDataRequest/', { method: 'POST', body, headers: { 'Content-Type': 'application/x-www-form-urlencoded' } }, 12000);
    if (!sr.ok) throw new Error('submit ' + sr.status);
    const raw = await sr.json();
    const jid = Array.isArray(raw) ? raw[0] : raw;
    if (!jid || jid === 'null') throw new Error('null job');
    console.log('[CHIRPS] job=' + jid);
    const dl = Date.now() + 15000;
    let done = false;
    while (Date.now() < dl) {
      await new Promise(r => setTimeout(r, 3000));
      try {
        const pr = await cFetch('https://climateserv.servirglobal.net/api/getDataRequestProgress/?id=' + encodeURIComponent(String(jid)), {}, 8000);
        const pg = await pr.json();
        const pct = Array.isArray(pg) ? (pg[0]?.progress ?? 0) : (pg?.progress ?? 0);
        if (pct >= 100) { done = true; break; }
      } catch(e) { console.warn('[CHIRPS] poll', e.message); }
    }
    if (!done) throw new Error('timeout');
    const dr = await cFetch('https://climateserv.servirglobal.net/api/getDataFromRequest/?id=' + encodeURIComponent(String(jid)), {}, 10000);
    if (!dr.ok) throw new Error('getData ' + dr.status);
    const data = await dr.json();
    const rows = Array.isArray(data) ? data : (data.data || data.value || []);
    if (!rows.length) throw new Error('empty');
    const { mo, yr } = mkBuckets(rows, 'date', null);
    console.log('[CHIRPS] OK rows=' + rows.length);
    return mkResult(mo, yr, 'CHIRPS v2.0 (2020-2024) 0.05 grau / 5 km');
  } catch(e) {
    console.warn('[CHIRPS] ClimateSERV failed:', e.message, '-> ERA5-Land fallback');
  }

    // FALLBACK: NASA POWER API (PRECTOTCORR, 0.5 deg / 55 km, 1984-present)
  // Designed for server/research access — no IP restrictions, free, no auth
  try {
    const baseUrl = 'https://power.larc.nasa.gov/api/temporal/monthly/point';
    const qp = 'parameters=PRECTOTCORR&community=RE&format=JSON&start=1994&end=2024';
    const url = baseUrl + '?' + qp + '&longitude=' + lng.toFixed(4) + '&latitude=' + lat.toFixed(4);
    const res = await cFetch(url, {}, 30000);
    if (!res.ok) throw new Error('NASA POWER ' + res.status);
    const data = await res.json();
    const param = data.properties?.parameter?.PRECTOTCORR;
    if (!param) throw new Error('NASA POWER: PRECTOTCORR ausente');
    const mo = {}, yr = {};
    for (const [ym, mmDay] of Object.entries(param)) {
      if (mmDay === null || mmDay < -900) continue; // skip fill values (-999)
      const y = ym.substring(0, 4), m = parseInt(ym.substring(4, 6));
      if (m < 1 || m > 12) continue; // skip annual summary entries
      const days = new Date(parseInt(y), m, 0).getDate();
      const val = mmDay * days; // mm/day -> monthly total mm
      (mo[m] = mo[m] || []).push(val);
      yr[y] = (yr[y] || 0) + val;
    }
    return mkResult(mo, yr, 'NASA POWER (1994-2024) 0.5 grau / 55 km');
} catch(e) {
    console.error('[ERA5-Land] failed:', e.message);
    return { pendente: false, erro: 'Pluviometria indisponivel: ' + e.message, resumo: null, media_mensal: [], total_anual: [] };
  }
}

// ============================================================================
// SoilGrids v2.0 — ISRIC — propriedades do solo por camada
// ============================================================================
async function fetchSoilGrids(lat, lng) {
  const props  = ['clay', 'sand', 'silt', 'soc', 'phh2o', 'nitrogen', 'bdod'];
  const depths = ['0-5cm', '5-15cm', '15-30cm', '30-60cm'];

  const qs = new URLSearchParams();
  qs.append('lon', lng.toFixed(6));
  qs.append('lat', lat.toFixed(6));
  props.forEach(p  => qs.append('property', p));
  depths.forEach(d => qs.append('depth', d));
  ['mean'].forEach(val => qs.append('value', val));

  const url = 'https://rest.isric.org/soilgrids/v2.0/properties/query?' + qs.toString();

  try {
    console.log('[SoilGrids] fetch', url.slice(0, 80));
    // Timeout: ISRIC pode ser lento — aguarda ate 20s antes de desistir
    const resp = await fetch(url, { signal: AbortSignal.timeout(20000) });
    if (!resp.ok) throw new Error('SoilGrids HTTP ' + resp.status);
    const data = await resp.json();

    const factor = { clay: 0.1, sand: 0.1, silt: 0.1, soc: 0.1, phh2o: 0.1, nitrogen: 0.01, bdod: 0.01 };
    const label  = { clay: 'argila_%', sand: 'areia_%', silt: 'silte_%', soc: 'carbono_organico_g_kg', phh2o: 'ph', nitrogen: 'nitrogenio_g_kg', bdod: 'densidade_g_cm3' };

    const camadas = {};
    const layers = (data && data.properties && data.properties.layers) ? data.properties.layers : [];
    for (const layer of layers) {
      for (const dep of (layer.depths || [])) {
        const d   = dep.label;
        const val = dep.values && dep.values.mean;
        if (!camadas[d]) camadas[d] = {};
        camadas[d][label[layer.name]] = (val !== null && val !== undefined)
          ? Math.round(val * factor[layer.name] * 10) / 10
          : null;
      }
    }
    return { pendente: false, erro: null, camadas };
  } catch (e) {
    console.warn('[SoilGrids] erro:', e.message);
    return { pendente: false, erro: 'SoilGrids indisponivel: ' + e.message, camadas: null };
  }
}
function parseGeoJSONFeature(geojson) {
  if (typeof geojson === 'string') {
    return JSON.parse(geojson);
  }
  return geojson;
}
// ============================================================================
// ENDPOINTS
// ============================================================================

/**
 * Health check endpoint
 */
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

/**
 * POST /api/buscar-feicao
 * Click search with SIGEF→SNCI→CAR priority cascade
 * Finds which parcel contains the clicked point
 */
app.post('/api/buscar-feicao', async (req, res) => {
  try {
    // Accept both lat/lng (frontend) and latitude/longitude (legacy)
    const lat = req.body.lat ?? req.body.latitude;
    const lng = req.body.lng ?? req.body.longitude;
    const latitude = lat;
    const longitude = lng;

    // Respeita as camadas ativas enviadas pelo frontend
    // ['sigef'] → só SIGEF/SNCI | ['car'] → só CAR | [] ou undefined → cascata completa
    const camadasAtivas = req.body.camadasAtivas;
    const querySigef = !camadasAtivas || camadasAtivas.length === 0 || camadasAtivas.includes('sigef');
    const queryCar   = !camadasAtivas || camadasAtivas.length === 0 || camadasAtivas.includes('car');

    if (!longitude || !latitude) {
      return res.status(400).json({ error: 'lat/lng required' });
    }

    const point = `SRID=${SRID};POINT(${longitude} ${latitude})`;

    // Determine UF dynamically from coordinates (DB → bbox fallback, never hardcoded 'mt')
    const uf = await resolveUF(latitude, longitude);
    const sigefTable = `incra.sigef_${uf}`;
    const snciTable = `incra.snci_${uf}`;
    const carTable = `car.area_imovel_${uf}`;

    let result = { rows: [] };

    // Busca SIGEF apenas se a camada estiver ativa
    if (querySigef) {
      result = await safeQuery(`
        SELECT
          'SIGEF' as source,
          gid as id,
          parcela_co as numero,
          ST_AsGeoJSON(geom) as geojson,
          ROUND(CAST(ST_Area(ST_Transform(geom, 32721)) / 10000 AS numeric), 2) as area_hectares
        FROM ${sigefTable}
        WHERE geom && ST_GeomFromText($1) AND ST_Intersects(
          CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_GeomFromText($1)
        )
        ORDER BY ST_Area(geom) ASC
        LIMIT 1
      `, [point]);

      // Busca SNCI se SIGEF vazio (SIGEF/SNCI são agrupados na mesma camada)
      if (result.rows.length === 0) {
        result = await safeQuery(`
          SELECT
            'SNCI' as source,
            gid as id,
            num_proces as numero,
            ST_AsGeoJSON(geom) as geojson,
            ROUND(CAST(ST_Area(ST_Transform(geom, 32721)) / 10000 AS numeric), 2) as area_hectares
          FROM ${snciTable}
          WHERE geom && ST_GeomFromText($1) AND ST_Intersects(
            CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
            ST_GeomFromText($1)
          )
          ORDER BY ST_Area(geom) ASC
          LIMIT 1
        `, [point]);
      }
    }

    // Busca CAR apenas se a camada estiver ativa (e SIGEF/SNCI não encontraram nada)
    if (queryCar && result.rows.length === 0) {
      result = await safeQuery(`
        SELECT
          'CAR' as source,
          gid as id,
          cod_imovel as numero,
          ST_AsGeoJSON(geom) as geojson,
          ROUND(CAST(ST_Area(ST_Transform(geom, 32721)) / 10000 AS numeric), 2) as area_hectares
        FROM ${carTable}
        WHERE geom && ST_GeomFromText($1) AND ST_Intersects(
          CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_GeomFromText($1)
        )
          AND des_condic IS DISTINCT FROM 'Cancelado por decisao administrativa'
          AND ind_status IS DISTINCT FROM 'CA'
        ORDER BY ST_Area(geom) ASC
        LIMIT 1
      `, [point]);
    }

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'No parcel found at this location' });
    }

    const row = result.rows[0];
    res.json({
      source: row.source,
      id: row.id,
      numero: row.numero,
      area_hectares: row.area_hectares,
      geojson: JSON.parse(row.geojson),
    });
  } catch (error) {
    console.error('Error in /api/buscar-feicao:', error);
    res.status(500).json({ error: error.message });
  }
});

/**
 * POST /api/analises
 * Runs ALL analyses on a GeoJSON polygon
 */
app.post('/api/analises', async (req, res) => {
  try {
    // origem: 'sigef' | 'snci' | 'car' — indica a camada de origem da parcela selecionada
    const { geojson, origem } = req.body;

    if (!geojson) {
      return res.status(400).json({ error: 'geojson required in body' });
    }

    // PERF: cache hit (mesma parcela analisada recentemente)
    const _ck = _cacheKey({ geojson, origem });
    const _nocache = req.query.nocache === '1' || req.body?.nocache === true;
    if (!_nocache) {
      const _hit = _cacheGet(_ck);
      if (_hit) {
        console.log('[CACHE HIT /api/analises]', _ck.slice(0, 12));
        return res.json(_hit);
      }
    } else {
      console.log('[CACHE BYPASS] nocache=1');
    }

    const feature = parseGeoJSONFeature(geojson);
    const geojsonStr = JSON.stringify(feature);
    const origemParcel = (origem || '').toLowerCase(); // 'sigef' | 'snci' | 'car'

    // Get centroid and municipality info
    let municResult = await safeQuery(`
      SELECT
        m."NM_MUN" as municipio,
        m."SIGLA_UF" as uf,
        ST_AsGeoJSON(ST_Centroid(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))) as centroid,
        ROUND(CAST(ST_Area(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674), 32721)) / NULLIF(ST_Area(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}))::geography), 0) * 100 AS numeric), 2) as percentual_sobreposicao
      FROM municipios.municipios_2024 m
      WHERE m.geom && ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AND ST_Intersects(CASE WHEN ST_SRID(m.geom) != ${SRID} THEN ST_SetSRID(m.geom, ${SRID}) ELSE m.geom END, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))
      ORDER BY ST_Area(ST_Intersection(m.geom, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))) DESC
      LIMIT 1
    `, [geojsonStr]);

    // Determine UF from geometry if municipality query returned nothing
    let ufFallback = null;
    if (!municResult.rows[0]) {
      ufFallback = await getUFFromGeoJSON(geojsonStr);
      // If DB failed, try centroid bbox fallback
      if (!ufFallback) {
        try {
          const centroidRes = await safeQuery(
            `SELECT ST_X(ST_Centroid(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))) as lng,
                    ST_Y(ST_Centroid(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))) as lat`,
            [geojsonStr]
          );
          const c = centroidRes.rows[0];
          if (c) ufFallback = getUFFromPointFallback(c.lat, c.lng);
        } catch (_) {}
      }
    }

    const municipio = municResult.rows[0] || {
      municipio: 'Desconhecido',
      uf: (ufFallback || 'mt').toUpperCase(),
      centroid: null,
      area_hectares: 0,
    };

    // State-specific table names (lowercase UF)
    const uf = municipio.uf.toLowerCase();
    const sigefTable = `incra.sigef_${uf}`;
    const snciTable = `incra.snci_${uf}`;
    const anmProcessoTable = `anm.anm_mt`;
    const anmOcorrenciasTable = `anm.anm_mt`;
    const carAreaTable = `car.area_imovel_${uf}`;
    const carAppsTable = `car.apps_${uf}`;
    const carReservaTable = `car.reserva_legal_${uf}`;
    const carVegNativaTable = `car.vegetacao_nativa_${uf}`;
    const carAreaConsolidadaTable = `car.area_consolidada_${uf}`;

    const analyses = {};

    // 9.1 Fundiária (SIGEF + SNCI)
    analyses['9.1_fundiaria'] = {
      nome: 'Fundiária',
      sigef: [],
      snci: [],
    };

    // CTE reutilizável: gera 4 pontos geodistribuídos (grade 2×2 da bbox) dentro da parcela.
    // Usa ST_PointOnSurface como fallback para quadrantes fora do polígono (formas irregulares).
    // Gera pontos de amostra DENTRO de CADA parcela do MultiPolygon
    // (ST_Dump separa MultiPolygon em polígonos individuais).
    // Para cada parcela: 1 ponto on-surface + 4 cantos do bbox interno.
    // Assim, ao selecionar várias parcelas SIGEF, cada uma fornece seus próprios
    // pontos de amostra, garantindo que CARs sobre qualquer uma sejam identificados.
    const samplePtsCTE = `
      WITH parcel_geom AS (
        SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}) AS g
      ),
      parcel_parts AS (
        SELECT (ST_Dump(g)).geom AS p FROM parcel_geom
      ),
      sample_pts AS (
        SELECT DISTINCT pt FROM (
          -- 1 ponto on-surface por parcela
          SELECT ST_PointOnSurface(p) AS pt FROM parcel_parts
          UNION ALL
          -- 4 cantos do bbox de cada parcela (com fallback para on-surface se cair fora)
          SELECT CASE WHEN ST_Contains(pp.p, candidate) THEN candidate ELSE ST_PointOnSurface(pp.p) END AS pt
          FROM parcel_parts pp
          CROSS JOIN LATERAL (
            SELECT ST_SetSRID(ST_MakePoint(
              ST_XMin(pp.p) + (ST_XMax(pp.p) - ST_XMin(pp.p)) * pct_x,
              ST_YMin(pp.p) + (ST_YMax(pp.p) - ST_YMin(pp.p)) * pct_y
            ), ${SRID}) AS candidate
            FROM (VALUES (0.25, 0.25), (0.75, 0.25), (0.25, 0.75), (0.75, 0.75)) AS f(pct_x, pct_y)
          ) sub
        ) all_pts
      )
    `;

    // Se origem é CAR → usa 4 pontos para encontrar SIGEF/SNCI correspondentes
    // Se origem é SIGEF/SNCI/mixed → usa intersecção direta (parcela selecionada é a própria)
    const isCARSelected = origemParcel === 'car';

    if (isCARSelected) {
      // Parcela CAR selecionada → todas SIGEF com sobreposição real de área (SIGEF pode ser menor que CAR)
      let fundiariaResult = await safeQuery(`
        SELECT DISTINCT ON (t.gid) t.gid as id,
          t.parcela_co, t.nome_area, t.situacao_i,
          t.data_aprov::text as data_aprov,
          t.codigo_imo, t.registro_m,
          t.registro_d::text as registro_d,
          ROUND(CAST(ST_Area(ST_Transform(t.geom, 32721)) / 10000 AS numeric), 2) as area_hectares
        FROM ${sigefTable} t
        WHERE t.geom && ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}) AND ST_Intersects(
          CASE WHEN ST_SRID(t.geom) != ${SRID} THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )
        AND ST_Area(ST_Intersection(
          CASE WHEN ST_SRID(t.geom) != ${SRID} THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )) / NULLIF(ST_Area(CASE WHEN ST_SRID(t.geom) != ${SRID} THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END), 0) >= 0.3
        ORDER BY t.gid
      `, [geojsonStr]);
      analyses['9.1_fundiaria'].sigef = fundiariaResult.rows;

      // Parcela CAR selecionada → todas SNCI com sobreposição real de área
      let snciResult = await safeQuery(`
        SELECT DISTINCT ON (t.gid) t.gid as id,
          t.cod_imovel, t.nome_imove, t.num_certif,
          t.data_certi::text as data_certi,
          t.qtd_area_p,
          ROUND(CAST(ST_Area(ST_Transform(t.geom, 32721)) / 10000 AS numeric), 2) as area_hectares
        FROM ${snciTable} t
        WHERE t.geom && ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}) AND ST_Intersects(
          CASE WHEN ST_SRID(t.geom) != ${SRID} THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )
        AND ST_Area(ST_Intersection(
          CASE WHEN ST_SRID(t.geom) != ${SRID} THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )) / NULLIF(ST_Area(CASE WHEN ST_SRID(t.geom) != ${SRID} THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END), 0) >= 0.3
        ORDER BY t.gid
      `, [geojsonStr]);
      analyses['9.1_fundiaria'].snci = snciResult.rows;

    } else {
      // Parcela SIGEF selecionada → retorna a própria parcela (maior sobreposição = ela mesma)
      let fundiariaResult = await safeQuery(`
        SELECT t.gid as id,
          t.parcela_co, t.nome_area, t.situacao_i,
          t.data_aprov::text as data_aprov,
          t.codigo_imo, t.registro_m,
          t.registro_d::text as registro_d,
          ROUND(CAST(ST_Area(ST_Transform(t.geom, 32721)) / 10000 AS numeric), 2) as area_hectares
        FROM ${sigefTable} t
        WHERE t.geom && ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}) AND ST_Intersects(
          CASE WHEN ST_SRID(t.geom) != ${SRID} THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )
          AND ST_Area(ST_Intersection(
            CASE WHEN ST_SRID(t.geom) != ${SRID} THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
            ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
          )) / NULLIF(ST_Area(CASE WHEN ST_SRID(t.geom) != ${SRID} THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END), 0) >= 0.3
        ORDER BY ST_Area(ST_Transform(ST_Intersection(
          CASE WHEN ST_SRID(t.geom) != ${SRID} THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        ), 32721)) DESC
      `, [geojsonStr]);
      analyses['9.1_fundiaria'].sigef = fundiariaResult.rows;

      // Parcela SNCI selecionada → retorna a própria parcela (maior sobreposição = ela mesma)
      let snciResult = await safeQuery(`
        SELECT t.gid as id,
          t.cod_imovel, t.nome_imove, t.num_certif,
          t.data_certi::text as data_certi,
          t.qtd_area_p,
          ROUND(CAST(ST_Area(ST_Transform(t.geom, 32721)) / 10000 AS numeric), 2) as area_hectares
        FROM ${snciTable} t
        WHERE t.geom && ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}) AND ST_Intersects(
          CASE WHEN ST_SRID(t.geom) != ${SRID} THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )
          AND ST_Area(ST_Intersection(
            CASE WHEN ST_SRID(t.geom) != ${SRID} THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
            ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
          )) / NULLIF(ST_Area(CASE WHEN ST_SRID(t.geom) != ${SRID} THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END), 0) >= 0.3
        ORDER BY ST_Area(ST_Transform(ST_Intersection(
          CASE WHEN ST_SRID(t.geom) != ${SRID} THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        ), 32721)) DESC
      `, [geojsonStr]);
      analyses['9.1_fundiaria'].snci = snciResult.rows;
    }

    // 9.2 Registral (Serventias by municipality)
    analyses['9.2_registral'] = {
      nome: 'Registral',
      serventias: [],
      message: 'Dados de serventia disponíveis por consulta específica',
    };

    if (municipio.municipio !== 'Desconhecido') {
      let serventiaResult = await safeQuery(`
        SELECT id, cartorio, codigo_cns as cns, comarca
        FROM serventias.serventias_brasil
        WHERE comarca ILIKE $1 AND UPPER(uf) = UPPER($2)
        LIMIT 10
      `, [municipio.municipio, municipio.uf]);
      analyses['9.2_registral'].serventias = serventiaResult.rows;
    }

    // ── PARALELIZADO: 5 queries grandes (solo + soloGeom + bioma + geologia + geoloGeom) ──
    analyses['9.3_solo'] = { nome: 'Solo', data: [] };
    analyses['9.4_bioma'] = { nome: 'Bioma', data: [] };
    analyses['9.5_geologia'] = { nome: 'Geologia', data: [] };

    console.time('[par] solo+bioma+geol');
    const [soloResult, soloGeomRes, biomaResult, geologiaResult, geoloGeomRes] = await Promise.all([
      safeQuery(`
        SELECT
          nom_unidad as codigo,
          legenda, legenda as nome,
          ordem, subordem, textura, relevo, componente,
          ROUND(CAST(SUM(ST_Area(ST_Intersection(
            CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
            ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
          ))) / ST_Area(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)) * 100 AS numeric), 2) as percentual
        FROM solo.pedo_area
        WHERE geom && ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AND ST_Intersects(
          CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
        )
        GROUP BY nom_unidad, legenda, ordem, subordem, textura, relevo, componente
        ORDER BY percentual DESC
      `, [geojsonStr]),
      pool.query(`
        SELECT
          legenda,
          legenda as nome,
          ST_AsGeoJSON(ST_Union(ST_Intersection(
            CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
            ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
          ))) as geom_json
        FROM solo.pedo_area
        WHERE ST_Intersects(
          CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )
        GROUP BY legenda
      `, [geojsonStr]).catch(e => { console.warn('[analises] solo geoms:', e.message); return { rows: [] }; }),
      safeQuery(`
        WITH bioma_union AS (
          SELECT "Bioma" as nome,
                 ST_Union(CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom, ${SRID}) ELSE geom END) as geom
          FROM bioma.bioma_250
          WHERE geom && ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AND ST_Intersects(
            CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
            ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
          )
          GROUP BY "Bioma"
        )
        SELECT nome,
          ROUND(CAST(
            ST_Area(ST_Intersection(geom, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))) /
            ST_Area(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)) * 100
          AS numeric), 2) as percentual
        FROM bioma_union
        ORDER BY percentual DESC
      `, [geojsonStr]),
      safeQuery(`
        SELECT
          "NOME"               as nome,
          "SIGLA"              as sigla,
          "LITOTIPOS"          as litotipos,
          "ERA_MIN"            as era_min,
          "ERA_MAX"            as era_ma,
          "EON_MIN"            as eon_min,
          "SISTEMA_MIN"        as sistema_min,
          "AMBIENTE_TECTONICO" as ambiente_tectonico,
          "HIERARQUIA"         as hierarquia,
          "LEGENDA"            as legenda,
          ROUND(CAST(
            ST_Area(ST_Intersection(
              CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
              ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
            )) /
            ST_Area(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)) * 100
          AS numeric), 2) as percentual
        FROM geologia_litologia.litoestratigafia_br
        WHERE geom && ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AND ST_Intersects(
          CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
        )
        ORDER BY percentual DESC
      `, [geojsonStr]),
      pool.query(`
        SELECT "NOME" as nome,
          ST_AsGeoJSON(ST_SimplifyPreserveTopology(ST_Union(ST_Intersection(
            CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
            ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
          )), 0.0001)) as geom_json
        FROM geologia_litologia.litoestratigafia_br
        WHERE ST_Intersects(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
        )
        GROUP BY "NOME"
      `, [geojsonStr]).catch(e => { console.warn('[analises] geologia geoms:', e.message); return { rows: [] }; }),
    ]);
    console.timeEnd('[par] solo+bioma+geol');

    // Mescla solo + soloGeom
    {
      const soloGeomMap = {};
      soloGeomRes.rows.forEach(row => { if (row.legenda) soloGeomMap[row.legenda] = row.geom_json; });
      analyses['9.3_solo'].data = soloResult.rows.map(s => ({
        ...s,
        geom_json: soloGeomMap[s.nome || s.legenda] || null,
      }));
    }

    analyses['9.4_bioma'].data = biomaResult.rows;

    // Mescla geologia + geoloGeom
    {
      const geoloGeomMap = {};
      geoloGeomRes.rows.forEach(row => { if (row.nome) geoloGeomMap[row.nome] = row.geom_json; });
      analyses['9.5_geologia'].data = geologiaResult.rows.map(g => ({
        ...g,
        geom_json: geoloGeomMap[g.nome] || null,
      }));
    }

    // 9.5b — pontos/ocorrências/estruturas são preenchidos pelo bloco 9.14 mais abaixo
    analyses['9.5_geologia'].pontos      = [];
    analyses['9.5_geologia'].ocorrencias = [];
    analyses['9.5_geologia'].dobras      = [];
    analyses['9.5_geologia'].falhas      = [];
    analyses['9.5_geologia'].fraturas    = [];

    // ── PARALELIZADO: ANM + Embargos (independentes entre si) ──
    analyses['9.6_mineracao'] = { nome: 'Mineração', processes: [], occurrences: [] };
    analyses['9.7_embargos'] = { nome: 'Embargos', data: [] };

    console.time('[par] anm+embargos');
    const [anmResult, embargosResult] = await Promise.all([
      safeQuery(`
        SELECT DISTINCT ON ("PROCESSO")
          "PROCESSO"  as numero_processo,
          "FASE"      as fase,
          "NOME"      as titular,
          "SUBS"      as substancia,
          ST_AsGeoJSON(ST_SimplifyPreserveTopology(ST_Intersection(
            CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
            ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
          ), 0.001)) as geom_json
        FROM ${anmProcessoTable}
        WHERE geom && ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AND ST_Intersects(
          CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
        )
        ORDER BY "PROCESSO"
      `, [geojsonStr]),
      safeQuery(`
        SELECT id, num_auto_i as auto_numero, nome_embar, cpf_cnpj_e, nome_imove, dat_embarg, qtd_area_e, des_infrac, des_tad, sit_desmat as situacao
        FROM embargos.embargos_ibama
        WHERE geom && ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AND ST_Intersects(
          CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
        )
      `, [geojsonStr]),
    ]);
    console.timeEnd('[par] anm+embargos');
    analyses['9.6_mineracao'].processes = anmResult.rows;
    analyses['9.6_mineracao'].occurrences = [];
    analyses['9.7_embargos'].data = embargosResult.rows;

    // 9.8 Terras Indígenas
    analyses['9.8_terras_indigenas'] = { nome: 'Terras Indígenas', data: [] };
    let tisResult = await safeQuery(`
      SELECT id, terrai_nome as nome, superficie_perimetro_ha as superficie,
        ROUND(CAST(ST_Area(ST_Intersection(
                    ST_MakeValid(ST_SetSRID(geom::geometry, ${SRID})),
                    ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}))
                )::geography) / NULLIF(ST_Area(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}))::geography), 0) * 100 AS numeric), 2) as percentual_sobreposicao,
        ST_AsGeoJSON(ST_SimplifyPreserveTopology(ST_Intersection(
          ST_MakeValid(ST_SetSRID(geom::geometry, ${SRID})),
          ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}))
        ), 0.001)) as geom_json
      FROM terra_indigena.poligonais_portarias
            WHERE ST_Intersects(
                ST_MakeValid(ST_SetSRID(geom::geometry, ${SRID})),
                ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}))
      )
    `, [geojsonStr]);

    // Fallback: FUNAI WFS quando DB nao retorna sobreposicao com TI
    if (!tisResult.rows.length) {
      try {
        const _feat = JSON.parse(geojsonStr);
        const _g = _feat.geometry;
        // Calcular BBOX para query WFS mais confiavel (evita URL longa com WKT)
        const _allCoords = _g.type === 'MultiPolygon' ? _g.coordinates.flat(2) : _g.coordinates.flat(1);
        const _lons = _allCoords.map(c => c[0]);
        const _lats =_allCoords.map(c => c[1]);
        const _bboxStr = Math.min(..._lons)+','+Math.min(..._lats)+','+Math.max(..._lons)+','+Math.max(..._lats);
        const _ctrl = new AbortController();
        const _tmr = setTimeout(() => _ctrl.abort(), 15000);
        const _wfsUrl = 'https://geoserver.funai.gov.br/geoserver/Funai/ows?service=WFS&version=2.0.0&request=GetFeature&typeName=Funai:tis_poligonais&outputFormat=application/json&BBOX='+_bboxStr+',EPSG:4674&count=50';
        const _wfsResp = await fetch(_wfsUrl, { signal: _ctrl.signal });
        clearTimeout(_tmr);
        if (_wfsResp.ok) {
          const _wfsData = await _wfsResp.json();
          const _wfsRows = (_wfsData.features || []).map(f => ({
            id: f.id, nome: f.properties.terrai_nome || f.properties.terrai_nom,
            etnia: f.properties.etnia_nome || null,
            superficie: f.properties.superficie || null,
            fase_ti: f.properties.fase_ti || null,
            geom_json: f.geometry || null, // fix: incluir geometria para o mapa do relatório PDF
          }));
          // PERF: paraleliza queries de % de sobreposição (antes era loop serial)
          const _pctQueries = _wfsRows.map((_row, _i) => safeQuery(`SELECT ROUND(CAST(ST_Area(ST_Intersection(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb),${SRID})),ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON($2::jsonb->'geometry'),${SRID})))::geography)/NULLIF(ST_Area(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON($2::jsonb->'geometry'),${SRID}))::geography),0)*100 AS numeric),2) as pct`, [JSON.stringify(_wfsData.features[_i].geometry), geojsonStr]).catch(() => ({ rows: [{ pct: null }] })));
          const _pctResults = await Promise.all(_pctQueries);
          _wfsRows.forEach((_row, _i) => { _row.percentual_sobreposicao = _pctResults[_i].rows[0]?.pct ?? null; }); if (_wfsRows.length) { tisResult = { rows: _wfsRows }; }
        }
      } catch (_e) { console.warn('[TI WFS]', _e.message); }
    }
    analyses['9.8_terras_indigenas'].data = tisResult.rows;

    // ── PARALELIZADO: UCs + muniProx + 5 bacias (níveis 2-6) — todas independentes ──
    analyses['9.9_ucs'] = { nome: 'Unidades de Conservação', data: [] };
    analyses['localizacao_municipios_proximos'] = [];
    analyses['9.10_hidrografia'] = { nome: 'Hidrografia', bacias: [], cursos_agua_count: 0, rica_em_agua: false, padrao_drenagem: null, nomes_rios: [], comprimento_influencia_km: 0, rios_geom: [] };

    console.time('[par] ucs+muniprox+bacias');
    const baciasPromises = [2, 3, 4, 5, 6].map(nivel =>
      pool.query(`
        SELECT nome_bacia, curso_prin, princ_aflu, sub_bacias, suprabacia, ${nivel} as nivel
        FROM bacias_hidrograficas.bacias_nivel_${nivel}
        WHERE geom && ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AND ST_Intersects(
          CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
        )
        LIMIT 1
      `, [geojsonStr]).catch(e => { console.error(`[bacia] nivel=${nivel} ERROR: ${e.message}`); return { rows: [] }; })
    );

    const [ucsResult, muniProxResult, ...baciasResults] = await Promise.all([
      safeQuery(`
        SELECT id,
          "NOME_UC1" as nome,
          "CATEGORI3" as categoria,
          "GRUPO4" as grupo,
          "ESFERA5" as esfera,
          "ANO_CRIA6" as ano_criacao,
          "ATO_LEGA9" as ato_legal,
          ROUND(CAST(ST_Area(ST_Intersection(
            CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
            ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
          )) / ST_Area(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)) * 100 AS numeric), 2) as sobreposicao_percentual,
          ST_AsGeoJSON(ST_SimplifyPreserveTopology(ST_Intersection(
            CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
            ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
          ), 0.001)) as geom_json
        FROM unidade_conservacao.unidade_conserv
        WHERE geom && ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AND ST_Intersects(
          CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
        )
      `, [geojsonStr]),
      safeQuery(`
        WITH parc AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AS g),
             centro AS (SELECT ST_Centroid(g) AS c FROM parc),
             buffer AS (SELECT ST_Expand(c, 60.0/111.0) AS bg FROM centro)
        SELECT
          "NM_MUN" AS nome,
          "CD_MUN" AS codigo,
          ST_AsGeoJSON(ST_SimplifyPreserveTopology(geom, 0.005)) AS geom_json
        FROM municipios.municipios_2024 m, buffer
        WHERE m.geom && buffer.bg AND ST_Intersects(m.geom, buffer.bg)
        LIMIT 30
      `, [geojsonStr]).catch(e => { console.warn('[LOCALIZACAO] failed:', e.message); return { rows: [] }; }),
      ...baciasPromises,
    ]);
    console.timeEnd('[par] ucs+muniprox+bacias');

    analyses['9.9_ucs'].data = ucsResult.rows;
    analyses['localizacao_municipios_proximos'] = muniProxResult?.rows || [];
    const baciasRows = [];
    baciasResults.forEach(r => { if (r?.rows?.length > 0) baciasRows.push(...r.rows); });
    analyses['9.10_hidrografia'].bacias = baciasRows;

    // ── PARALELIZADO: 8 queries de hidrografia (todas independentes — mesmo geojsonStr) ──
    const RAIO_DRENAGEM_KM = 25;
    console.time('[par] hidrografia');
    const [cursosResult, riosGeomRes, padraoAgg, terrainAgg, radialAgg, compParcelRes, nomesRes, ordensRes] = await Promise.all([
      // Q-cursos: count de cursos d'água
      safeQuery(`
        WITH parc AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AS p)
        SELECT COUNT(*) as total
        FROM hidrografia.geoft_bho_2017_curso_dagua c, parc
        WHERE c.geom && parc.p AND ST_Intersects(c.geom, parc.p)
      `, [geojsonStr]),
      // Q-riosGeom: geometrias simplificadas
      safeQuery(`
        WITH parc AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AS p)
        SELECT ST_AsGeoJSON(ST_SimplifyPreserveTopology(c.geom, 0.00005)) AS geom,
               COALESCE(c.nuordemcda, 0) AS ordem
        FROM hidrografia.geoft_bho_2017_curso_dagua c, parc
        WHERE c.geom && parc.p AND ST_Intersects(c.geom, parc.p)
        ORDER BY COALESCE(c.nuordemcda, 0) DESC NULLS LAST
        LIMIT 2000
      `, [geojsonStr]),
      // Q-padrao: estatísticas de azimute/convergência/bimodalidade no raio de 25km
      safeQuery(`
        WITH parcel AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AS g),
        centroid_pt AS (SELECT ST_Centroid(g) AS c FROM parcel),
        buffer25 AS (
          SELECT ST_Expand(c, ${RAIO_DRENAGEM_KM} / 111.0) AS bg FROM centroid_pt
        ),
        raw_segs AS (
          SELECT (ST_Dump(c.geom)).geom AS seg
          FROM hidrografia.geoft_bho_2017_curso_dagua c, buffer25 bb
          WHERE c.geom && bb.bg AND ST_Intersects(c.geom, bb.bg)
        ),
        courses AS (
          SELECT
            ST_Azimuth(ST_StartPoint(seg), ST_EndPoint(seg))                               AS az_full,
            MOD(degrees(ST_Azimuth(ST_StartPoint(seg), ST_EndPoint(seg)))::numeric, 180)   AS az_norm,
            ST_Azimuth(ST_Centroid(seg), (SELECT cp.c FROM centroid_pt cp))                AS bear_ctr,
            ST_Length(seg::geography) / 1000.0                                             AS len_km
          FROM raw_segs WHERE ST_NPoints(seg) >= 2
        ),
        courses_valid AS (SELECT * FROM courses WHERE az_full IS NOT NULL AND az_norm IS NOT NULL),
        global_stats AS (
          SELECT
            COUNT(*) AS total,
            ROUND(COALESCE(STDDEV(az_norm), 0)::numeric, 1) AS std_az,
            ROUND(COALESCE(SQRT(POWER(AVG(cos(2.0 * az_norm * pi() / 180.0)), 2) + POWER(AVG(sin(2.0 * az_norm * pi() / 180.0)), 2)), 0)::numeric, 3) AS rayleigh_r,
            ROUND(COALESCE(AVG(cos(az_full - bear_ctr)), 0)::numeric, 3) AS mean_conv,
            ROUND(COALESCE(AVG(ABS(sin(az_full - bear_ctr))), 0)::numeric, 3) AS mean_tang,
            COALESCE(SUM(len_km), 0) AS total_len_km
          FROM courses_valid
        ),
        az_histogram AS (SELECT (az_norm::int / 10) * 10 AS bin, COUNT(*) AS cnt FROM courses_valid GROUP BY 1),
        ranked_bins AS (SELECT bin, cnt, ROW_NUMBER() OVER (ORDER BY cnt DESC) AS rn FROM az_histogram),
        top2 AS (
          SELECT
            COALESCE(MAX(CASE WHEN rn = 1 THEN bin END), 0) AS bin1,
            COALESCE(MAX(CASE WHEN rn = 2 THEN bin END), 0) AS bin2,
            COALESCE(MAX(CASE WHEN rn = 1 THEN cnt END), 0) AS cnt1,
            COALESCE(MAX(CASE WHEN rn = 2 THEN cnt END), 0) AS cnt2
          FROM ranked_bins
        )
        SELECT g.total, g.std_az, g.rayleigh_r, g.mean_conv, g.mean_tang, g.total_len_km,
          COALESCE(ABS(t.bin1 - t.bin2), 0) AS bin_sep,
          COALESCE((t.cnt1 + t.cnt2)::float / NULLIF(g.total, 0), 0) AS bimodal_frac
        FROM global_stats g, top2 t
      `, [geojsonStr]),
      // Q-terrain: relevo regional
      safeQuery(`
        WITH parcel AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AS g),
        centroid_pt AS (SELECT ST_Centroid(g) AS c FROM parcel),
        buffer25 AS (SELECT ST_Buffer(c::geography, ${RAIO_DRENAGEM_KM * 1000})::geometry AS bg FROM centroid_pt),
        clipped AS (SELECT ST_Clip(r.rast, b.bg, TRUE) AS rc FROM altitude_br.altitude_raster r, buffer25 b WHERE ST_Intersects(r.rast, b.bg)),
        tstats AS (SELECT (ST_SummaryStats(rc, 1, TRUE)).* FROM clipped WHERE rc IS NOT NULL)
        SELECT COALESCE(MIN(min), 0) AS min_alt, COALESCE(MAX(max), 0) AS max_alt, COALESCE(MAX(max) - MIN(min), 0) AS relief_m FROM tstats
      `, [geojsonStr]),
      // Q-radial: gradiente radial
      safeQuery(`
        WITH parcel AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AS g),
        centroid_pt AS (SELECT ST_Centroid(g) AS c FROM parcel),
        ring_pts AS (
          SELECT d.b AS bearing, ST_Project(cp.c::geography, 15000, radians(d.b))::geometry AS pt
          FROM centroid_pt cp CROSS JOIN (VALUES (0),(45),(90),(135),(180),(225),(270),(315)) AS d(b)
        ),
        ring_alts AS (
          SELECT ST_Value(r.rast, 1, rp.pt, TRUE) AS alt
          FROM ring_pts rp JOIN altitude_br.altitude_raster r ON ST_Intersects(r.rast, rp.pt)
          WHERE ST_Value(r.rast, 1, rp.pt, TRUE) IS NOT NULL
        ),
        c_alt AS (
          SELECT ST_Value(r.rast, 1, cp.c, TRUE) AS alt
          FROM centroid_pt cp JOIN altitude_br.altitude_raster r ON ST_Intersects(r.rast, cp.c)
          WHERE ST_Value(r.rast, 1, cp.c, TRUE) IS NOT NULL LIMIT 1
        )
        SELECT COALESCE((SELECT alt FROM c_alt), 0) AS c_alt, COALESCE(AVG(alt), 0) AS ring_alt,
          COALESCE((SELECT alt FROM c_alt) - AVG(alt), 0) AS radial_grad
        FROM ring_alts
      `, [geojsonStr]),
      // Q-comprimento: rios dentro da parcela com buffer 200m
      safeQuery(`
        WITH parc AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AS p),
             parc_buf AS (SELECT ST_SetSRID(ST_Buffer(p::geography, 200)::geometry, 4674) AS pb FROM parc)
        SELECT COALESCE(SUM(ST_Length(ST_Intersection(c.geom, parc_buf.pb)::geography)), 0) AS len_m
        FROM hidrografia.geoft_bho_2017_curso_dagua c, parc_buf
        WHERE c.geom && parc_buf.pb AND ST_Intersects(c.geom, parc_buf.pb)
      `, [geojsonStr]),
      // Q-nomes: nomes de rios (200m)
      safeQuery(`
        WITH parc AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AS p)
        SELECT DISTINCT "NORIOCOMP"::text AS nome
        FROM hidrografia.rio_nomes rn, parc
        WHERE rn.geom && ST_Expand(parc.p, 0.0025)
          AND ST_DWithin(rn.geom::geography, parc.p::geography, 200)
          AND "NORIOCOMP" IS NOT NULL AND TRIM("NORIOCOMP"::text) <> ''
        LIMIT 50
      `, [geojsonStr]),
      // Q-ordens: distribuição de ordens dos cursos
      safeQuery(`
        WITH parc AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AS p)
        SELECT nuordemcda AS ordem, COUNT(*)::int AS cnt
        FROM hidrografia.geoft_bho_2017_curso_dagua c, parc
        WHERE c.geom && parc.p AND ST_Intersects(c.geom, parc.p)
        GROUP BY nuordemcda ORDER BY nuordemcda DESC
      `, [geojsonStr]),
    ]);
    console.timeEnd('[par] hidrografia');

    analyses['9.10_hidrografia'].cursos_agua_count = parseInt(cursosResult.rows[0]?.total || 0);
    analyses['9.10_hidrografia'].rica_em_agua = analyses['9.10_hidrografia'].cursos_agua_count > 5;
    analyses['9.10_hidrografia'].rios_geom = (riosGeomRes.rows || []).map(r => { try { return JSON.parse(r.geom); } catch { return null; } }).filter(Boolean);

    // (queries movidas para Promise.all acima — bloco antigo removido)
    /* OLD_PADRAO_AGG_REMOVED — começo do bloco a deletar
    const _padraoAgg_unused = await safeQuery(`
      WITH parcel AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AS g),
      centroid_pt AS (SELECT ST_Centroid(g) AS c FROM parcel),
      buffer25 AS (
        SELECT ST_Expand(c, ${RAIO_DRENAGEM_KM} / 111.0) AS bg FROM centroid_pt
      ),
      raw_segs AS (
        -- Achata MULTILINESTRING → LineStrings para que ST_StartPoint/ST_EndPoint funcionem
        SELECT (ST_Dump(c.geom)).geom AS seg
        FROM hidrografia.geoft_bho_2017_curso_dagua c, buffer25 bb
        WHERE c.geom && bb.bg
          AND ST_Intersects(c.geom, bb.bg)
      ),
      courses AS (
        SELECT
          ST_Azimuth(ST_StartPoint(seg), ST_EndPoint(seg))                               AS az_full,
          MOD(degrees(ST_Azimuth(ST_StartPoint(seg), ST_EndPoint(seg)))::numeric, 180)   AS az_norm,
          ST_Azimuth(ST_Centroid(seg), (SELECT cp.c FROM centroid_pt cp))                AS bear_ctr,
          ST_Length(seg::geography) / 1000.0                                             AS len_km
        FROM raw_segs
        WHERE ST_NPoints(seg) >= 2
      ),
      courses_valid AS (
        -- Remove segmentos degenerados (ponto inicial = ponto final → azimute NULL)
        SELECT * FROM courses WHERE az_full IS NOT NULL AND az_norm IS NOT NULL
      ),
      global_stats AS (
        SELECT
          COUNT(*)                                                                    AS total,
          ROUND(COALESCE(STDDEV(az_norm), 0)::numeric, 1)                           AS std_az,
          -- Rayleigh R para estatistica circular (direcoes em [0,180°] → dobrar angulo)
          ROUND(COALESCE(SQRT(
            POWER(AVG(cos(2.0 * az_norm * pi() / 180.0)), 2) +
            POWER(AVG(sin(2.0 * az_norm * pi() / 180.0)), 2)
          ), 0)::numeric, 3)                                                          AS rayleigh_r,
          -- Indice de convergencia: +1 = centrípeta, -1 = centrífuga, 0 = tangencial
          ROUND(COALESCE(AVG(cos(az_full - bear_ctr)), 0)::numeric, 3)             AS mean_conv,
          -- Tangencialidade: alto = fluxo ao redor do centroide (anelar)
          ROUND(COALESCE(AVG(ABS(sin(az_full - bear_ctr))), 0)::numeric, 3)        AS mean_tang,
          COALESCE(SUM(len_km), 0)                                                  AS total_len_km
        FROM courses_valid
      ),
      az_histogram AS (
        SELECT (az_norm::int / 10) * 10 AS bin, COUNT(*) AS cnt
        FROM courses_valid
        GROUP BY 1
      ),
      ranked_bins AS (
        SELECT bin, cnt, ROW_NUMBER() OVER (ORDER BY cnt DESC) AS rn FROM az_histogram
      ),
      top2 AS (
        -- MAX(CASE WHEN) garante sempre 1 linha mesmo se ranked_bins estiver vazio
        SELECT
          COALESCE(MAX(CASE WHEN rn = 1 THEN bin END), 0) AS bin1,
          COALESCE(MAX(CASE WHEN rn = 2 THEN bin END), 0) AS bin2,
          COALESCE(MAX(CASE WHEN rn = 1 THEN cnt END), 0) AS cnt1,
          COALESCE(MAX(CASE WHEN rn = 2 THEN cnt END), 0) AS cnt2
        FROM ranked_bins
      )
      SELECT
        g.total, g.std_az, g.rayleigh_r, g.mean_conv, g.mean_tang, g.total_len_km,
        COALESCE(ABS(t.bin1 - t.bin2), 0)                                          AS bin_sep,
        COALESCE((t.cnt1 + t.cnt2)::float / NULLIF(g.total, 0), 0)                AS bimodal_frac
      FROM global_stats g, top2 t
    `, [geojsonStr]);

    // Q2: Relevo regional (altitude_br.altitude_raster) no raio de 25km — usa ST_SummaryStats por tile
    const terrainAgg = await safeQuery(`
      WITH parcel AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AS g),
      centroid_pt AS (SELECT ST_Centroid(g) AS c FROM parcel),
      buffer25 AS (
        SELECT ST_Buffer(c::geography, ${RAIO_DRENAGEM_KM * 1000})::geometry AS bg FROM centroid_pt
      ),
      clipped AS (
        SELECT ST_Clip(r.rast, b.bg, TRUE) AS rc
        FROM altitude_br.altitude_raster r, buffer25 b
        WHERE ST_Intersects(r.rast, b.bg)
      ),
      tstats AS (
        SELECT (ST_SummaryStats(rc, 1, TRUE)).* FROM clipped WHERE rc IS NOT NULL
      )
      SELECT
        COALESCE(MIN(min), 0)                AS min_alt,
        COALESCE(MAX(max), 0)                AS max_alt,
        COALESCE(MAX(max) - MIN(min), 0)     AS relief_m
      FROM tstats
    `, [geojsonStr]);

    // Q3: Gradiente radial — altitude do centroide vs media de 8 pontos cardinais a 15km
    const radialAgg = await safeQuery(`
      WITH parcel AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AS g),
      centroid_pt AS (SELECT ST_Centroid(g) AS c FROM parcel),
      ring_pts AS (
        SELECT d.b AS bearing,
               ST_Project(cp.c::geography, 15000, radians(d.b))::geometry AS pt
        FROM centroid_pt cp
        CROSS JOIN (VALUES (0),(45),(90),(135),(180),(225),(270),(315)) AS d(b)
      ),
      ring_alts AS (
        SELECT ST_Value(r.rast, 1, rp.pt, TRUE) AS alt
        FROM ring_pts rp
        JOIN altitude_br.altitude_raster r ON ST_Intersects(r.rast, rp.pt)
        WHERE ST_Value(r.rast, 1, rp.pt, TRUE) IS NOT NULL
      ),
      c_alt AS (
        SELECT ST_Value(r.rast, 1, cp.c, TRUE) AS alt
        FROM centroid_pt cp
        JOIN altitude_br.altitude_raster r ON ST_Intersects(r.rast, cp.c)
        WHERE ST_Value(r.rast, 1, cp.c, TRUE) IS NOT NULL
        LIMIT 1
      )
      SELECT
        COALESCE((SELECT alt FROM c_alt), 0)                        AS c_alt,
        COALESCE(AVG(alt), 0)                                       AS ring_alt,
        COALESCE((SELECT alt FROM c_alt) - AVG(alt), 0)            AS radial_grad
      FROM ring_alts
    `, [geojsonStr]);

    const compParcelRes = await safeQuery(`
      WITH parc AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AS p),
           parc_buf AS (
             -- Buffer de 200m para fora da parcela (geodesic)
             SELECT ST_SetSRID(ST_Buffer(p::geography, 200)::geometry, 4674) AS pb FROM parc
           )
      SELECT COALESCE(SUM(ST_Length(ST_Intersection(c.geom, parc_buf.pb)::geography)), 0) AS len_m
      FROM hidrografia.geoft_bho_2017_curso_dagua c, parc_buf
      WHERE c.geom && parc_buf.pb AND ST_Intersects(c.geom, parc_buf.pb)
    `, [geojsonStr]);
    // Nomes de rios: hidrografia.rio_nomes (NORIOCOMP) — ST_DWithin 100m
    const nomesRes = await safeQuery(`
      WITH parc AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AS p)
      SELECT DISTINCT "NORIOCOMP"::text AS nome
      FROM hidrografia.rio_nomes rn, parc
      WHERE rn.geom && ST_Expand(parc.p, 0.0025)
        AND ST_DWithin(rn.geom::geography, parc.p::geography, 200)
        AND "NORIOCOMP" IS NOT NULL AND TRIM("NORIOCOMP"::text) <> ''
      LIMIT 50
    `, [geojsonStr]);
    const _ordensRes_unused_x = await safeQuery(`
      WITH parc AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AS p)
      SELECT nuordemcda AS ordem, COUNT(*)::int AS cnt
      FROM hidrografia.geoft_bho_2017_curso_dagua c, parc
      WHERE c.geom && parc.p AND ST_Intersects(c.geom, parc.p)
      GROUP BY nuordemcda ORDER BY nuordemcda DESC
    `, [geojsonStr]).catch(()=>({rows:[]}));
    OLD_PADRAO_AGG_REMOVED — fim do bloco a deletar */

    // === METRICAS EXTRAIDAS ===
    const totalRaio    = parseInt(padraoAgg.rows[0]?.total         || 0);
    const stdAz        = parseFloat(padraoAgg.rows[0]?.std_az      || 0);
    const rayleighR    = parseFloat(padraoAgg.rows[0]?.rayleigh_r  || 0); // 0=aleatório, 1=paralelo
    const meanConv     = parseFloat(padraoAgg.rows[0]?.mean_conv   || 0);
    const meanTang     = parseFloat(padraoAgg.rows[0]?.mean_tang   || 0);
    const totalLenKm   = parseFloat(padraoAgg.rows[0]?.total_len_km || 0);
    const binSep       = parseFloat(padraoAgg.rows[0]?.bin_sep      || 0);
    const bimodalFrac  = parseFloat(padraoAgg.rows[0]?.bimodal_frac || 0);
    const reliefM      = parseFloat(terrainAgg.rows[0]?.relief_m   || 0);
    const radialGrad   = parseFloat(radialAgg.rows[0]?.radial_grad  || 0);
    const bufAreaKm2   = Math.PI * RAIO_DRENAGEM_KM * RAIO_DRENAGEM_KM;  // ~1963 km²
    const streamDens   = bufAreaKm2 > 0 ? totalLenKm / bufAreaKm2 : 0;
    const lenKm        = parseFloat(((compParcelRes.rows[0]?.len_m || 0) / 1000).toFixed(2));
    // spread circular: 0 = todos paralelos, 1 = aleatório — mais robusto que stdAz puro
    const circSpread   = 1 - rayleighR;

    // === CLASSIFICACAO TECNICA DO PADRAO DE DRENAGEM ===
    // Usa circSpread (Rayleigh 1-R) como medida de dispersão direcional circular correta.
    // Escala: circSpread~0 = canais paralelos; circSpread~1 = direções aleatórias (dendrítico).
    let padrao = 'Indefinido', descricao = 'Dados insuficientes para classificação do padrão de drenagem.';

    if (totalRaio >= 5) {
      // Flags de classificacao
      // Bimodal perpendicular: dois grupos separados ~90° (treliça/retangular)
      const isBimodalPerp    = bimodalFrac >= 0.35 && binSep >= 55 && binSep <= 125;
      // Bimodal obliquo: grupos em ângulo ~30-60° (angular)
      const isBimodalOblique = bimodalFrac >= 0.38 && binSep >= 25 && binSep < 55;
      // Radial: convergência/divergência em relação ao centróide
      const isRadCentripeta  = meanConv > 0.35 || (radialGrad < -50 && meanConv > 0.15);
      const isRadCentrifuga  = meanConv < -0.35 || (radialGrad > 50 && meanConv < -0.15);
      // Anelar: tangencial ao centróide com baixa convergência
      const isAnelar         = meanTang > 0.72 && circSpread > 0.30 && Math.abs(meanConv) < 0.30;
      // Desorganizada: planície rasa com baixíssima densidade de drenagem
      const isDesorganizada  = reliefM < 25 && streamDens < 0.04;
      // Paralela: Rayleigh R alto (canais muito concentrados em 1 direção)
      const isParalela       = rayleighR > 0.75 && !isBimodalPerp && !isBimodalOblique;
      // Subparalela: R moderadamente alto
      const isSubparalela    = rayleighR > 0.50 && !isBimodalPerp && !isBimodalOblique;

      if (isDesorganizada) {
        padrao    = 'Desorganizada';
        descricao = "Relevo plano com baixa densidade de drenagem. Canais sem organização definida, típico de planícies mal drenadas, áreas de sedimentação recente, várzeas ou zonas de alagamento periódico.";
      } else if (isRadCentripeta) {
        padrao    = 'Radial Centrípeta';
        descricao = "Cursos d'\u00e1gua convergem para uma área central deprimida. Indica bacia fechada, depressão, área endorreica, pantanal ou planície interior que recebe a drenagem regional.";
      } else if (isRadCentrifuga) {
        padrao    = 'Radial Centrífuga';
        descricao = "Cursos d'\u00e1gua divergem de uma elevação central. Indica domo, serra isolada, chapada elevada ou maciço residual de onde a drenagem se irradia para todos os lados.";
      } else if (isAnelar) {
        padrao    = 'Anelar';
        descricao = "Cursos d'\u00e1gua formam padrão circular ou semicircular contornando estruturas geológicas em arco. Típico de domos erodidos, intrusões geológicas circulares ou crateras antigas com controle estrutural anelar.";
      } else if (isBimodalPerp && rayleighR < 0.55) {
        padrao    = 'Retangular';
        descricao = "Canais com mudanças bruscas de direção em ângulos próximos de 90°, controlados por dois sistemas de falhas/fraturas perpendiculares entre si. Indica controle tectônico com lineamentos estruturais bem definidos.";
      } else if (isBimodalPerp) {
        padrao    = 'Treliça';
        descricao = "Rio principal numa direção dominante com afluentes entrando em ângulo reto. Típico de camadas alternadas de rochas resistentes e frágeis (arenitos, folhelhos, quartzitos), dobramentos e relevo de cristas e vales paralelos com forte controle estrutural.";
      } else if (isBimodalOblique) {
        padrao    = 'Angular';
        descricao = "Canais com mudanças bruscas de direção em ângulos variados (não necessariamente 90°), controlados por fraturas e falhas oblíquas. Indica fraturamento intenso com múltiplos lineamentos geológicos em direções diversas.";
      } else if (isParalela) {
        padrao    = 'Paralela';
        descricao = "Canais correm em direções semelhantes, paralelos entre si. Indica forte declividade uniforme, relevo inclinado, vertentes longas ou escarpas com controle topográfico dominante — comum em áreas de cuesta ou relevos alongados.";
      } else if (isSubparalela) {
        padrao    = 'Subparalela';
        descricao = "Orientação parcialmente consistente dos canais com variação moderada. Padrão intermediário entre paralela e dendrítica, com algum controle direcional, comum em relevo suavemente inclinado com leve controle estrutural.";
      } else {
        padrao    = 'Dendrítica';
        descricao = "Cursos d'\u00e1gua ramificam-se irregularmente como galhos de árvore em ângulos variados. Típico de terreno litologicamente homogêneo sem forte controle estrutural, comum em áreas sedimentares ou cristalinas pouco estruturadas com relevo suavemente dissecado.";
      }
    }

    const ordens    = ordensRes.rows || [];
    const ordemMax  = ordens.reduce((m, r) => Math.max(m, parseInt(r.ordem || 0)), 0);
    const ordem1    = parseInt(ordens.find(r => parseInt(r.ordem) === 1)?.cnt || 0);
    const ordem2    = parseInt(ordens.find(r => parseInt(r.ordem) === 2)?.cnt || 0);
    const ordem3plus = ordens.filter(r => parseInt(r.ordem) >= 3).reduce((s, r) => s + parseInt(r.cnt || 0), 0);

    analyses['9.10_hidrografia'].padrao_drenagem = {
      padrao, descricao,
      raio_analise_km:   RAIO_DRENAGEM_KM,
      total_raio:        totalRaio,
      desvio_azimuth:    stdAz,
      rayleigh_r:        Math.round(rayleighR * 1000) / 1000,
      circ_spread:       Math.round(circSpread * 1000) / 1000,
      convergencia:      Math.round(meanConv * 1000) / 1000,
      tangencialidade:   Math.round(meanTang * 1000) / 1000,
      gradiente_radial:  Math.round(radialGrad),
      relevo_regional_m: Math.round(reliefM),
      bimodalidade_pct:  Math.round(bimodalFrac * 100),
      separacao_bins:    binSep,
      comprimento_km:    lenKm,
      ordem_maxima:      ordemMax,
      ordem1, ordem2, ordem3plus
    };
    analyses['9.10_hidrografia'].comprimento_influencia_km = lenKm;
    analyses['9.10_hidrografia'].nomes_rios = (nomesRes.rows || []).map(r => r.nome).filter(Boolean);

    // ── PARALELIZADO: riosGeomResult + altitude + carbono + aquíferos (4 blocos independentes) ──
    analyses['9.11_altitude'] = { nome: 'Altitude', min_m: null, max_m: null, media_m: null, ponto_m: null, grid: [] };
    analyses['9.12_carbono'] = { nome: 'Carbono', total_toneladas: null };
    analyses['9.13b_aquiferos'] = { nome: 'Aquíferos', poroso: [], fraturado: [], carstico: [], data: [] };

    // Função carbono (cliente separado por causa do statement_timeout 65s)
    const runCarbonoQuery = async () => {
      try {
        const carbClient = new Client({
          host: process.env.DB_HOST,
          port: parseInt(process.env.DB_PORT || '5432'),
          database: process.env.DB_NAME,
          user: process.env.DB_USER,
          password: process.env.DB_PASS,
          ssl: process.env.DB_SSL === 'false' ? false : { rejectUnauthorized: false },
          statement_timeout: 65000,
          family: 4
        });
        await carbClient.connect();
        try {
          const r = await carbClient.query(`
            WITH parcel_geom AS (
              SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AS g
            ),
            parcel_lat AS (SELECT ST_Y(ST_Centroid(g)) AS lat FROM parcel_geom),
            sample AS (SELECT ABS(ST_ScaleX(rast)) * ABS(ST_ScaleY(rast)) AS sx FROM carbono_solo.carbono_2024 LIMIT 1),
            clipped AS (
              SELECT ST_SetBandNoDataValue(ST_Clip(r.rast, pg.g, true), 0) AS rast
              FROM carbono_solo.carbono_2024 r, parcel_geom pg
              WHERE ST_Intersects(r.rast, pg.g)
            ),
            tile_stats AS (
              SELECT
                (ST_SummaryStats(rast, 1, true)).sum   AS s,
                (ST_SummaryStats(rast, 1, true)).count AS c,
                (ST_SummaryStats(rast, 1, true)).min   AS mn,
                (ST_SummaryStats(rast, 1, true)).max   AS mx
              FROM clipped
            )
            SELECT
              ROUND(CAST(COALESCE(SUM(s), 0) * sample.sx * COS(RADIANS(parcel_lat.lat)) * 111320.0 * 111320.0 / 10000.0 AS numeric), 2) AS total_toneladas,
              ROUND(CAST(COALESCE(SUM(s), 0) / NULLIF(SUM(c), 0) AS numeric), 2) AS media_t_ha,
              ROUND(CAST(MIN(mn) AS numeric), 2) AS min_t_ha,
              ROUND(CAST(MAX(mx) AS numeric), 2) AS max_t_ha,
              COALESCE(SUM(c), 0) AS pixels_validos
            FROM tile_stats, sample, parcel_lat
            GROUP BY sample.sx, parcel_lat.lat
          `, [geojsonStr]);
          return r.rows[0] || null;
        } finally {
          await carbClient.end();
        }
      } catch (e) {
        console.warn('[CARBONO] failed:', e.message);
        return null;
      }
    };

    console.time('[par] riosgeom+altitude+carbono+aquiferos');
    const [riosGeomResult, altitudeResult, altitudeGridResult, carbonoRow, aqRes] = await Promise.all([
      // Q-riosGeom: geometrias com nomes via JOIN LATERAL
      safeQuery(`
        WITH parcel AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AS g)
        SELECT
          ST_AsGeoJSON(ST_SimplifyPreserveTopology((ST_Dump(c.geom)).geom, 0.00005)) AS geom_json,
          COALESCE(NULLIF(TRIM(rn."NORIOCOMP"::text), ''), '') AS nome,
          COALESCE(c.nuordemcda, 0) AS ordem
        FROM hidrografia.geoft_bho_2017_curso_dagua c, parcel
        LEFT JOIN LATERAL (
          SELECT "NORIOCOMP" FROM hidrografia.rio_nomes rn2
          WHERE rn2.geom && c.geom
            AND ST_DWithin(rn2.geom::geography, ST_Centroid(c.geom)::geography, 100)
          LIMIT 1
        ) rn ON true
        WHERE c.geom && parcel.g AND ST_Intersects(c.geom, parcel.g)
        ORDER BY COALESCE(c.nuordemcda, 0) DESC NULLS LAST
        LIMIT 5000
      `, [geojsonStr]).catch(e => { console.warn('[riosGeom]', e.message); return { rows: [] }; }),
      // Q-altitude: stats de altitude na parcela
      safeQuery(`
        SELECT
          MIN(v.val) as min_alt,
          MAX(v.val) as max_alt,
          ROUND(CAST(AVG(v.val) AS numeric), 1) as avg_alt
        FROM altitude_br.altitude_raster r,
             LATERAL ST_PixelAsPoints(ST_Clip(r.rast, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))) v
        WHERE ST_Intersects(r.rast, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))
          AND v.val IS NOT NULL
      `, [geojsonStr]).catch(e => { console.warn('[altitude]', e.message); return { rows: [] }; }),
      // Q-altitude-grid: pixels do raster clipado à parcela (subamostrado no front)
      // Mesmo padrão da query de min/max — clip + PixelAsCentroids — proven fast
      safeQuery(`
        SELECT
          ST_X(pc.geom)::float8 AS lng,
          ST_Y(pc.geom)::float8 AS lat,
          pc.val::float8 AS m
        FROM altitude_br.altitude_raster r,
             LATERAL ST_PixelAsCentroids(ST_Clip(r.rast, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)), 1) pc
        WHERE ST_Intersects(r.rast, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))
          AND pc.val IS NOT NULL
        LIMIT 30000
      `, [geojsonStr]).catch(e => { console.warn('[altitude-grid]', e.message); return { rows: [] }; }),
      // Q-carbono: cliente separado com statement_timeout maior
      runCarbonoQuery(),
      // Q-aquíferos: 3 tipos numa só query (UNION)
      safeQuery(`
        WITH parc AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AS g)
        SELECT 'poroso' AS tipo, "SAQ_NM_NOM" AS nome, "SAQ_CD_COD" AS codigo, "SAQ_AR_KM2" AS area_km2
        FROM aquifero_br.aquiferos_poroso a, parc
        WHERE a.geom && parc.g AND ST_Intersects(a.geom, parc.g)
        UNION ALL
        SELECT 'fraturado', "SAQ_NM_NOM", "SAQ_CD_COD", "SAQ_AR_KM2"
        FROM aquifero_br.aquiferos_fraturado a, parc
        WHERE a.geom && parc.g AND ST_Intersects(a.geom, parc.g)
        UNION ALL
        SELECT 'carstico', "SAQ_NM_NOM", "SAQ_CD_COD", "SAQ_AR_KM2"
        FROM aquifero_br.aquiferos_carstico a, parc
        WHERE a.geom && parc.g AND ST_Intersects(a.geom, parc.g)
      `, [geojsonStr]).catch(e => { console.warn('[AQUIFEROS]', e.message); return { rows: [] }; }),
    ]);
    console.timeEnd('[par] riosgeom+altitude+carbono+aquiferos');

    // Processa riosGeom
    analyses['9.10_hidrografia'].rios_geom = riosGeomResult.rows || [];

    // Processa altitude
    if (altitudeResult.rows[0]) {
      analyses['9.11_altitude'].min_m = altitudeResult.rows[0].min_alt;
      analyses['9.11_altitude'].max_m = altitudeResult.rows[0].max_alt;
      analyses['9.11_altitude'].media_m = altitudeResult.rows[0].avg_alt;
    }
    // Processa altitude grid (mapa hipsométrico) - subamostra se vier muita coisa
    if (altitudeGridResult && altitudeGridResult.rows && altitudeGridResult.rows.length) {
      const raw = altitudeGridResult.rows;
      const TARGET = 5000; // mais pixels = mapa mais detalhado
      const stride = Math.max(1, Math.floor(raw.length / TARGET));
      const sampled = [];
      for (let i = 0; i < raw.length; i += stride) {
        const r = raw[i];
        sampled.push({ lng: parseFloat(r.lng), lat: parseFloat(r.lat), m: parseFloat(r.m) });
      }
      analyses['9.11_altitude'].grid = sampled;
      console.log('[altitude-grid] pixels brutos:', raw.length, '→ amostrados:', sampled.length);
    }
    if (municipio.centroid) {
      try {
        const centroidAltResult = await safeQuery(`
          SELECT ST_Value(r.rast, ST_SetSRID(ST_GeomFromGeoJSON($1), 4674)) as altitude
          FROM altitude_br.altitude_raster r
          WHERE ST_Intersects(r.rast, ST_SetSRID(ST_GeomFromGeoJSON($1), 4674))
          LIMIT 1
        `, [municipio.centroid]);
        if (centroidAltResult.rows[0]) {
          analyses['9.11_altitude'].ponto_m = centroidAltResult.rows[0].altitude;
        }
      } catch (_e) {}
    }
    if (analyses['9.11_altitude'].min_m === null && analyses['9.11_altitude'].ponto_m !== null) {
      analyses['9.11_altitude'].min_m = analyses['9.11_altitude'].ponto_m;
      analyses['9.11_altitude'].max_m = analyses['9.11_altitude'].ponto_m;
      analyses['9.11_altitude'].media_m = analyses['9.11_altitude'].ponto_m;
    }

    // Processa carbono
    if (carbonoRow) {
      analyses['9.12_carbono'].total_toneladas = carbonoRow.total_toneladas;
      analyses['9.12_carbono'].media_t_ha = carbonoRow.media_t_ha;
      analyses['9.12_carbono'].min_t_ha = carbonoRow.min_t_ha;
      analyses['9.12_carbono'].max_t_ha = carbonoRow.max_t_ha;
      analyses['9.12_carbono'].pixels_validos = carbonoRow.pixels_validos;
    }

    // Processa aquíferos
    for (const row of (aqRes?.rows || [])) {
      const item = { nome: row.nome || '', codigo: row.codigo || '', area_km2: row.area_km2 || null };
      const tgt = analyses['9.13b_aquiferos'];
      const flatItem = { tipo_aquifero: row.tipo, nome: item.nome, codigo: item.codigo, area_km2: item.area_km2 };
      if (row.tipo === 'poroso') tgt.poroso.push(item);
      else if (row.tipo === 'fraturado') tgt.fraturado.push(item);
      else if (row.tipo === 'carstico') tgt.carstico.push(item);
      if (!Array.isArray(tgt.data)) tgt.data = [];
      tgt.data.push(flatItem);
    }
    console.log('[AQUIFEROS] poroso:', analyses['9.13b_aquiferos'].poroso.length, 'fraturado:', analyses['9.13b_aquiferos'].fraturado.length, 'carstico:', analyses['9.13b_aquiferos'].carstico.length);

    // 9.13 CAR (área, APP, reserva legal, vegetação nativa)
    analyses['9.13_car'] = {
      nome: 'CAR',
      area_imovel: [],
      app_area_hectares: 0,
      reserva_legal_hectares: 0,
      vegetacao_nativa_hectares: 0,
      area_consolidada_hectares: 0,
    };

    let carAreaResult;
    if (isCARSelected) {
      // Parcela CAR selecionada → retorna só a parcela com maior sobreposição (ela mesma)
      carAreaResult = await safeQuery(`
        SELECT
          gid as id, cod_imovel, num_area, ind_tipo, ind_status, des_condic, dat_criaca, dat_atuali,
          ROUND(CAST(ST_Area(ST_Transform(geom, 32721)) / 10000 AS numeric), 2) as area_hectares
        FROM ${carAreaTable}
        WHERE geom && ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AND ST_Intersects(
          CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
        )
        ORDER BY ST_Area(ST_Transform(ST_Intersection(
          CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
        ), 32721)) DESC
        LIMIT 1
      `, [geojsonStr]);
    } else {
      // Parcela SIGEF/SNCI: usa buffer negativo de 5m + ST_Intersects.
      // Elimina vizinhos que só tocam a fronteira (não cobrem área real) mas captura
      // qualquer CAR que cobre parte da parcela, independente da forma/tamanho.
      // Fallback: se buffer -5m esvazia parcelas estreitas, usa parcela original.
      carAreaResult = await safeQuery(`
        WITH parcel_orig AS (
          SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}) AS g
        ),
        parcel_shrunk AS (
          SELECT
            CASE
              WHEN ST_IsEmpty(s) OR s IS NULL THEN p.g
              ELSE s
            END AS g
          FROM parcel_orig p,
          LATERAL (
            SELECT ST_Transform(
              ST_Buffer(ST_Transform(p.g, 32721), -20.0),
              ${SRID}
            ) AS s
          ) sub
        ),
        candidatos AS (
          -- Pre-filtra com parcela ENCOLHIDA (evita vizinhos tangenciais)
          -- Calcula ST_Intersection com parcela ORIGINAL (medida correta de "dentro")
          SELECT
            t.gid, t.cod_imovel, t.num_area, t.ind_tipo, t.ind_status,
            t.des_condic, t.dat_criaca, t.dat_atuali, t.geom AS car_geom,
            ST_Intersection(
              CASE WHEN ST_SRID(t.geom) != ${SRID} THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
              po.g
            ) AS inter_geom_orig
          FROM ${carAreaTable} t, parcel_shrunk ps, parcel_orig po
          WHERE t.geom && ps.g
            AND ST_Intersects(
              CASE WHEN ST_SRID(t.geom) != ${SRID} THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
              ps.g
            )
        ),
        candidatos_medidos AS (
          -- Pre-calcula areas em UTM uma única vez (evita recálculo no WHERE)
          SELECT
            c.*,
            ST_Area(ST_Transform(c.car_geom, 32721)) / 10000 AS area_car_ha,
            ST_Area(ST_Transform(c.inter_geom_orig, 32721)) / 10000 AS area_intersecao_ha
          FROM candidatos c
        )
        SELECT DISTINCT ON (cm.gid) cm.gid as id,
          cm.cod_imovel, cm.num_area, cm.ind_tipo, cm.ind_status, cm.des_condic, cm.dat_criaca, cm.dat_atuali,
          ROUND(CAST(cm.area_car_ha AS numeric), 2) as area_hectares,
          ROUND(CAST(cm.area_intersecao_ha AS numeric), 2) as area_intersecao_ha,
          ROUND(CAST((cm.area_car_ha - cm.area_intersecao_ha) AS numeric), 2) as area_fora_ha
        FROM candidatos_medidos cm
        WHERE cm.area_intersecao_ha >= 0.5
          AND (cm.area_car_ha - cm.area_intersecao_ha) <= 2.0
      `, [geojsonStr]);
    }

    analyses['9.13_car'].area_imovel = carAreaResult.rows;

    let appResult = await safeQuery(`
      SELECT
        ROUND(CAST(SUM(ST_Area(ST_Intersection(
          CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
        ))) / NULLIF(ST_Area(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}))::geography), 0) * 100 AS numeric), 2) as percentual_sobreposicao
      FROM ${carAppsTable}
      WHERE geom && ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AND ST_Intersects(
        CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
      )
    `, [geojsonStr]);

    if (appResult.rows[0] && appResult.rows[0].area_hectares) {
      analyses['9.13_car'].app_area_hectares = appResult.rows[0].area_hectares;
    }

    // Reserva Legal Proposta e Vegetação Nativa: busca por cod_imovel das parcelas CAR encontradas
    // usando num_area (valor declarado no CAR), não área calculada por intersecção espacial
    const codImoveisCar = analyses['9.13_car'].area_imovel
      .map(r => r.cod_imovel)
      .filter(Boolean);

    console.log('[DEBUG] codImoveisCar:', JSON.stringify(codImoveisCar));
    console.log('[DEBUG] carReservaTable:', carReservaTable, '| carVegNativaTable:', carVegNativaTable);

    if (codImoveisCar.length > 0) {
      const placeholders = codImoveisCar.map((_, i) => `$${i + 1}`).join(', ');

      // ── PARALELIZADO: 3 queries CAR (RL + Veg Nativa + Área Consolidada) — todas usam codImoveisCar mas são independentes entre si ──
      console.time('[par] car-detail');
      const [reservaLegalResult, vegNativaResult, areaConsolidadaResult] = await Promise.all([
        safeQuery(`
          SELECT ROUND(CAST(SUM(num_area) AS numeric), 2) as area_ha
          FROM (
            SELECT DISTINCT ON (gid) CAST(num_area AS numeric) as num_area
            FROM ${carReservaTable}
            WHERE cod_imovel IN (${placeholders})
          ) d
        `, codImoveisCar),
        safeQuery(`
          SELECT ROUND(CAST(SUM(num_area) AS numeric), 2) as area_ha
          FROM (
            SELECT DISTINCT ON (gid) CAST(num_area AS numeric) as num_area
            FROM ${carVegNativaTable}
            WHERE cod_imovel IN (${placeholders})
          ) d
        `, codImoveisCar),
        pool.query(`
          SELECT ROUND(CAST(SUM(num_area) AS numeric), 2) as area_ha
          FROM (
            SELECT DISTINCT ON (gid) CAST(num_area AS numeric) as num_area
            FROM ${carAreaConsolidadaTable}
            WHERE cod_imovel IN (${placeholders})
          ) d
        `, codImoveisCar).catch(e => {
          console.error('[DEBUG] areaConsolidada ERRO:', e.message, '| tabela:', carAreaConsolidadaTable);
          return { rows: [] };
        }),
      ]);
      console.timeEnd('[par] car-detail');

      if (reservaLegalResult.rows[0] && reservaLegalResult.rows[0].area_ha) {
        analyses['9.13_car'].reserva_legal_hectares = parseFloat(reservaLegalResult.rows[0].area_ha);
      }
      if (vegNativaResult.rows[0] && vegNativaResult.rows[0].area_ha) {
        analyses['9.13_car'].vegetacao_nativa_hectares = parseFloat(vegNativaResult.rows[0].area_ha);
      }
      if (areaConsolidadaResult.rows[0] && areaConsolidadaResult.rows[0].area_ha) {
        analyses['9.13_car'].area_consolidada_hectares = parseFloat(areaConsolidadaResult.rows[0].area_ha);
      }
    }

    // (legacy aquiferos block removed - now at line ~1602)

    // 9.13b' SILOS / ARMAZÉNS PRÓXIMOS (CONAB SICARM)
    analyses['9.13e_silos'] = {
      nome: 'Silos / Armazéns próximos',
      raio_km: 50,
      total_silos: 0,
      capacidade_total_t: 0,
      silos: [],
      por_tipo: {},
    };
    try {
      const silosRes = await pool.query(`
        WITH parcel AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AS g),
        centroid AS (SELECT ST_Centroid(g) AS c FROM parcel)
        SELECT s.id, s.razao_social, s.municipio, s.uf, s.tipo,
               s.capacidade_t, s.produtos, s.cnpj, s.situacao,
               s.lat, s.lng,
               ROUND(CAST(ST_Distance(s.geom::geography, c.c::geography) / 1000 AS numeric), 2) AS distancia_km
        FROM silos.armazens_brasil s, centroid c
        WHERE ST_DWithin(s.geom::geography, c.c::geography, 50000)
        ORDER BY ST_Distance(s.geom::geography, c.c::geography)
        LIMIT 200
      `, [geojsonStr]);
      const rows = silosRes.rows || [];
      analyses['9.13e_silos'].silos = rows.map(r => ({
        id: r.id, razao_social: r.razao_social, municipio: r.municipio, uf: r.uf,
        tipo: r.tipo, capacidade_t: parseFloat(r.capacidade_t) || 0,
        produtos: r.produtos, cnpj: r.cnpj, situacao: r.situacao,
        lat: parseFloat(r.lat), lng: parseFloat(r.lng),
        distancia_km: parseFloat(r.distancia_km),
      }));
      analyses['9.13e_silos'].total_silos = rows.length;
      analyses['9.13e_silos'].capacidade_total_t = rows.reduce((s, x) => s + (parseFloat(x.capacidade_t) || 0), 0);
      // Agrupa por tipo
      const porTipo = {};
      for (const r of rows) {
        const k = r.tipo || 'Outro';
        if (!porTipo[k]) porTipo[k] = { count: 0, cap_t: 0 };
        porTipo[k].count++;
        porTipo[k].cap_t += parseFloat(r.capacidade_t) || 0;
      }
      analyses['9.13e_silos'].por_tipo = porTipo;
    } catch (e) {
      // Tabela pode não existir ainda — silencia (precisa do import CONAB SICARM)
      if (!/does not exist/i.test(e.message)) {
        console.warn('[silos]', e.message);
      }
    }

    // 9.13f LOGÍSTICA — distâncias até cidades, portos e terminais
    analyses['9.13f_logistica'] = {
      nome: 'Logística',
      cidades_proximas: [],
      portos_maritimos: [],
      portos_fluviais: [],
      portos_secos: [],
      terminais_ferroviarios: [],
      rodovias_que_cruzam: [],
      rodovias_proximas: [],
    };
    try {
      const cidadesRes = await pool.query(`
        WITH parc AS (
          SELECT ST_PointOnSurface(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))) AS centroide
        )
        SELECT m."NM_MUN" AS nome, m."SIGLA_UF" AS uf,
          ROUND(CAST(ST_Distance(ST_Transform(m.geom, 4326)::geography, ST_Transform(parc.centroide, 4326)::geography) / 1000.0 AS numeric), 1) AS distancia_km
        FROM municipios.municipios_2024 m, parc
        WHERE ST_DWithin(ST_Transform(m.geom, 4326)::geography, ST_Transform(parc.centroide, 4326)::geography, 200000)
        ORDER BY distancia_km ASC LIMIT 15
      `, [geojsonStr]).catch(e => { console.warn('[LOGI cidades]', e.message); return { rows: [] }; });
      analyses['9.13f_logistica'].cidades_proximas = cidadesRes.rows || [];

      const termsRes = await pool.query(`
        WITH parc AS (
          SELECT ST_PointOnSurface(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))) AS centroide
        ),
        ranked AS (
          SELECT t.nome, t.tipo, t.municipio, t.uf,
            ROUND(CAST(ST_Distance(t.geom::geography, parc.centroide::geography) / 1000.0 AS numeric), 1) AS distancia_km,
            ROW_NUMBER() OVER (PARTITION BY t.tipo ORDER BY ST_Distance(t.geom::geography, parc.centroide::geography) ASC) AS rn
          FROM infra.terminais_logisticos t, parc
        )
        SELECT nome, tipo, municipio, uf, distancia_km FROM ranked WHERE rn <= 5 ORDER BY tipo, distancia_km
      `, [geojsonStr]).catch(e => { console.warn('[LOGI terms]', e.message); return { rows: [] }; });
      (termsRes.rows || []).forEach(r => {
        const key = r.tipo === 'porto_maritimo' ? 'portos_maritimos'
                  : r.tipo === 'porto_fluvial'  ? 'portos_fluviais'
                  : r.tipo === 'porto_seco'      ? 'portos_secos'
                  : r.tipo === 'terminal_ferroviario' ? 'terminais_ferroviarios' : null;
        if (key) analyses['9.13f_logistica'][key].push(r);
      });

      // Rodovias que cruzam a parcela
      const rodCruzaRes = await pool.query(`
        SELECT codigo, nome, jurisdicao, pavimentacao
        FROM infra.rodovias_principais
        WHERE ST_Intersects(geom, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))
        ORDER BY codigo
      `, [geojsonStr]).catch(e => { console.warn('[LOGI rod cruza]', e.message); return { rows: [] }; });
      analyses['9.13f_logistica'].rodovias_que_cruzam = rodCruzaRes.rows || [];

      // Rodovias próximas (≤25km) que NÃO cruzam
      const rodProxRes = await pool.query(`
        WITH parc AS (
          SELECT ST_PointOnSurface(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))) AS centroide,
                 ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AS poligono
        )
        SELECT r.codigo, r.nome, r.jurisdicao, r.pavimentacao,
          ROUND(CAST(ST_Distance(r.geom::geography, parc.centroide::geography) / 1000.0 AS numeric), 1) AS distancia_km
        FROM infra.rodovias_principais r, parc
        WHERE ST_Distance(r.geom::geography, parc.poligono::geography) <= 25000
          AND NOT ST_Intersects(r.geom, parc.poligono)
        ORDER BY distancia_km ASC
        LIMIT 8
      `, [geojsonStr]).catch(e => { console.warn('[LOGI rod prox]', e.message); return { rows: [] }; });
      analyses['9.13f_logistica'].rodovias_proximas = rodProxRes.rows || [];

    } catch (e) { console.warn('[LOGI]', e.message); }

    // 9.15 HIDROLOGIA + APTIDÃO PIVÔS CENTRAIS — análise integrada
    analyses['9.15_hidrologia_pivos'] = {
      nome: 'Hidrologia e Aptidão para Pivôs Centrais',
      iap_score: 0,
      iap_classificacao: 'baixa',
      iap_componentes: {},
      configuracoes_sugeridas: [],
      demanda_hidrica: {},
      fontes_agua: [],
      restricoes: [],
      outorga: {},
      sistemas_irrigacao_comparativo: [],
      aptidao_culturas: [],
      custos_estimados: {},
    };
    try {
      const _decl = (analyses?.['9.12_altitude']?.declividade_pct_media ?? analyses?.['9.12_altitude']?.declividade_pct ?? 5);
      const _pma = (analyses?.['9.11_pluviometria']?.media_anual_30anos ?? analyses?.['9.11_pluviometria']?.resumo?.media_anual_mm ?? 1200);
      const _solos = analyses?.['9.4_solo']?.data || [];
      const _solosNomes = _solos.map(s => (s.nome || '').toUpperCase());
      const _bioma = analyses?.['9.3_bioma']?.data?.[0]?.nome || '';
      const _bacias = analyses?.['9.10_hidrografia']?.bacias || [];
      const _cursos = analyses?.['9.10_hidrografia']?.cursos_agua_count || 0;
      const _padraoDren = analyses?.['9.10_hidrografia']?.padrao_drenagem || {};
      const _aqPoroso = analyses?.['9.13b_aquiferos']?.poroso || [];
      const _aqFraturado = analyses?.['9.13b_aquiferos']?.fraturado || [];
      const _aqCarstico = analyses?.['9.13b_aquiferos']?.carstico || [];
      const _areaHa = parseFloat(analyses?.area_total_ha || 0) || 100;

      // (a) Declividade — 30%
      let scDecl = 0;
      if (_decl <= 3) scDecl = 100;
      else if (_decl <= 8) scDecl = 90 - (_decl - 3) * 4;
      else if (_decl <= 13) scDecl = 70 - (_decl - 8) * 8;
      else if (_decl <= 20) scDecl = 30 - (_decl - 13) * 4;
      else scDecl = 0;

      // (b) Solo — 20%
      const _hasArgila = _solosNomes.some(n => /LATOSSOLO|ARGISSOLO|NITOSSOLO|CAMBISSOLO/.test(n));
      const _hasArenoso = _solosNomes.some(n => /NEOSSOLO\s+QUARTZAR/.test(n));
      const _hasGleico = _solosNomes.some(n => /GLEISSOLO|ORGANOSSOLO|PLANOSSOLO/.test(n));
      let scSolo = 60;
      if (_hasArgila && !_hasArenoso) scSolo = 90;
      if (_hasArenoso) scSolo = 45;
      if (_hasGleico) scSolo = 25;

      // (c) Água — 25%
      let scAgua = 30;
      if (_cursos >= 3 || _bacias.length >= 1) scAgua += 30;
      if (_padraoDren.ordem_maxima >= 4) scAgua += 15;
      if (_aqFraturado.length || _aqPoroso.length) scAgua += 20;
      if (_aqCarstico.length) scAgua += 5;
      scAgua = Math.min(100, scAgua);

      // (d) Déficit — 15%
      const etoAnual = 1825;
      const deficit = Math.max(0, etoAnual - _pma);
      let scDeficit = 0;
      if (deficit >= 800) scDeficit = 100;
      else if (deficit >= 500) scDeficit = 85;
      else if (deficit >= 300) scDeficit = 70;
      else if (deficit >= 150) scDeficit = 50;
      else if (deficit >= 50) scDeficit = 30;
      else scDeficit = 10;

      // (e) Forma — 10%
      let scForma = 50;
      if (_areaHa >= 60) scForma += 20;
      if (_areaHa >= 200) scForma += 20;
      scForma = Math.min(100, scForma);

      const iap = Math.round(scDecl*0.30 + scSolo*0.20 + scAgua*0.25 + scDeficit*0.15 + scForma*0.10);
      const classif = iap >= 75 ? 'alta' : (iap >= 50 ? 'media' : 'baixa');

      analyses['9.15_hidrologia_pivos'].iap_score = iap;
      analyses['9.15_hidrologia_pivos'].iap_classificacao = classif;
      analyses['9.15_hidrologia_pivos'].iap_componentes = {
        declividade: { score: Math.round(scDecl), peso: 30, valor: _decl },
        solo: { score: Math.round(scSolo), peso: 20, valor: _solos.length ? (_solos[0].nome || '?') : '?' },
        disponibilidade_agua: { score: Math.round(scAgua), peso: 25, cursos: _cursos, aquiferos: _aqPoroso.length + _aqFraturado.length + _aqCarstico.length },
        deficit_hidrico: { score: Math.round(scDeficit), peso: 15, deficit_mm: Math.round(deficit), pluv_mm: Math.round(_pma) },
        forma_tamanho: { score: Math.round(scForma), peso: 10, area_ha: _areaHa }
      };

      // Empacotamento de pivôs (maior círculo inscrito)
      try {
        const pivosRes = await pool.query(`
          WITH p AS (SELECT ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674), 32721) AS g),
          tentativas AS (
            SELECT raio, ST_Buffer(p.g, -raio) AS centros, PI()*raio*raio/10000.0 AS area_p
            FROM p, (VALUES (309), (276), (240), (200), (160), (120)) AS r(raio)
          ),
          melhor AS (
            SELECT raio, area_p, ST_AsGeoJSON(ST_Transform(ST_PointOnSurface(centros), 4674)) AS centro_wgs_json
            FROM tentativas WHERE NOT ST_IsEmpty(centros) ORDER BY raio DESC LIMIT 1
          )
          SELECT * FROM melhor
        `, [geojsonStr]).catch(e => { console.warn('[PIVOS]', e.message); return { rows: [] }; });
        if (pivosRes.rows.length > 0) {
          const row = pivosRes.rows[0];
          const centroWgs = JSON.parse(row.centro_wgs_json || '{}');
          const raio = parseFloat(row.raio);
          const areaP = parseFloat(row.area_p);
          analyses['9.15_hidrologia_pivos'].configuracoes_sugeridas.push({
            tipo: 'pivo_central',
            quantidade: 1,
            raio_m: raio,
            area_ha_cada: Math.round(areaP * 100) / 100,
            area_total_ha: Math.round(areaP * 100) / 100,
            cobertura_pct: Math.round((areaP / _areaHa) * 100),
            centros: centroWgs.coordinates ? [{ lng: centroWgs.coordinates[0], lat: centroWgs.coordinates[1], raio_m: raio, area_ha: Math.round(areaP*100)/100 }] : []
          });
        }
      } catch (e) { console.warn('[PIVOS empacotamento]', e.message); }

      // Demanda hídrica
      const etoDiario = /CAATINGA/i.test(_bioma) ? 6.0 : /CERRADO/i.test(_bioma) ? 5.2 : /MATA.*ATL/i.test(_bioma) ? 4.5 : /PAMPA/i.test(_bioma) ? 3.8 : /AMAZ/i.test(_bioma) ? 4.0 : /PANTANAL/i.test(_bioma) ? 5.0 : 5.0;
      const cfg0 = analyses['9.15_hidrologia_pivos'].configuracoes_sugeridas[0];
      const areaIrrigada = cfg0?.area_total_ha || Math.min(_areaHa, 100);
      const consumoDiario_m3 = (etoDiario * 0.95 * areaIrrigada * 10) / 0.85;
      const vazao_m3h = consumoDiario_m3 / 20;
      const vazao_Ls = vazao_m3h * 1000 / 3600;
      analyses['9.15_hidrologia_pivos'].demanda_hidrica = {
        eto_diario_mm: etoDiario, kc_medio: 0.95, eficiencia_irrigacao: 0.85,
        consumo_diario_m3: Math.round(consumoDiario_m3),
        vazao_necessaria_m3h: Math.round(vazao_m3h),
        vazao_necessaria_Ls: Math.round(vazao_Ls * 10) / 10,
        deficit_anual_mm: Math.round(deficit), bioma: _bioma || '?'
      };

      // Fontes
      const fontes = [];
      if (_cursos >= 1 || _bacias.length >= 1) {
        fontes.push({ tipo: 'superficial', nome: (analyses?.['9.10_hidrografia']?.nomes_rios || []).slice(0, 3).join(', ') || 'Curso d\u00e1gua local', bacia: _bacias[0]?.nome_bacia || _bacias[0]?.nome || '?', ordem_strahler: _padraoDren.ordem_maxima || null, recomendacao: _padraoDren.ordem_maxima >= 4 ? 'Fonte primária — outorga superficial viável' : 'Fonte secundária — verificar perenidade', orgao_outorga: 'ANA ou órgão estadual' });
      }
      if (_aqFraturado.length) fontes.push({ tipo: 'subterranea_fraturada', nome: _aqFraturado.map(a => a.nome).filter(Boolean).join(', ') || 'Aquífero fraturado', profundidade_tipica_m: '80-200', vazao_tipica_m3h: '5-30', recomendacao: 'Poço tubular profundo — produtividade variável' });
      if (_aqPoroso.length) fontes.push({ tipo: 'subterranea_porosa', nome: _aqPoroso.map(a => a.nome).filter(Boolean).join(', ') || 'Aquífero poroso', profundidade_tipica_m: '30-150', vazao_tipica_m3h: '20-80', recomendacao: 'Poço tubular — boa produtividade' });
      if (_aqCarstico.length) fontes.push({ tipo: 'subterranea_carstica', nome: _aqCarstico.map(a => a.nome).filter(Boolean).join(', ') || 'Aquífero cárstico', recomendacao: 'Vazão alta possível, risco de subsidência — estudo geotécnico' });
      if (!fontes.length) fontes.push({ tipo: 'nenhuma_identificada', recomendacao: 'Avaliar captação em propriedade vizinha ou caminhão-pipa' });
      analyses['9.15_hidrologia_pivos'].fontes_agua = fontes;

      // Restrições
      const restr = [];
      if (_decl > 13) restr.push({ severidade: 'critica', texto: `Declividade média ${_decl.toFixed(1)}% — acima do limite técnico de 13% para pivô central` });
      else if (_decl > 8) restr.push({ severidade: 'atencao', texto: `Declividade média ${_decl.toFixed(1)}% — operação possível mas com custo de energia maior` });
      if (_hasArenoso) restr.push({ severidade: 'atencao', texto: 'NEOSSOLO QUARTZARÊNICO presente — risco de lixiviação' });
      if (_hasGleico) restr.push({ severidade: 'critica', texto: 'GLEISSOLO/PLANOSSOLO — má drenagem natural, pivô contraindicado' });
      if (deficit < 100) restr.push({ severidade: 'atencao', texto: `Pluviometria ${Math.round(_pma)}mm/ano — ROI baixo em região úmida` });
      if (!_cursos && !_aqPoroso.length && !_aqFraturado.length) restr.push({ severidade: 'critica', texto: 'Nenhuma fonte de água identificada — captação externa necessária' });
      if (_areaHa < 30) restr.push({ severidade: 'atencao', texto: `Parcela com ${_areaHa.toFixed(0)}ha — CAPEX alto, considerar gotejamento ou aspersão linear` });
      analyses['9.15_hidrologia_pivos'].restricoes = restr;

      analyses['9.15_hidrologia_pivos'].outorga = {
        orgao_competente: fontes[0]?.orgao_outorga || 'Verificar com órgão estadual',
        tempo_medio_dias: 60,
        documentos_basicos: ['Declaração de uso', 'Cadastro CNARH', 'Outorga preventiva', 'Outorga de direito', 'Projeto técnico assinado']
      };

      analyses['9.15_hidrologia_pivos'].sistemas_irrigacao_comparativo = [
        { sistema: 'Pivô central', capex_rha: 7500, opex_rha_ano: 1200, eficiencia: 85, aptidao_local: classif, observacao: 'Padrão para grandes áreas' },
        { sistema: 'Aspersão convencional', capex_rha: 5000, opex_rha_ano: 1000, eficiencia: 75, aptidao_local: _decl <= 20 ? 'alta' : 'media', observacao: 'Flexível mas trabalhoso' },
        { sistema: 'Gotejamento', capex_rha: 12000, opex_rha_ano: 1500, eficiencia: 95, aptidao_local: 'alta', observacao: 'Eficiência máxima — frutas, hortaliças, café' },
        { sistema: 'Sulcos / inundação', capex_rha: 3000, opex_rha_ano: 800, eficiencia: 50, aptidao_local: _decl <= 2 ? 'alta' : 'baixa', observacao: 'Só em terreno muito plano' }
      ];

      const aptCultura = (nome, plu_min, plu_max, decl_max, exige_argiloso, observ) => {
        let apt = 'alta';
        const motivos = [];
        if (_pma < plu_min - 200 && iap < 50) { apt = 'baixa'; motivos.push(`Pluviometria baixa sem irrigação`); }
        if (_decl > decl_max) { apt = 'baixa'; motivos.push(`Declividade ${_decl}% > limite ${decl_max}%`); }
        if (exige_argiloso && _hasArenoso && !_hasArgila) { apt = apt === 'alta' ? 'media' : 'baixa'; motivos.push('Solo arenoso desfavorece'); }
        return { cultura: nome, aptidao: apt, observacoes: motivos.length ? motivos.join('; ') : observ };
      };
      analyses['9.15_hidrologia_pivos'].aptidao_culturas = [
        aptCultura('Soja', 600, 1800, 15, false, 'Cultura amplamente adaptada'),
        aptCultura('Milho', 500, 1800, 15, false, 'Excelente resposta à irrigação'),
        aptCultura('Algodão', 700, 1300, 10, true, 'Exige solo bem drenado'),
        aptCultura('Cana-de-açúcar', 1100, 1800, 8, true, 'Tradicional irrigado'),
        aptCultura('Café', 1200, 1800, 25, false, 'Pivô viável; mais comum gotejamento'),
        aptCultura('Feijão', 300, 900, 13, false, 'Ótima para safrinha irrigada'),
        aptCultura('Hortaliças', 400, 1200, 8, false, 'Alta exigência — pivô ou gotejamento'),
        aptCultura('Pastagem', 600, 1800, 30, false, 'Aumenta lotação 3-5x'),
        aptCultura('Arroz inundado', 1100, 2200, 1, true, 'Incompatível com pivô')
      ];

      const areaCalc = cfg0?.area_total_ha || areaIrrigada;
      analyses['9.15_hidrologia_pivos'].custos_estimados = {
        sistema_recomendado: classif === 'alta' ? 'Pivô central' : (classif === 'media' ? 'Aspersão convencional ou Pivô parcial' : 'Gotejamento (cultura específica) ou sem irrigação'),
        capex_total_estimado: Math.round(areaCalc * 7500),
        opex_anual_estimado: Math.round(areaCalc * 1200),
        poco_tubular_estimado: fontes.some(f => f.tipo.startsWith('subterranea')) ? 'R$ 80.000–150.000' : null,
        outorga_estimado: 'R$ 5.000–15.000',
        prazo_payback_anos: classif === 'alta' ? '3-5' : (classif === 'media' ? '5-8' : '> 8 ou inviável'),
        observacao: 'Valores médios 2024'
      };
    } catch (e) { console.error('[HIDRO_PIVOS]', e.message); }

    // 9.13c PRODES + DETER (desmatamento INPE via WFS TerraBrasilis)
    analyses['9.13c_prodes_deter'] = {
      nome: 'Desmatamento (PRODES/DETER)',
      prodes: [],
      deter: [],
      prodes_area_total_ha: 0,
      deter_area_total_ha: 0,
      fonte: 'INPE TerraBrasilis (WFS)',
      debug: { wfs_counts: {}, errors: [], ms: 0 },
    };
    try {
      // Bbox da parcela em EPSG:4326
      const bboxResult = await pool.query(`
        SELECT ST_XMin(g) AS minx, ST_YMin(g) AS miny, ST_XMax(g) AS maxx, ST_YMax(g) AS maxy
        FROM (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) g) x
      `, [geojsonStr]);
      const bb = bboxResult.rows[0];
      if (bb) {
        const bbox = [parseFloat(bb.minx), parseFloat(bb.miny), parseFloat(bb.maxx), parseFloat(bb.maxy)];
        // Roda WFS pra todos biomas em paralelo (cada um com timeout próprio)
        const t0 = Date.now();
        const [prodesAmz, prodesCer, prodesMA, prodesCa, prodesPampa, prodesPant, deterAmz, deterCer] = await Promise.all([
          fetchWFS('prodes-amazon-nb:yearly_deforestation_biome', bbox, 15000),
          fetchWFS('prodes-cerrado-nb:yearly_deforestation', bbox, 15000),
          fetchWFS('prodes-mata-atlantica-nb:yearly_deforestation', bbox, 15000),
          fetchWFS('prodes-caatinga-nb:yearly_deforestation', bbox, 15000),
          fetchWFS('prodes-pampa-nb:yearly_deforestation', bbox, 15000),
          fetchWFS('prodes-pantanal-nb:yearly_deforestation', bbox, 15000),
          fetchWFS('deter-amz:deter_amz', bbox, 15000),
          fetchWFS('deter-cerrado-nb:deter_cerrado', bbox, 15000),
        ]);
        const elapsed = Date.now() - t0;
        const wfsCounts = {
          prodes_amazon: (prodesAmz.features || []).length,
          prodes_cerrado: (prodesCer.features || []).length,
          prodes_mata_atlantica: (prodesMA.features || []).length,
          prodes_caatinga: (prodesCa.features || []).length,
          prodes_pampa: (prodesPampa.features || []).length,
          prodes_pantanal: (prodesPant.features || []).length,
          deter_amz: (deterAmz.features || []).length,
          deter_cerrado: (deterCer.features || []).length,
        };
        const wfsErrors = {};
        for (const [k, v] of [
          ['prodes_amazon', prodesAmz], ['prodes_cerrado', prodesCer], ['prodes_mata_atlantica', prodesMA],
          ['prodes_caatinga', prodesCa], ['prodes_pampa', prodesPampa], ['prodes_pantanal', prodesPant],
          ['deter_amz', deterAmz], ['deter_cerrado', deterCer]
        ]) { if (v.error) wfsErrors[k] = v.error; }
        analyses['9.13c_prodes_deter'].debug = { wfs_counts: wfsCounts, errors: wfsErrors, ms: elapsed };
        console.log('[prodes-deter] WFS ms:', elapsed, 'counts:', JSON.stringify(wfsCounts), 'errors:', JSON.stringify(wfsErrors));

        // Junta todos features PRODES + faz intersect real
        const allProdesFeatures = []
          .concat(prodesAmz.features || [], prodesCer.features || [], prodesMA.features || [],
                  prodesCa.features || [], prodesPampa.features || [], prodesPant.features || []);
        const prodesProps = ['uid', 'year', 'class_name', 'main_class', 'state', 'image_date', 'satellite', 'sensor', 'area_km'];
        const prodesItems = await intersectFeaturesWithParcel(allProdesFeatures, geojsonStr, prodesProps);

        const allDeterFeatures = [].concat(deterAmz.features || [], deterCer.features || []);
        const deterProps = ['uid', 'classname', 'view_date', 'sensor', 'satellite', 'areamunkm', 'areauckm', 'areatotalkm', 'municipalit', 'uf'];
        const deterItems = await intersectFeaturesWithParcel(allDeterFeatures, geojsonStr, deterProps);

        analyses['9.13c_prodes_deter'].prodes = prodesItems.sort((a, b) => (a.year || 0) - (b.year || 0));
        analyses['9.13c_prodes_deter'].deter = deterItems.sort((a, b) => (a.view_date || '').localeCompare(b.view_date || ''));
        analyses['9.13c_prodes_deter'].prodes_area_total_ha = prodesItems.reduce((s, x) => s + (x.area_ha || 0), 0);
        analyses['9.13c_prodes_deter'].deter_area_total_ha = deterItems.reduce((s, x) => s + (x.area_ha || 0), 0);
      }
    } catch (e) {
      console.warn('[prodes-deter] erro:', e.message);
    }

    // 9.13d ANÁLISE DE CONFORMIDADE AMBIENTAL (Código Florestal + CAR × PRODES × DETER)
    {
      const ufAmazoniaLegal = new Set(['AC','AP','AM','MA','MT','PA','RO','RR','TO']);
      const uf = (municipio.uf || '').toUpperCase();
      const wfsCounts = analyses['9.13c_prodes_deter']?.debug?.wfs_counts || {};
      // Detecta bioma dominante: se PRODES Amazon retornou features → Amazônia, senão Cerrado/Pantanal/etc
      let bioma = 'OUTROS', rlMinPct = 0.20;
      if ((wfsCounts.prodes_amazon || 0) > 0) { bioma = 'AMAZÔNIA'; rlMinPct = ufAmazoniaLegal.has(uf) ? 0.80 : 0.20; }
      else if ((wfsCounts.prodes_cerrado || 0) > 0) { bioma = 'CERRADO'; rlMinPct = ufAmazoniaLegal.has(uf) ? 0.35 : 0.20; }
      else if ((wfsCounts.prodes_pantanal || 0) > 0) { bioma = 'PANTANAL'; rlMinPct = 0.20; }
      else if ((wfsCounts.prodes_mata_atlantica || 0) > 0) { bioma = 'MATA ATLÂNTICA'; rlMinPct = 0.20; }
      else if ((wfsCounts.prodes_caatinga || 0) > 0) { bioma = 'CAATINGA'; rlMinPct = 0.20; }
      else if ((wfsCounts.prodes_pampa || 0) > 0) { bioma = 'PAMPA'; rlMinPct = 0.20; }

      const areaTotal = parseFloat(municipio.area_hectares) || 0;
      const carImoveis = analyses['9.13_car']?.area_imovel || [];
      const carNumAreaHa = carImoveis.reduce((s, c) => s + (parseFloat(c.num_area) || 0), 0);
      const areaRef = Math.max(areaTotal, carNumAreaHa); // usa o maior dos dois
      const rlDeclarada = analyses['9.13_car']?.reserva_legal_hectares || 0;
      const vegNativa = analyses['9.13_car']?.vegetacao_nativa_hectares || 0;
      const areaConsolidada = analyses['9.13_car']?.area_consolidada_hectares || 0;
      const prodesTotal = analyses['9.13c_prodes_deter']?.prodes_area_total_ha || 0;
      const deterTotal = analyses['9.13c_prodes_deter']?.deter_area_total_ha || 0;
      // DETER recente: alertas dos últimos 24 meses
      const cutoff24m = new Date(); cutoff24m.setMonth(cutoff24m.getMonth() - 24);
      const deterRecente = (analyses['9.13c_prodes_deter']?.deter || [])
        .filter(d => d.view_date && new Date(d.view_date) >= cutoff24m);
      const deterRecenteHa = deterRecente.reduce((s, d) => s + (d.area_ha || 0), 0);

      const rlMinHa = areaRef * rlMinPct;
      const rlAtende = rlDeclarada >= rlMinHa;
      // Discrepância: se PRODES > Área Consolidada declarada, o CAR declarou MENOS área desmatada que o real
      const discrepanciaConsolidada = prodesTotal > 0 && areaConsolidada > 0 && prodesTotal > areaConsolidada * 1.1;
      // Discrepância: Veg Nativa CAR + Área Consolidada CAR ≠ Área Total (com 5% tolerância)
      const somaCAR = vegNativa + areaConsolidada;
      const desbalanco = areaRef > 0 ? Math.abs(somaCAR - areaRef) / areaRef : 0;
      // CAR status crítico
      const statusCritico = carImoveis.some(c => ['CA', 'SU'].includes((c.ind_status || '').toUpperCase()));
      const statusAtivo = carImoveis.every(c => (c.ind_status || '').toUpperCase() === 'AT');

      const flags = [];
      // RED FLAGS
      if (deterRecenteHa > 0) {
        flags.push({ nivel: 'red', titulo: `DETER recente: ${deterRecenteHa.toFixed(2)} ha em ${deterRecente.length} alerta(s) nos últimos 24 meses`, descricao: 'Desmatamento ativo detectado. Pode estar gerando embargo automático IBAMA (Decreto 11.687/2023) e bloqueio de crédito rural (Res. BACEN 4.943/2021).' });
      }
      if (carImoveis.length > 0 && !rlAtende && rlMinHa > 0) {
        const deficit = rlMinHa - rlDeclarada;
        flags.push({ nivel: 'red', titulo: `Reserva Legal declarada (${rlDeclarada.toFixed(2)} ha) é inferior ao mínimo legal (${rlMinHa.toFixed(2)} ha)`, descricao: `Bioma ${bioma}${ufAmazoniaLegal.has(uf) ? ' (Amazônia Legal)' : ''} exige ${(rlMinPct * 100).toFixed(0)}% de RL. Déficit: ${deficit.toFixed(2)} ha. Recompor via PRA — custo estimado R$ ${(deficit * 12000).toLocaleString('pt-BR', {maximumFractionDigits: 0})} (R$ ~12k/ha plantio nativa).` });
      }
      if (discrepanciaConsolidada) {
        flags.push({ nivel: 'red', titulo: 'Discrepância CAR × PRODES detectada', descricao: `PRODES indica ${prodesTotal.toFixed(2)} ha desmatado historicamente, mas CAR declara apenas ${areaConsolidada.toFixed(2)} ha como área consolidada. Possível omissão na declaração CAR — alto risco fiscalização.` });
      }
      if (statusCritico) {
        flags.push({ nivel: 'red', titulo: 'CAR com status crítico (Cancelado ou Suspenso)', descricao: 'Propriedade não tem CAR ativo. Bloqueia certificação, crédito rural e exportação (Moratória da Soja/Acordo da Carne).' });
      }
      // YELLOW FLAGS
      if (prodesTotal > 0 && areaRef > 0 && prodesTotal / areaRef > 0.1) {
        flags.push({ nivel: 'yellow', titulo: `Passivo PRODES significativo (${(prodesTotal / areaRef * 100).toFixed(1)}% da propriedade)`, descricao: `${prodesTotal.toFixed(2)} ha de desmatamento histórico oficial. Se sobrepor RL/APP, é infração consumada (Lei 9.605/98).` });
      }
      if (desbalanco > 0.05 && carImoveis.length > 0 && areaRef > 0) {
        flags.push({ nivel: 'yellow', titulo: 'Veg. Nativa + Área Consolidada CAR não fecha com área total', descricao: `Soma declarada: ${somaCAR.toFixed(2)} ha, área da propriedade: ${areaRef.toFixed(2)} ha (${(desbalanco * 100).toFixed(1)}% de diferença). Pode haver APP/servidão não declarada ou inconsistência.` });
      }
      if (carImoveis.some(c => (c.ind_status || '').toUpperCase() === 'PE' || (c.des_condic || '').toLowerCase().includes('aguardando'))) {
        flags.push({ nivel: 'yellow', titulo: 'CAR aguardando análise', descricao: 'Auto-declaração ainda não validada pelo órgão estadual. Pode haver pendências bloqueando atos administrativos.' });
      }
      // GREEN FLAGS
      if (prodesTotal === 0 && deterTotal === 0) {
        flags.push({ nivel: 'green', titulo: '✓ Nenhum desmatamento PRODES/DETER detectado', descricao: 'Propriedade sem passivo de desmatamento oficial pelo INPE. Apta a Moratória da Soja e Acordo da Carne.' });
      }
      if (carImoveis.length > 0 && rlAtende && rlMinHa > 0) {
        flags.push({ nivel: 'green', titulo: `✓ Reserva Legal atende mínimo legal (${(rlMinPct * 100).toFixed(0)}%)`, descricao: `RL declarada (${rlDeclarada.toFixed(2)} ha) >= mínimo do bioma ${bioma} (${rlMinHa.toFixed(2)} ha).` });
      }
      if (carImoveis.length > 0 && statusAtivo) {
        flags.push({ nivel: 'green', titulo: '✓ CAR Ativo', descricao: 'Cadastro Ambiental Rural em situação ativa.' });
      }

      analyses['9.13d_conformidade'] = {
        nome: 'Conformidade Ambiental',
        bioma,
        bioma_uf: uf,
        amazonia_legal: ufAmazoniaLegal.has(uf),
        rl_minima_pct: rlMinPct,
        rl_minima_ha: parseFloat(rlMinHa.toFixed(2)),
        rl_declarada_ha: parseFloat(rlDeclarada.toFixed(2)),
        rl_atende_minimo: rlAtende,
        veg_nativa_declarada_ha: parseFloat(vegNativa.toFixed(2)),
        area_consolidada_declarada_ha: parseFloat(areaConsolidada.toFixed(2)),
        prodes_total_ha: parseFloat(prodesTotal.toFixed(2)),
        deter_total_ha: parseFloat(deterTotal.toFixed(2)),
        deter_recente_ha: parseFloat(deterRecenteHa.toFixed(2)),
        deter_recente_count: deterRecente.length,
        discrepancia_consolidada: discrepanciaConsolidada,
        desbalanco_pct: parseFloat((desbalanco * 100).toFixed(1)),
        flags,
      };
    }

    // 9.14 Análises Adicionais (Geologia Estrutural + Ocorrências + Tectônica)
    analyses['9.14_analises_adicionais'] = {
      nome: 'Análises Adicionais',
      geologia: {
        pontos: [],          // geol_ponto — afloramentos
        ocorrencias: [],     // ocorrências_br — ocorrências minerais
        linhas_dobra: [],    // geol_linha_dobra
        linhas_falha: [],    // geol_linha_falha
        linhas_fratura: [],  // geol_linha_fratura
      },
      tectonicas: {
        plate_boundary: [],
        continent_structure: [],
        craton_terranes_limits: [],
        dykes: [],
        eclogites: [],
        isopachs: [],
        kimberlites: [],
        paleoz_erosional_border: [],
        suture_zones: [],
      },
    };

    // Helper para normalizar SRID e retornar geom_json simplificado (linhas)
    const geomLinhaJson = `ST_AsGeoJSON(ST_SimplifyPreserveTopology(CASE WHEN ST_SRID(geom)=0 THEN ST_SetSRID(geom,4674) ELSE geom END, 0.001))`;
    const geomPtoJson   = `ST_AsGeoJSON(CASE WHEN ST_SRID(geom)=0 THEN ST_SetSRID(geom,4674) ELSE geom END)`;
    const geoIntersect  = `ST_Intersects(CASE WHEN ST_SRID(geom)=0 THEN ST_SetSRID(geom,${SRID}) ELSE geom END, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))`;

    // ── PARALELIZADO: 5 queries de estruturas geológicas (independentes) ──
    console.time('[par] estruturas-geol');
    const [
      geolPontoResult,
      ocorrenciasResult,
      geolLinhaDbraResult,
      geolLinhaFalhaResult,
      geolLinhaFraturaResult,
    ] = await Promise.all([
      safeQuery(`SELECT cd_fcim, ds_afl1, nm_unidade as nome, tipo_pto as tipo, fonte, ${geomPtoJson} as geom_json FROM geologia_litologia.geol_ponto WHERE ${geoIntersect}`, [geojsonStr]),
      safeQuery(`SELECT "SUBSTANCIAS" as substancias, "ROCHAS_HOSPEDEIRAS" as rochas_hospedeiras, ST_AsGeoJSON(geom) as geom_json FROM geologia_litologia."ocorrências_br" WHERE ST_Intersects(geom, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))`, [geojsonStr]),
      safeQuery(`SELECT cd_fcim, classif, caract_eix, caime_eix, ${geomLinhaJson} as geom_json FROM geologia_litologia.geol_linha_dobra WHERE ${geoIntersect}`, [geojsonStr]),
      safeQuery(`SELECT cd_fcim, classif, forma, estm_merg, sentido, ${geomLinhaJson} as geom_json FROM geologia_litologia.geol_linha_falha WHERE ${geoIntersect}`, [geojsonStr]),
      safeQuery(`SELECT cd_fcim, classif, forma, mergulho, ${geomLinhaJson} as geom_json FROM geologia_litologia.geol_linha_fratura WHERE ${geoIntersect}`, [geojsonStr]),
    ]);
    console.timeEnd('[par] estruturas-geol');
    analyses['9.14_analises_adicionais'].geologia.pontos = geolPontoResult.rows;
    analyses['9.5_geologia'].pontos = geolPontoResult.rows;
    analyses['9.14_analises_adicionais'].geologia.ocorrencias = ocorrenciasResult.rows;
    analyses['9.5_geologia'].ocorrencias = ocorrenciasResult.rows;
    analyses['9.14_analises_adicionais'].geologia.linhas_dobra = geolLinhaDbraResult.rows;
    analyses['9.5_geologia'].dobras = geolLinhaDbraResult.rows;
    analyses['9.14_analises_adicionais'].geologia.linhas_falha = geolLinhaFalhaResult.rows;
    analyses['9.5_geologia'].falhas = geolLinhaFalhaResult.rows;
    analyses['9.14_analises_adicionais'].geologia.linhas_fratura = geolLinhaFraturaResult.rows;
    analyses['9.5_geologia'].fraturas = geolLinhaFraturaResult.rows;

    // Tectônicas — todas as tabelas do schema tectonic_map com geom_json
    const tectonicGeoFilter    = `ST_DWithin(CASE WHEN ST_SRID(geom)=0 THEN ST_SetSRID(geom,${SRID}) ELSE geom END, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674), 1.0)`;
    const tectonicGeoIntersect = `ST_Intersects(CASE WHEN ST_SRID(geom)=0 THEN ST_SetSRID(geom,${SRID}) ELSE geom END, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))`;
    const tecGeomLinha = `ST_AsGeoJSON(ST_SimplifyPreserveTopology(CASE WHEN ST_SRID(geom)=0 THEN ST_SetSRID(geom,4674) ELSE geom END, 0.01))`;
    const tecGeomPto   = `ST_AsGeoJSON(CASE WHEN ST_SRID(geom)=0 THEN ST_SetSRID(geom,4674) ELSE geom END)`;

    // ── PARALELIZADO: 9 queries tectônicas globais (todas independentes) ──
    console.time('[par] tectonicas');
    const [
      plateBoundaryResult,
      continentStructureResult,
      cratonResult,
      dykesResult,
      eclogitesResult,
      isopachsResult,
      kimberlitesResult,
      paleozResult,
      sutureResult,
    ] = await Promise.all([
      safeQuery(`SELECT id, feature, type as tipo FROM tectonic_map.plate_boundary WHERE ${tectonicGeoFilter}`, [geojsonStr]),
      safeQuery(`SELECT id, type as tipo, name, ${tecGeomLinha} as geom_json FROM tectonic_map.continent_structure WHERE ${tectonicGeoIntersect}`, [geojsonStr]),
      safeQuery(`SELECT id, type as tipo, ${tecGeomLinha} as geom_json FROM tectonic_map.craton_terranes_limits WHERE ${tectonicGeoFilter}`, [geojsonStr]),
      safeQuery(`SELECT id, type as tipo, age, ${tecGeomLinha} as geom_json FROM tectonic_map.dykes WHERE ${tectonicGeoIntersect}`, [geojsonStr]),
      safeQuery(`SELECT id, type as tipo, long_dec as longitude, lat_dec as latitude, ${tecGeomPto} as geom_json FROM tectonic_map.eclogites WHERE ${tectonicGeoFilter}`, [geojsonStr]),
      safeQuery(`SELECT id, type as tipo, depth_base, ${tecGeomLinha} as geom_json FROM tectonic_map.isopachs WHERE ${tectonicGeoIntersect}`, [geojsonStr]),
      safeQuery(`SELECT id, source, age, long_dec as longitude, lat_dec as latitude, ${tecGeomPto} as geom_json FROM tectonic_map.kimberlites WHERE ${tectonicGeoFilter}`, [geojsonStr]),
      safeQuery(`SELECT id, type as tipo, ${tecGeomLinha} as geom_json FROM tectonic_map.paleoz_erosional_border WHERE ${tectonicGeoIntersect}`, [geojsonStr]),
      safeQuery(`SELECT id, type as tipo, obs, ${tecGeomLinha} as geom_json FROM tectonic_map.suture_zones WHERE ${tectonicGeoIntersect}`, [geojsonStr]),
    ]);
    console.timeEnd('[par] tectonicas');
    analyses['9.14_analises_adicionais'].tectonicas.plate_boundary = plateBoundaryResult.rows;
    analyses['9.14_analises_adicionais'].tectonicas.continent_structure = continentStructureResult.rows;
    analyses['9.14_analises_adicionais'].tectonicas.craton_terranes_limits = cratonResult.rows;
    analyses['9.14_analises_adicionais'].tectonicas.dykes = dykesResult.rows;
    analyses['9.14_analises_adicionais'].tectonicas.eclogites = eclogitesResult.rows;
    analyses['9.14_analises_adicionais'].tectonicas.isopachs = isopachsResult.rows;
    analyses['9.14_analises_adicionais'].tectonicas.kimberlites = kimberlitesResult.rows;
    analyses['9.14_analises_adicionais'].tectonicas.paleoz_erosional_border = paleozResult.rows;
    analyses['9.14_analises_adicionais'].tectonicas.suture_zones = sutureResult.rows;

    // 9.15 Pluviometria (CHIRPS 30 anos) + 9.16 Solo (SoilGrids)
    let pluviometria = { pendente: false, erro: 'Coordenadas não disponíveis', media_mensal: null };
    let solo = { pendente: false, erro: 'Coordenadas não disponíveis', camadas: null };
    try {
      const centroidForPluvio = municipio.centroid ? JSON.parse(municipio.centroid) : null;
      if (centroidForPluvio) {
        const pLat = centroidForPluvio.coordinates[1];
        const pLng = centroidForPluvio.coordinates[0];
        const [chirpsResult, soilResult] = await Promise.all([
          fetchPluviometriaCHIRPS(pLat, pLng),
          fetchSoilGrids(pLat, pLng),
        ]);



        pluviometria = {
          pendente: false,
          erro: chirpsResult.erro || null,
                      fonte: chirpsResult.resumo?.fonte || chirpsResult.fonte || null,
                      media_mensal: (chirpsResult.media_mensal || []).map(m => ({ mes: m.mes, mm: m.media_mm ?? m.mm ?? 0 })),
          total_anual: chirpsResult.total_anual || null,
                      media_anual_30anos: chirpsResult.resumo?.media_anual_mm || chirpsResult.media_anual_30anos || null,
        };
        // fix: computa resumo para o frontend
        if (pluviometria && pluviometria.media_mensal && pluviometria.media_mensal.length > 0) {
          const _totais = pluviometria.total_anual || [];
          const _mediaAnual = _totais.length > 0
            ? Math.round(_totais.reduce((s, a) => s + a.total_mm, 0) / _totais.length)
            : (pluviometria.media_anual_30anos || 0);
          const _sorted = [...pluviometria.media_mensal].sort((a, b) => b.media_mm - a.media_mm);
          const _pluvLat = centroidForPluvio ? centroidForPluvio[1] : 0;
          const _pluvLng = centroidForPluvio ? centroidForPluvio[0] : 0;
          pluviometria.resumo = {
            media_anual_mm: _mediaAnual,
            mes_mais_chuvoso: _sorted[0] || null,
            mes_mais_seco:    _sorted[_sorted.length - 1] || null,
            fonte: pluviometria.fonte || 'CHIRPS/ERA5 (1994-2024)',
            latitude:  _pluvLat ? parseFloat(_pluvLat.toFixed(4)) : 0,
            longitude: _pluvLng ? parseFloat(_pluvLng.toFixed(4)) : 0,
          };
        }

          // ─── Override: media anual vem do raster PostGIS (PMA Brasil 1977-2006) ───

          try {

            console.log('[pluvio-override] iniciando, pluviometria_atual=' + (pluviometria ? 'OK' : 'null') + ' geojson_type=' + (typeof geojson));
  const pmaPostgis = await fetchPluviometriaPostGIS(pLat, pLng, geojson);
  console.log('[pluvio-override] pmaPostgis=' + pmaPostgis);

            if (pmaPostgis != null && pluviometria) {

              if (pluviometria.resumo) {

                pluviometria.resumo.media_anual_mm = pmaPostgis;

                pluviometria.resumo.fonte = 'PMA Brasil 1977-2006 (CPRM/ANA) + mensal NASA POWER';

              }

              pluviometria.media_anual_30anos = pmaPostgis;

              pluviometria.fonte = 'PMA Brasil 1977-2006 (CPRM/ANA) + mensal NASA POWER';

            }

          } catch (eOverride) {

            console.warn('[pluvio-override] falhou, mantendo CHIRPS:', eOverride?.message);

          }
        solo = soilResult;
      }
    } catch (e) {
      console.warn('[Pluviometria/Solo] erro:', e.message);
      pluviometria = { pendente: false, erro: 'Erro interno: ' + e.message, media_mensal: null };
    }

        // 9.16 SoilGrids v2.0 - reutiliza fetchSoilGrids (evita 2a chamada ao ISRIC)
        let soilGrids = null;
        if (solo && solo.camadas && solo.camadas['0-5cm']) {
          const d = solo.camadas['0-5cm'];
          soilGrids = {
            clayPercent:   d['argila_%']              ?? null,
            sandPercent:   d['areia_%']               ?? null,
            siltPercent:   d['silte_%']               ?? null,
            ph:            d['ph']                    ?? null,
            organicCarbon: d['carbono_organico_g_kg'] ?? null,
            bulkDensity:   d['densidade_g_cm3']       ?? null,
            source: 'SoilGrids v2.0 (ISRIC)',
            depth: '0-5cm',
          };
          console.log('[SoilGrids] OK (reused):', soilGrids);
        }

    // Mapear analyses para o formato AnaliseResultados esperado pelo frontend
    const centroidParsed = municipio.centroid ? JSON.parse(municipio.centroid) : null;
    const resultados = {
      fundiaria: {
        sigef: analyses['9.1_fundiaria'].sigef,
        snci:  analyses['9.1_fundiaria'].snci,
        car:   analyses['9.13_car'].area_imovel,
      },
      registral:            { encontrado: analyses['9.2_registral'].serventias.length > 0, cartorios: analyses['9.2_registral'].serventias },
      solo:                 analyses['9.3_solo'].data,
      bioma:                analyses['9.4_bioma'].data,
      geologia:             analyses['9.5_geologia'].data,
      geologia_estruturas: {
        pontos:      analyses['9.5_geologia'].pontos,
        ocorrencias: analyses['9.5_geologia'].ocorrencias,
        dobras:      analyses['9.5_geologia'].dobras,
        falhas:      analyses['9.5_geologia'].falhas,
        fraturas:    analyses['9.5_geologia'].fraturas,
      },
      // Chaves flat para o mapa SVG do frontend (geoloMapaSvg getter)
      geol_ponto:              analyses['9.5_geologia'].pontos,
      ocorrencias_br:          analyses['9.5_geologia'].ocorrencias,
      geol_linha_dobra:        analyses['9.5_geologia'].dobras,
      geol_linha_falha:        analyses['9.5_geologia'].falhas,
      geol_linha_fratura:      analyses['9.5_geologia'].fraturas,
      continent_structure:     analyses['9.14_analises_adicionais'].tectonicas.continent_structure,
      craton_terranes_limits:  analyses['9.14_analises_adicionais'].tectonicas.craton_terranes_limits,
      dykes:                   analyses['9.14_analises_adicionais'].tectonicas.dykes,
      eclogites:               analyses['9.14_analises_adicionais'].tectonicas.eclogites,
      isopachs:                analyses['9.14_analises_adicionais'].tectonicas.isopachs,
      kimberlites:             analyses['9.14_analises_adicionais'].tectonicas.kimberlites,
      paleoz_erosional_border: analyses['9.14_analises_adicionais'].tectonicas.paleoz_erosional_border,
      suture_zones:            analyses['9.14_analises_adicionais'].tectonicas.suture_zones,
      mineracao:            { processos_anm: analyses['9.6_mineracao'].processes, ocorrencias: analyses['9.6_mineracao'].occurrences },
      embargos:             analyses['9.7_embargos'].data,
      terras_indigenas:     analyses['9.8_terras_indigenas'].data,
      unidades_conservacao: analyses['9.9_ucs'].data,
      hidrografia: {
        bacias:                   analyses['9.10_hidrografia'].bacias,
        cursos_dagua_count:       analyses['9.10_hidrografia'].cursos_agua_count,
        intensidade_hidrica:      analyses['9.10_hidrografia'].rica_em_agua ? 'Alta' : 'Normal',
        padrao_drenagem:          analyses['9.10_hidrografia'].padrao_drenagem,
        comprimento_influencia_km: analyses['9.10_hidrografia'].comprimento_influencia_km || 0,
        nomes_rios:               analyses['9.10_hidrografia'].nomes_rios || [],
        rios_geom:                analyses['9.10_hidrografia'].rios_geom || [],
      },
      altitude: {
        altitude_min:    analyses['9.11_altitude'].min_m,
        altitude_max:    analyses['9.11_altitude'].max_m,
        altitude_media:  analyses['9.11_altitude'].media_m,
        altitude_ponto:  analyses['9.11_altitude'].ponto_m,
        altitude_grid:   analyses['9.11_altitude'].grid,
      },
      carbono: {
        estoque_total_toneladas: analyses['9.12_carbono'].total_toneladas,
        medio_t_ha: analyses['9.12_carbono'].media_t_ha, min_t_ha: analyses['9.12_carbono'].min_t_ha, max_t_ha: analyses['9.12_carbono'].max_t_ha,
      },
      car: {
        imoveis:                  analyses['9.13_car'].area_imovel,
        area_app_ha:              analyses['9.13_car'].app_area_hectares,
        app_detalhes:             [],
        area_reserva_legal_ha:    analyses['9.13_car'].reserva_legal_hectares,
        area_vegetacao_nativa_ha: analyses['9.13_car'].vegetacao_nativa_hectares,
        area_consolidada_ha:      analyses['9.13_car'].area_consolidada_hectares,
        reserva_proposta_ha:      0,
        veg_nativa_proposta_ha:   0,
      },
      silos: analyses['9.13e_silos'] || null,
      logistica: analyses['9.13f_logistica'] || null,
      hidrologia_pivos: analyses['9.15_hidrologia_pivos'] || null,
      prodes_deter: {
        prodes:                   analyses['9.13c_prodes_deter']?.prodes || [],
        deter:                    analyses['9.13c_prodes_deter']?.deter || [],
        prodes_area_total_ha:     analyses['9.13c_prodes_deter']?.prodes_area_total_ha || 0,
        deter_area_total_ha:      analyses['9.13c_prodes_deter']?.deter_area_total_ha || 0,
        fonte:                    analyses['9.13c_prodes_deter']?.fonte || 'INPE TerraBrasilis',
        debug:                    analyses['9.13c_prodes_deter']?.debug || {},
      },
      conformidade: analyses['9.13d_conformidade'] || null,
      aquiferos: analyses['9.13b_aquiferos'].data,
      analises_adicionais: analyses['9.14_analises_adicionais'],
      pluviometria,
      soilgrids: soilGrids,
      solo_soilgrids: solo,
      municipio:  { NM_MUN: municipio.municipio, SIGLA_UF: municipio.uf, CD_MUN: '' },
      municipios: [{ nm_mun: municipio.municipio, sigla_uf: municipio.uf }],
            localizacao_municipios_proximos: analyses['localizacao_municipios_proximos'] || [],
      centroide:  centroidParsed ? { lat: centroidParsed.coordinates[1], lng: centroidParsed.coordinates[0] } : { lat: 0, lng: 0 },
      area_total_ha: parseFloat(municipio.area_hectares) || 0,
      parcel_geojson: JSON.parse(geojsonStr),  // geometria da parcela analisada — usada para overlay nos mapas SVG
    };

    const _resp = { sucesso: true, gerado_em: new Date().toISOString(), resultados };
    _cacheSet(_ck, _resp);
    res.json(_resp);
  } catch (error) {
    console.error('Error in /api/analises:', error);
    res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/camadas/:camada
 * Viewport-based GeoJSON loading with ST_DWithin radius filter
 * ST_Simplify for performance, LIMIT 500
 */
app.get('/api/camadas/:camada', async (req, res) => {
  try {
    const { camada } = req.params;
    const { bbox, lat, lng, raio, zoom: zoomStr } = req.query;
    const zoom = parseFloat(zoomStr) || 10;

    let minLng, minLat, maxLng, maxLat;

    if (bbox) {
      // Parse bbox: minLng,minLat,maxLng,maxLat
      [minLng, minLat, maxLng, maxLat] = bbox.split(',').map(Number);
    } else if (lat && lng) {
      // Compute bbox from lat/lng/raio (raio in km, default 50km)
      const raioKm = parseFloat(raio) || 50;
      const deltaLat = raioKm / 111.0;
      const deltaLng = raioKm / (111.0 * Math.cos(parseFloat(lat) * Math.PI / 180));
      minLat = parseFloat(lat) - deltaLat;
      maxLat = parseFloat(lat) + deltaLat;
      minLng = parseFloat(lng) - deltaLng;
      maxLng = parseFloat(lng) + deltaLng;
    } else {
      return res.status(400).json({ error: 'bbox or lat/lng query parameters required' });
    }

    const bboxWkt = `SRID=${SRID};POLYGON((${minLng} ${minLat}, ${maxLng} ${minLat}, ${maxLng} ${maxLat}, ${minLng} ${maxLat}, ${minLng} ${minLat}))`;
    const geomExpr = `CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom, ${SRID}) ELSE geom END`;

    // Zoom-based tuning — keeps original ST_Intersects/bboxWkt intact (safe for SRID=0 tables)
    const simplifyTol   = zoom >= 14 ? 0.00005 : zoom >= 12 ? 0.0003 : zoom >= 10 ? 0.001 : zoom >= 8 ? 0.004 : 0.01;
    // CAR tem mais parcelas que SIGEF/SNCI numa mesma área → carrega 1.6x a mais
    const isCAR = camada === 'car';
    const ratioCAR = isCAR ? 1.6 : 1;
    const limitPerState = Math.round((zoom >= 12 ? 150 : zoom >= 10 ? 80 : zoom >= 8 ? 40 : 20) * ratioCAR);
    const limitTotal    = Math.round((zoom >= 12 ? 400 : zoom >= 10 ? 200 : zoom >= 8 ? 100 : 60) * ratioCAR);

    // State-partitioned layers: query ALL states that intersect the bbox (UNION ALL)
    const stateLayerConfig = {
      sigef: { schema: 'incra', prefix: 'sigef',       idCol: 'gid', labelCol: 'parcela_co' },
      snci:  { schema: 'incra', prefix: 'snci',        idCol: 'gid', labelCol: 'num_proces' },
      car:   { schema: 'car',   prefix: 'area_imovel', idCol: 'gid', labelCol: 'cod_imovel',
               // Excluir imóveis CAR cancelados administrativamente
               filter: "des_condic IS DISTINCT FROM 'Cancelado por decisao administrativa' AND ind_status IS DISTINCT FROM 'CA'" },
    };

    // Static (non-partitioned) layers
    const staticLayerConfig = {
      bioma:    { table: 'bioma.bioma_250',                    idCol: 'id', labelCol: '"Bioma"' },
      tis:      { table: 'terra_indigena.poligonais_portarias',      idCol: 'id', labelCol: 'terrai_nome' },
      ucs:      { table: 'unidade_conservacao.unidade_conserv', idCol: 'id', labelCol: '"NOME_UC1"' },
      embargos: { table: 'embargos.embargos_ibama',            idCol: 'id', labelCol: 'num_auto_i' },
    };

    let query;
    let idCol, labelCol;

    if (stateLayerConfig[camada]) {
      // MULTI-STATE: find all UFs overlapping bbox, query each table in parallel
      const cfg = stateLayerConfig[camada];
      idCol = cfg.idCol;
      labelCol = cfg.labelCol;

      const ufs = getUFsFromBbox(minLng, minLat, maxLng, maxLat);

      // Centro do viewport para ordenar do centro para fora
      const centerLng = (minLng + maxLng) / 2;
      const centerLat = (minLat + maxLat) / 2;

      // Query each state table safely (tables without data return empty array)
      const extraFilter = cfg.filter ? `AND ${cfg.filter}` : '';

      // Para camada 'sigef': consulta SIGEF + SNCI juntas (ambas compõem a camada fundiária)
      let queryTargets;
      if (camada === 'sigef') {
        const sigefCfg = stateLayerConfig['sigef'];
        const snciCfg  = stateLayerConfig['snci'];
        queryTargets = ufs.flatMap(u => [
          safeQuery(
            `SELECT ${sigefCfg.idCol}, ${sigefCfg.labelCol} as label, 'sigef' as source,
               ST_AsGeoJSON(ST_Simplify(${geomExpr}, ${simplifyTol})) as geometry,
               ST_Distance(ST_Centroid(${geomExpr}), ST_SetSRID(ST_MakePoint($2, $3), ${SRID})) as dist_center
             FROM ${sigefCfg.schema}.${sigefCfg.prefix}_${u}
             WHERE ${geomExpr} && ST_GeomFromText($1) AND ST_Intersects(${geomExpr}, ST_GeomFromText($1))
             ORDER BY dist_center ASC LIMIT ${limitPerState}`,
            [bboxWkt, centerLng, centerLat]
          ),
          safeQuery(
            `SELECT ${snciCfg.idCol}, ${snciCfg.labelCol} as label, 'snci' as source,
               ST_AsGeoJSON(ST_Simplify(${geomExpr}, ${simplifyTol})) as geometry,
               ST_Distance(ST_Centroid(${geomExpr}), ST_SetSRID(ST_MakePoint($2, $3), ${SRID})) as dist_center
             FROM ${snciCfg.schema}.${snciCfg.prefix}_${u}
             WHERE ${geomExpr} && ST_GeomFromText($1) AND ST_Intersects(${geomExpr}, ST_GeomFromText($1))
             ORDER BY dist_center ASC LIMIT ${limitPerState}`,
            [bboxWkt, centerLng, centerLat]
          ),
        ]);
      } else {
        queryTargets = ufs.map(u => safeQuery(
          `SELECT ${idCol}, ${labelCol} as label,
             ST_AsGeoJSON(ST_Simplify(${geomExpr}, ${simplifyTol})) as geometry,
             ST_Distance(
               ST_Centroid(${geomExpr}),
               ST_SetSRID(ST_MakePoint($2, $3), ${SRID})
             ) as dist_center
           FROM ${cfg.schema}.${cfg.prefix}_${u}
           WHERE ${geomExpr} && ST_GeomFromText($1) AND ST_Intersects(${geomExpr}, ST_GeomFromText($1))
             ${extraFilter}
           ORDER BY dist_center ASC
           LIMIT ${limitPerState}`,
          [bboxWkt, centerLng, centerLat]
        ));
      }

      const perStateResults = await Promise.all(queryTargets);

      // Merge todos os estados, ordena globalmente do centro para fora
      const allRows = perStateResults
        .flatMap(r => r.rows)
        .sort((a, b) => a.dist_center - b.dist_center)
        .slice(0, limitTotal);
      const features = allRows
        .filter(row => row.geometry != null)
        .map(row => ({
          type: 'Feature',
          id: `${row.source || camada}_${row[idCol]}`,
          properties: { label: row.label, source: row.source || camada },
          geometry: JSON.parse(row.geometry),
        }));

      return res.json({ type: 'FeatureCollection', features });

    } else if (staticLayerConfig[camada]) {
      // STATIC: single table, existing logic
      const cfg = staticLayerConfig[camada];
      idCol = cfg.idCol;
      labelCol = cfg.labelCol;

      query = `
        SELECT ${idCol}, ${labelCol} as label,
          ST_AsGeoJSON(ST_Simplify(${geomExpr}, 0.001)) as geometry
        FROM ${cfg.table}
        WHERE ${geomExpr} && ST_GeomFromText('${bboxWkt}') AND ST_Intersects(${geomExpr}, ST_GeomFromText('${bboxWkt}'))
        LIMIT 500
      `;
    } else {
      return res.status(400).json({ error: 'Unknown camada: ' + camada });
    }

    let result = await pool.query(query);

    // Convert to GeoJSON FeatureCollection
    const features = result.rows
      .filter(row => row.geometry != null)
      .map(row => ({
        type: 'Feature',
        id: row[idCol],
        properties: {
          label: row.label,
        },
        geometry: JSON.parse(row.geometry),
      }));

    res.json({
      type: 'FeatureCollection',
      features: features,
    });
  } catch (error) {
    console.error('Error in /api/camadas/:camada:', error);
    res.status(500).json({ error: error.message });
  }
});

/**
 * POST /api/exportar
 * Export as GeoJSON or KML
 */
/**
 * POST /api/analise-ia
 * Gera laudo técnico via Claude Sonnet usando TODOS os resultados das análises.
 * Body: { resultados: <objeto completo retornado por /api/analises> }
 * Response: { laudo: "<texto markdown>" }
 */
let _anthropicClient = null;
function _getAnthropic() {
  if (_anthropicClient) return _anthropicClient;
  if (!process.env.ANTHROPIC_API_KEY) return null;
  const Anthropic = require('@anthropic-ai/sdk').default || require('@anthropic-ai/sdk');
  _anthropicClient = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
  return _anthropicClient;
}

function _summarizeResultadosForIA(r) {
  // Compacta o payload da análise em texto estruturado pra IA (geólogo prospector) digerir
  const lines = [];
  const fmt = (n, dec=2) => n == null ? '—' : Number(n).toLocaleString('pt-BR', { minimumFractionDigits: dec, maximumFractionDigits: dec });

  // 1. Localização da parcela
  if (r?.municipio?.NM_MUN) lines.push(`Município: ${r.municipio.NM_MUN}/${r.municipio.SIGLA_UF || ''}`);
  if (r?.area_total_ha) lines.push(`Área total selecionada: ${fmt(r.area_total_ha)} ha`);
  if (r?.centroide) lines.push(`Centroide: ${r.centroide.lat?.toFixed(6)}, ${r.centroide.lng?.toFixed(6)}`);

  // 2. Fundiária (resumido — não é foco do laudo geológico)
  const sigef = r?.fundiaria?.sigef || [];
  if (sigef.length) {
    lines.push(`\n## FUNDIÁRIA (SIGEF/SNCI/CAR)`);
    lines.push(`SIGEF: ${sigef.length} parcela(s)`);
    sigef.forEach(s => lines.push(`  - ${s.nome_area || s.parcela_co}: ${fmt(s.area_hectares)} ha, situação ${s.situacao_i || '?'}, certif. ${s.data_aprov || '?'}`));
  }
  const snci = r?.fundiaria?.snci || [];
  if (snci.length) {
    lines.push(`SNCI: ${snci.length} imóvel(is)`);
    snci.forEach(s => lines.push(`  - ${s.nome_imove || s.cod_imovel}: ${fmt(s.qtd_area_p || s.area_hectares)} ha`));
  }
  const car = r?.fundiaria?.car || [];
  if (car.length) {
    lines.push(`CAR sobreposto: ${car.length} imóvel(is)`);
    car.forEach(c => {
      const intersec = c.area_intersecao_ha != null ? ` (${fmt(c.area_intersecao_ha)} ha sobrepostos)` : '';
      lines.push(`  - ${c.cod_imovel}: ${fmt(c.area_hectares)} ha total${intersec}, status ${c.ind_status || '?'}, ${c.des_condic || '?'}`);
    });
  }

  // 3. Reserva Legal Proposta / Vegetação Nativa / Área Consolidada
  const carArea = r?.car_area || r?.['9.13_car'] || {};
  if (carArea.reserva_legal_hectares || carArea.app_area_hectares || carArea.vegetacao_nativa_hectares || carArea.area_consolidada_hectares) {
    lines.push(`\n## ÁREAS CAR INSCRITAS`);
    if (carArea.reserva_legal_hectares) lines.push(`Reserva Legal Proposta: ${fmt(carArea.reserva_legal_hectares)} ha`);
    if (carArea.vegetacao_nativa_hectares) lines.push(`Vegetação Nativa: ${fmt(carArea.vegetacao_nativa_hectares)} ha`);
    if (carArea.area_consolidada_hectares) lines.push(`Área Consolidada: ${fmt(carArea.area_consolidada_hectares)} ha`);
    if (carArea.app_area_hectares) lines.push(`APP: ${fmt(carArea.app_area_hectares)} ha`);
  }

  // 4. Conformidade Ambiental
  if (r?.conformidade) {
    const c = r.conformidade;
    lines.push(`\n## CONFORMIDADE AMBIENTAL`);
    if (c.bioma) lines.push(`Bioma: ${c.bioma}`);
    if (c.rl_minima_pct) lines.push(`RL mínima legal: ${c.rl_minima_pct}%`);
    if (c.rl_declarada_ha) lines.push(`RL declarada: ${fmt(c.rl_declarada_ha)} ha`);
    if (c.flags?.length) {
      lines.push(`Flags:`);
      c.flags.forEach(f => lines.push(`  - [${f.severidade}] ${f.titulo}: ${f.detalhe}`));
    }
  }

  // 5. PRODES/DETER (desmatamento)
  if (r?.prodes_deter) {
    const pd = r.prodes_deter;
    lines.push(`\n## DESMATAMENTO (INPE PRODES/DETER)`);
    if (pd.prodes_total_ha != null) lines.push(`PRODES acumulado: ${fmt(pd.prodes_total_ha)} ha`);
    if (pd.deter_total_ha != null) lines.push(`DETER alertas recentes: ${fmt(pd.deter_total_ha)} ha`);
    const prodes = pd.prodes || [];
    if (prodes.length) {
      lines.push(`PRODES por ano (top 10):`);
      prodes.slice(0, 10).forEach(p => lines.push(`  - ${p.ano || p.year}: ${fmt(p.area_ha)} ha (${p.classe || p.class || 'desmatamento'})`));
    }
    const deter = pd.deter || [];
    if (deter.length) {
      lines.push(`DETER alertas (top 5):`);
      deter.slice(0, 5).forEach(d => lines.push(`  - ${d.data_alerta || d.date}: ${fmt(d.area_ha)} ha, classe ${d.classe || d.class || '?'}`));
    }
  }

  // 6. GEOLOGIA — unidades litoestratigráficas
  if (r?.geologia?.length) {
    lines.push(`\n## CONTEXTO GEOLÓGICO`);
    r.geologia.forEach(g => {
      const pct = g.percentual_sobreposicao != null ? ` (${fmt(g.percentual_sobreposicao)}%)` : '';
      const grupo = g.grupo ? `, grupo ${g.grupo}` : '';
      const litotipo = g.litotipo ? `, litotipo: ${g.litotipo}` : '';
      lines.push(`  - ${g.nome_unidade || g.nome}${pct}, era ${g.era || '?'}${grupo}${litotipo}`);
      if (g.descricao) lines.push(`    descrição: ${g.descricao}`);
    });
  }

  // 7. ESTRUTURAS GEOLÓGICAS — falhas, lineamentos, afloramentos (críticos pra prospecção)
  const ge = r?.geologia_estruturas || r?.['9.14_analises_adicionais']?.geologia || {};
  if (ge?.linhas_falha?.length || ge?.falhas?.length) {
    const falhas = ge.linhas_falha || ge.falhas || [];
    lines.push(`\n## FALHAS / LINEAMENTOS ESTRUTURAIS`);
    lines.push(`Total: ${falhas.length} estrutura(s)`);
    falhas.slice(0, 20).forEach((f, i) => {
      lines.push(`  - F${i+1}: classif=${f.classif || '?'}, forma=${f.forma || '?'}, sentido=${f.sentido || '?'}, mergulho est.=${f.estm_merg || '?'}`);
    });
  }
  if (ge?.pontos?.length || ge?.afloramentos?.length) {
    const pts = ge.pontos || ge.afloramentos || [];
    lines.push(`\n## AFLORAMENTOS / PONTOS GEOLÓGICOS`);
    lines.push(`Total: ${pts.length} ponto(s)`);
    pts.slice(0, 15).forEach((p, i) => lines.push(`  - P${i+1}: tipo=${p.tipo || p.classif || '?'}, litologia=${p.litologia || p.rocha || '?'}`));
  }
  if (ge?.ocorrencias?.length || r?.mineracao?.ocorrencias?.length) {
    const ocr = ge.ocorrencias || r.mineracao.ocorrencias || [];
    lines.push(`\n## OCORRÊNCIAS MINERAIS REGIONAIS (CPRM)`);
    ocr.slice(0, 15).forEach((o, i) => {
      const subs = o.substancias || o.substancia || '?';
      const rochas = o.rochas_hospedeiras || o.rocha_hospedeira || '?';
      lines.push(`  - O${i+1}: substância(s)=${subs}, rocha(s) hospedeira(s)=${rochas}`);
    });
  }

  // 8. SOLO — indicador de intemperismo / mineralização superficial
  if (r?.solo?.length) {
    lines.push(`\n## SOLOS (EMBRAPA SiBCS — indicadores pedogeoquímicos)`);
    r.solo.forEach(s => {
      const pct = s.percentual_sobreposicao != null ? ` (${fmt(s.percentual_sobreposicao)}%)` : '';
      lines.push(`  - ${s.nome}${pct}, sigla ${s.sigla || '?'}, relevo ${s.relevo || s.declividade || '?'}`);
    });
  }

  // 8. HIDROGRAFIA & PADRÃO DE DRENAGEM — pista forte sobre litologia/controle estrutural
  if (r?.hidrografia) {
    lines.push(`\n## HIDROGRAFIA / PADRÃO DE DRENAGEM`);
    const h = r.hidrografia;
    if (h.cursos_dagua_count != null) lines.push(`Cursos d'água: ${h.cursos_dagua_count}`);
    if (h.padrao_drenagem) {
      const pd = h.padrao_drenagem;
      if (pd.padrao || pd.tipo) lines.push(`Padrão de drenagem identificado: **${pd.padrao || pd.tipo}**`);
      if (pd.descricao) lines.push(`Interpretação geológica: ${pd.descricao}`);
      if (pd.total_cursos) lines.push(`Total cursos analisados: ${pd.total_cursos}`);
      if (pd.ordem_maxima) lines.push(`Ordem máxima Strahler: ${pd.ordem_maxima}`);
    }
    if (h.massas_dagua_count != null) lines.push(`Massas d'água: ${h.massas_dagua_count}`);
  }

  // 9. Pluviometria
  const pma = r?.pluviometria?.media_anual_30anos ?? r?.pluviometria?.resumo?.media_anual_mm;
  if (pma != null) lines.push(`\n## CLIMA\nPluviometria média anual (30 anos): ${fmt(pma, 0)} mm`);

  // 10. Altimetria
  if (r?.altitude) {
    const a = r.altitude;
    lines.push(`\n## ALTIMETRIA`);
    if (a.min_m != null) lines.push(`Cota mínima: ${fmt(a.min_m, 0)} m`);
    if (a.max_m != null) lines.push(`Cota máxima: ${fmt(a.max_m, 0)} m`);
    if (a.mean_m != null) lines.push(`Cota média: ${fmt(a.mean_m, 0)} m`);
    if (a.declividade_pct_media != null) lines.push(`Declividade média: ${fmt(a.declividade_pct_media)}%`);
  }

  // 10b. AQUÍFEROS — potencial hidrogeológico e indicador estratigráfico
  const aq = r?.aquiferos || r?.['9.13b_aquiferos'];
  if (aq) {
    const poroso = aq.poroso || [];
    const fraturado = aq.fraturado || [];
    const carstico = aq.carstico || [];
    if (poroso.length || fraturado.length || carstico.length) {
      lines.push(`\n## AQUÍFEROS`);
      if (poroso.length) lines.push(`Aquíferos POROSOS (${poroso.length}): ${poroso.map(a => a.nome || a.codigo).filter(Boolean).join(', ')}`);
      if (fraturado.length) lines.push(`Aquíferos FRATURADOS (${fraturado.length}): ${fraturado.map(a => a.nome || a.codigo).filter(Boolean).join(', ')}`);
      if (carstico.length) lines.push(`Aquíferos CÁRSTICOS (${carstico.length}): ${carstico.map(a => a.nome || a.codigo).filter(Boolean).join(', ')}`);
    }
  }

  // 11. UCs e Terras Indígenas
  if (r?.unidades_conservacao?.length) {
    lines.push(`\n## UCs SOBREPOSTAS\n${r.unidades_conservacao.map(u => `  - ${u.nome} (${u.categoria})`).join('\n')}`);
  }
  if (r?.terras_indigenas?.length) {
    lines.push(`\n## TERRAS INDÍGENAS\n${r.terras_indigenas.map(t => `  - ${t.terrai_nom || t.nome} (${t.fase_ti || ''})`).join('\n')}`);
  }

  // 12. REQUERIMENTOS MINERÁRIOS (ANM) — diagnóstico prospectivo
  if (r?.mineracao?.processos?.length) {
    lines.push(`\n## REQUERIMENTOS / PROCESSOS MINERÁRIOS (ANM)`);
    lines.push(`Total: ${r.mineracao.processos.length} processo(s) sobreposto(s) ou vizinho(s)`);
    // Agrupa por substância para ver padrão regional
    const porSubst = {};
    r.mineracao.processos.forEach(m => {
      const k = (m.substancia || m.substância || '?').toString();
      porSubst[k] = (porSubst[k] || 0) + 1;
    });
    lines.push(`Substâncias requeridas (frequência):`);
    Object.entries(porSubst).sort((a,b) => b[1]-a[1]).forEach(([sub, n]) => lines.push(`  - ${sub}: ${n} processo(s)`));
    lines.push(`Top processos:`);
    r.mineracao.processos.slice(0, 10).forEach(m => {
      const titular = m.titular || m.nome_titular || '';
      lines.push(`  - ${m.processo || m.numero}: ${m.substancia || '?'}, fase ${m.fase || '?'}${titular ? ` (titular: ${titular})` : ''}`);
    });
  }

  // 12b. LOGÍSTICA — cidades, portos, terminais ferroviários, portos secos
  const lg = r?.logistica;
  if (lg) {
    const cid = lg.cidades_proximas || [];
    const pm = lg.portos_maritimos || [];
    const pf = lg.portos_fluviais || [];
    const ps = lg.portos_secos || [];
    const tf = lg.terminais_ferroviarios || [];
    if (cid.length || pm.length || pf.length || ps.length || tf.length) {
      lines.push(`\n## LOGÍSTICA E ESCOAMENTO`);
      if (cid.length) {
        lines.push(`Principais cidades próximas (top 10, distância geodésica):`);
        cid.slice(0, 10).forEach(c => lines.push(`  - ${c.nome}/${c.uf}: ${fmt(c.distancia_km, 1)} km`));
      }
      if (pm.length) {
        lines.push(`Portos marítimos próximos (top 5):`);
        pm.forEach(p => lines.push(`  - ${p.nome} (${p.municipio}/${p.uf}): ${fmt(p.distancia_km, 1)} km`));
      }
      if (pf.length) {
        lines.push(`Portos fluviais próximos (top 5):`);
        pf.forEach(p => lines.push(`  - ${p.nome} (${p.municipio}/${p.uf}): ${fmt(p.distancia_km, 1)} km`));
      }
      if (ps.length) {
        lines.push(`Portos secos / EADIs próximos (top 5):`);
        ps.forEach(p => lines.push(`  - ${p.nome} (${p.municipio}/${p.uf}): ${fmt(p.distancia_km, 1)} km`));
      }
      if (tf.length) {
        lines.push(`Terminais ferroviários próximos (top 5):`);
        tf.forEach(t => lines.push(`  - ${t.nome} (${t.municipio}/${t.uf}): ${fmt(t.distancia_km, 1)} km`));
      }
      const rc = lg.rodovias_que_cruzam || [];
      const rp = lg.rodovias_proximas || [];
      if (rc.length) {
        lines.push(`Rodovias que CRUZAM a parcela:`);
        rc.forEach(r => lines.push(`  - ${r.codigo} (${r.nome}) — ${r.jurisdicao}, ${r.pavimentacao}`));
      }
      if (rp.length) {
        lines.push(`Rodovias próximas (≤25 km, ordenadas por distância):`);
        rp.forEach(r => lines.push(`  - ${r.codigo} (${r.nome}): ${fmt(r.distancia_km, 1)} km — ${r.jurisdicao}, ${r.pavimentacao}`));
      }
    }
  }

  // 12c. APTIDÃO HIDROLÓGICA PARA PIVÔS CENTRAIS
  const hp = r?.hidrologia_pivos;
  if (hp) {
    lines.push(`\n## APTIDÃO HIDROLÓGICA PARA PIVÔS CENTRAIS`);
    lines.push(`Índice de Aptidão Pivotal (IAP): **${hp.iap_score}/100** — classificação: ${(hp.iap_classificacao||'').toUpperCase()}`);
    if (hp.iap_componentes) {
      Object.entries(hp.iap_componentes).forEach(([k, v]) => lines.push(`  - ${k}: score ${v.score}/100 (peso ${v.peso}%)`));
    }
    if (hp.configuracoes_sugeridas?.length) {
      const c = hp.configuracoes_sugeridas[0];
      lines.push(`Configuração sugerida: ${c.quantidade} pivô(s), raio ${c.raio_m}m, área irrigada ${c.area_total_ha}ha (${c.cobertura_pct}% da parcela)`);
    }
    if (hp.demanda_hidrica) {
      const d = hp.demanda_hidrica;
      lines.push(`Demanda hídrica: ETo=${d.eto_diario_mm}mm/dia, consumo ${d.consumo_diario_m3}m³/dia, vazão necessária ${d.vazao_necessaria_m3h}m³/h (${d.vazao_necessaria_Ls}L/s)`);
      lines.push(`Déficit hídrico anual: ${d.deficit_anual_mm}mm (bioma ${d.bioma})`);
    }
    if (hp.fontes_agua?.length) {
      lines.push(`Fontes de água:`);
      hp.fontes_agua.forEach(f => lines.push(`  - ${f.tipo}: ${f.nome || '?'} — ${f.recomendacao}`));
    }
    if (hp.restricoes?.length) {
      lines.push(`Restrições:`);
      hp.restricoes.forEach(rr => lines.push(`  - [${rr.severidade}] ${rr.texto}`));
    }
    if (hp.aptidao_culturas?.length) {
      lines.push(`Aptidão por cultura:`);
      hp.aptidao_culturas.forEach(c => lines.push(`  - ${c.cultura}: ${c.aptidao} — ${c.observacoes}`));
    }
    if (hp.custos_estimados) {
      const cc = hp.custos_estimados;
      lines.push(`Custo estimado: CAPEX R$ ${fmt(cc.capex_total_estimado, 0)}, OPEX/ano R$ ${fmt(cc.opex_anual_estimado, 0)}, payback ${cc.prazo_payback_anos} anos`);
    }
  }

  // 13. Silos
  if (r?.silos) {
    lines.push(`\n## SILOS PRÓXIMOS (CONAB SICARM, raio ${r.silos.raio_km || 50}km)`);
    lines.push(`Total silos: ${r.silos.total_silos || 0}, capacidade total: ${fmt(r.silos.capacidade_total_t, 0)} t`);
  }

  return lines.join('\n');
}

app.post('/api/analise-ia', async (req, res) => {
  try {
    const client = _getAnthropic();
    if (!client) {
      return res.status(503).json({ error: 'ANTHROPIC_API_KEY não configurada no servidor' });
    }
    const { resultados } = req.body || {};
    if (!resultados || typeof resultados !== 'object') {
      return res.status(400).json({ error: 'body precisa de { resultados }' });
    }

    const dadosCompactos = _summarizeResultadosForIA(resultados);
    const t0 = Date.now();

    const systemPrompt = `Você é um GEÓLOGO BRASILEIRO especialista em PROSPECÇÃO MINERAL, com pós-graduação em geologia econômica e mais de 15 anos de experiência cruzando dados regionais (CPRM, ANM, IBGE, EMBRAPA) para identificar áreas favoráveis à mineralização. Analise os dados georreferenciados fornecidos sobre uma parcela rural brasileira e produza um LAUDO GEOLÓGICO E PROSPECTIVO em português brasileiro, estruturado em seções com cabeçalhos markdown (##).

Cubra OBRIGATORIAMENTE estas seções (quando houver dados disponíveis):

1. **Contexto Geológico Regional**: identifique grupos, formações, era geotectônica, ambiente deposicional. Cite litotipos dominantes e processos formadores (vulcanismo, sedimentação, metamorfismo).

2. **Estruturas, Falhas e Lineamentos**: avalie controle tectônico. Falhas de empurrão, transcorrentes, normais, fraturamento — todos servem como condutos para fluidos hidrotermais e podem hospedar veios mineralizados (ouro, sulfetos polimetálicos). Cite direção predominante e relação com mineralizações regionais.

3. **Pontos de Afloramento**: descreva o que cada afloramento revela (litologia exposta, alteração hidrotermal, indicadores de mineralização — gossan, silicificação, sericitização).

4. **Padrão de Drenagem como Indicador Litoestrutural**: interprete o padrão (dendrítico = rocha homogênea; retangular/treliçado = controle por falhas; radial = domo/intrusão; anelar = caldeira ou diápiro). Padrões anômalos indicam estruturas profundas ainda não mapeadas.

5. **Assinatura Pedológica e Geoquímica**: solos como mapa de produto de intemperismo. Lateritas → bauxita/ferro/manganês/níquel; latossolos vermelhos → ferro residual; podzólicos → mobilização de elementos solúveis. Avalie a "vocação geoquímica" do solo.

6. **Hidrogeologia (Aquíferos)**: aquíferos fraturados sugerem controle estrutural com potencial mineralizador; cársticos indicam carbonatos com possibilidade de Zn-Pb tipo MVT; porosos arenitos podem hospedar urânio.

7. **Topografia e Geomorfologia**: avalie variação de cota, declividade, formas de relevo (cristas, escarpas) como pistas de litologias resistentes / falhamento / dobramento.

8. **Requerimentos Minerários (ANM) e Histórico Regional**: examine processos minerários sobrepostos OU vizinhos como evidência indireta de mineralização. Frequência de cada substância indica TIPOS de depósito ativos na região. Ocorrências CPRM corroboram modelo.

9. **Modelo Conceitual de Prospecção (MCP)**: com base no cruzamento de TODAS as evidências acima, proponha qual TIPO DE DEPÓSITO é provável aqui (orogenic gold, VMS, IOCG, MVT, BIF, placer aluvionar, laterítico, kimberlítico, IGRP, granito-relacionado, etc.). Cite analogias com depósitos brasileiros conhecidos (Carajás, Quadrilátero Ferrífero, Crixás, Bom Futuro, etc.).

10. **Substâncias Minerais Prováveis (ranking)**: liste em ORDEM DE PROBABILIDADE 3 a 5 substâncias mais prováveis na parcela, com a evidência que sustenta cada uma. Ex: "Ouro (alta) — falhas transcorrentes NE-SW em greenstone arqueano + processos ANM vizinhos de Au".

11. **Acesso e Rota Sugerida**: descreva em texto narrativo como chegar à parcela partindo da cidade-sede mais próxima. Cite as principais rodovias federais (BR-XXX) e estaduais relevantes (mesmo as não detectadas automaticamente, use seu conhecimento sobre a malha rodoviária brasileira). Indique direção cardinal (NE/SO/etc.) e distância aproximada. Mencione pontos de referência (postos, entroncamentos, balsas se houver) e condições típicas da via (asfaltada/de chão/sazonal). Se a parcela for em região remota (Amazônia, sertão), avise sobre limitações (período chuvoso, balsas suspensas, falta de combustível).

12. **Alvos de Investigação e Próximas Etapas**: recomende ações práticas:
   - **Geoquímica de solo**: malha de amostragem (espaçamento, profundidade, elementos analíticos)
   - **Geofísica**: magnetometria, gamaespectrometria, IP/resistividade — qual técnica indicada pra qual alvo
   - **Mapeamento de detalhe**: escala 1:25.000 ou 1:10.000, focos prioritários
   - **Sondagem**: alvos preliminares (se houver justificativa)
   - **Verificação cartorária ANM**: requerimento de área livre, oposição de terceiros

Use linguagem técnica de geólogo prospector. Cite valores numéricos exatos. Fundamente em literatura clássica (Robb, Misra, Pirajno, Dardenne & Schobbenhaus, Bizzi et al. 2003). Use terminologia da CPRM.
NÃO inclua disclaimers genéricos ("este laudo é apenas informativo" etc.). Vá direto ao ponto técnico. Se faltar dado crítico (ex: sem análise geoquímica), declare a lacuna e diga qual seria o próximo passo para resolvê-la.

Seja CAUTELOSO mas ASSERTIVO: indique probabilidades (alta/média/baixa) com base nas evidências, mas NÃO afirme presença de depósito sem dados confirmatórios diretos (furos/análises). O laudo é PROSPECTIVO — orienta investigação, não garante reserva.`;

    const userPrompt = `Dados georreferenciados da parcela rural brasileira para análise prospectiva:\n\n${dadosCompactos}\n\nProduza o laudo geológico e prospectivo completo, com modelo conceitual de prospecção e ranking de substâncias minerais prováveis.`;

    const msg = await client.messages.create({
      model: 'claude-sonnet-4-5',
      max_tokens: 4000,
      system: systemPrompt,
      messages: [{ role: 'user', content: userPrompt }],
    });

    const laudo = (msg.content || []).filter(c => c.type === 'text').map(c => c.text).join('\n\n');
    const ms = Date.now() - t0;
    console.log(`[analise-ia] ${ms}ms — ${laudo.length} chars`);
    res.json({ laudo, debug: { ms, model: msg.model, usage: msg.usage } });
  } catch (error) {
    console.error('[analise-ia] erro:', error?.message || error);
    res.status(500).json({ error: String(error?.message || error) });
  }
});

app.post('/api/exportar', async (req, res) => {
  try {
    const { geojson, formato = 'geojson' } = req.body;

    if (!geojson) {
      return res.status(400).json({ error: 'geojson required in body' });
    }

    const feature = parseGeoJSONFeature(geojson);

    if (formato === 'kml') {
      // Convert to KML using PostGIS ST_AsKML
      const result = await pool.query(`
        SELECT ST_AsKML(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)) as kml
      `, [JSON.stringify(feature)]);

      res.setHeader('Content-Type', 'application/vnd.google-earth.kml+xml');
      res.setHeader('Content-Disposition', 'attachment; filename="export.kml"');
      res.send(result.rows[0].kml);
    } else {
      // Return as GeoJSON
      res.setHeader('Content-Type', 'application/json');
      res.setHeader('Content-Disposition', 'attachment; filename="export.geojson"');
      res.json(feature);
    }
  } catch (error) {
    console.error('Error in /api/exportar:', error);
    res.status(500).json({ error: error.message });
  }
});

/**
 * POST /api/relatorio
 * Generate PDF report
 */
app.post('/api/relatorio', async (req, res) => {
  try {
    const { analyses, municipio, geojson } = req.body;

    if (!analyses) {
      return res.status(400).json({ error: 'analyses required in body' });
    }

    // Import reportService
    const reportService = require('./reportService');

    // Query soil polygon geometries for the map
    let soloGeoms = [];
    if (geojson) {
      try {
        // Normalize: extract raw geometry from Feature/FeatureCollection if needed
        let geomObj = typeof geojson === 'string' ? JSON.parse(geojson) : geojson;
        if (geomObj && geomObj.type === 'Feature') geomObj = geomObj.geometry;
        else if (geomObj && geomObj.type === 'FeatureCollection' && geomObj.features && geomObj.features[0]) {
          geomObj = geomObj.features[0].geometry;
        }
        const geomStr = JSON.stringify(geomObj);
        console.log('[relatorio] soloGeoms: geom type =', geomObj && geomObj.type, 'len =', geomStr.length);

        // ST_Transform brings table geoms to SRID 4326 (matching ST_GeomFromGeoJSON default)
        const soloQuery = await pool.query(`
          SELECT
            nome,
            ST_AsGeoJSON(ST_Intersection(
              ST_Transform(CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, 4674) ELSE geom END, 4326),
              ST_GeomFromGeoJSON($1)
            )) as geom_json,
            ROUND(CAST(ST_Area(ST_Intersection(
              ST_Transform(CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, 4674) ELSE geom END, 4326),
              ST_GeomFromGeoJSON($1)
            )) / ST_Area(ST_GeomFromGeoJSON($1)) * 100 AS numeric), 2) as percentual
          FROM solo.pedo_area
          WHERE ST_Intersects(
            ST_Transform(CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, 4674) ELSE geom END, 4326),
            ST_GeomFromGeoJSON($1)
          )
          ORDER BY percentual DESC
        `, [geomStr]);
        soloGeoms = soloQuery.rows;
        console.log('[relatorio] soloGeoms count:', soloGeoms.length);
      } catch (e) {
        console.error('[relatorio] soloGeoms error:', e.message);
      }
    } else {
      console.log('[relatorio] geojson not provided — soil map skipped');
    }
const pdfBuffer = await reportService.generatePDF({
      analyses: analyses,
      municipio: municipio,
      geojson: geojson,
      soloGeoms: soloGeoms,
      generatedAt: new Date(),
    });

    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', 'attachment; filename="adgain-relatorio.pdf"');
    res.send(pdfBuffer);
  } catch (error) {
    console.error('Error in /api/relatorio:', error);
    res.status(500).json({ error: error.message });
  }
});

// ============================================================================
// ERROR HANDLING & SERVER START
// ============================================================================


// Endpoint diagnostico bacias hidrograficas
app.get('/api/test-bacia', async (req, res) => {
    const results = {};
    for (const nivel of [2, 3, 4, 5, 6]) {
          try {
                  const r = await pool.query('SELECT COUNT(*) as total FROM bacias_hidrograficas.bacias_nivel_' + nivel);
                  results['n' + nivel] = { ok: true, count: r.rows[0].total };
          } catch (e) {
                  results['n' + nivel] = { ok: false, err: e.message };
          }
    }
    try {
          const c = await pool.query("SELECT column_name FROM information_schema.columns WHERE table_schema='hidrografia' AND table_name='bacias_nivel_2' ORDER BY ordinal_position");
          results.cols = c.rows.map(r => r.column_name);
    } catch (e) {
          results.cols_err = e.message;
    }
    res.json(results);
});
// Endpoint diagnostico: lista schemas e tabelas bacia
// Diagnóstico: num_area de reserva_legal por cod_imovel
// Diagnóstico: tamanho das tabelas CAR por prefixo em todos os estados
app.get('/api/car-table-sizes', async (req, res) => {
  try {
    const r = await pool.query(`
      SELECT
        table_schema || '.' || table_name AS tabela,
        pg_size_pretty(pg_total_relation_size(quote_ident(table_schema)||'.'||quote_ident(table_name))) AS tamanho_total,
        pg_size_pretty(pg_relation_size(quote_ident(table_schema)||'.'||quote_ident(table_name))) AS tamanho_dados,
        pg_size_pretty(pg_indexes_size(quote_ident(table_schema)||'.'||quote_ident(table_name))) AS tamanho_indices,
        (SELECT reltuples::bigint FROM pg_class WHERE relname = table_name) AS linhas_estimadas
      FROM information_schema.tables
      WHERE table_schema = 'car'
        AND (table_name LIKE 'reserva_legal_%'
          OR table_name LIKE 'vegetacao_nativa_%'
          OR table_name LIKE 'area_consolidada_%')
      ORDER BY pg_total_relation_size(quote_ident(table_schema)||'.'||quote_ident(table_name)) DESC
    `);
    const total = r.rows.reduce((s, x) => {
      const bytes = parseInt(x.tamanho_total);
      return s;
    }, 0);
    res.json({ tabelas: r.rows, total_tabelas: r.rows.length });
  } catch(e) { res.json({ error: e.message }); }
});

app.get('/api/create-car-cod-indexes', async (req, res) => {
  // Endpoint p/ criar btree em cod_imovel sem reiniciar server
  const carPrefixes = ['area_imovel', 'reserva_legal', 'vegetacao_nativa', 'area_consolidada', 'apps'];
  const ufsAll = ['ac','al','am','ap','ba','ce','df','es','go','ma','mg','ms','mt','pa','pb','pe','pi','pr','rj','rn','ro','rr','rs','sc','se','sp','to'];
  const created = [];
  const skipped = [];
  const errored = [];
  for (const prefix of carPrefixes) {
    for (const uf of ufsAll) {
      const tbl = `car.${prefix}_${uf}`;
      const idx = `btree_car_${prefix}_${uf}_cod_imovel`.substring(0, 63);
      try {
        await pool.query(`CREATE INDEX IF NOT EXISTS ${idx} ON ${tbl} (cod_imovel)`);
        created.push(idx);
      } catch (e) {
        if (/does not exist/i.test(e.message)) skipped.push(tbl);
        else errored.push({ tbl, error: e.message });
      }
    }
  }
  res.json({ created_count: created.length, skipped_count: skipped.length, errored, sample_created: created.slice(0, 5) });
});

app.get('/api/cache-clear', (req, res) => {
  const before = analiseCache.size;
  analiseCache.clear();
  res.json({ cleared: before, size_after: analiseCache.size });
});

app.get('/api/car-idx-summary', async (req, res) => {
  // Conta indices btree em cod_imovel agrupados por prefixo
  try {
    const r = await pool.query(`
      SELECT
        regexp_replace(tablename, '_[a-z]{2}$', '') AS prefixo,
        COUNT(*) FILTER (WHERE indexname LIKE 'btree_car_%_cod_imovel') AS estados_com_btree_cod,
        COUNT(*) FILTER (WHERE indexname LIKE 'gist_car_%') AS estados_com_gist
      FROM pg_indexes
      WHERE schemaname='car'
      GROUP BY 1
      ORDER BY 1
    `);
    // tambem lista uf que NÃO tem btree em cada prefixo
    const tablesByPrefix = await pool.query(`
      SELECT
        regexp_replace(tablename, '_[a-z]{2}$', '') AS prefixo,
        right(tablename, 2) AS uf,
        EXISTS(SELECT 1 FROM pg_indexes pi2 WHERE pi2.schemaname='car' AND pi2.tablename = pi.tablename AND pi2.indexname LIKE 'btree_%_cod_imovel') AS tem_btree
      FROM (SELECT DISTINCT tablename FROM pg_tables WHERE schemaname='car') pi
      ORDER BY 1, 2
    `);
    const semBtree = tablesByPrefix.rows.filter(r => !r.tem_btree);
    res.json({
      por_prefixo: r.rows,
      total_tabelas: tablesByPrefix.rows.length,
      total_sem_btree: semBtree.length,
      tabelas_sem_btree: semBtree.map(x => `${x.prefixo}_${x.uf}`),
    });
  } catch (e) {
    res.json({ error: e.message });
  }
});

app.get('/api/test-car-idx', async (req, res) => {
  const uf = (req.query.uf || 'mt').toLowerCase();
  const r = {};
  for (const tbl of [`reserva_legal_${uf}`, `vegetacao_nativa_${uf}`, `area_consolidada_${uf}`]) {
    try {
      const idx = await pool.query(`
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname='car' AND tablename=$1
        ORDER BY indexname
      `, [tbl]);
      r[tbl] = idx.rows;
    } catch(e) { r[tbl] = { error: e.message }; }
  }
  res.json(r);
});

app.get('/api/test-car-promise', async (req, res) => {
  // Replica EXATAMENTE o Promise.all do /api/analises com safeQuery
  const cod = req.query.cod;
  if (!cod) return res.json({ error: 'use ?cod=...' });
  const uf = cod.substring(0, 2).toLowerCase();
  const codImoveisCar = [cod];
  const placeholders = codImoveisCar.map((_, i) => `$${i + 1}`).join(', ');
  const carReservaTable = `car.reserva_legal_${uf}`;
  const carVegNativaTable = `car.vegetacao_nativa_${uf}`;
  const carAreaConsolidadaTable = `car.area_consolidada_${uf}`;
  try {
    const [reservaLegalResult, vegNativaResult, areaConsolidadaResult] = await Promise.all([
      safeQuery(`
        SELECT ROUND(CAST(SUM(num_area) AS numeric), 2) as area_ha
        FROM (
          SELECT DISTINCT ON (gid) CAST(num_area AS numeric) as num_area
          FROM ${carReservaTable}
          WHERE cod_imovel IN (${placeholders})
        ) d
      `, codImoveisCar),
      safeQuery(`
        SELECT ROUND(CAST(SUM(num_area) AS numeric), 2) as area_ha
        FROM (
          SELECT DISTINCT ON (gid) CAST(num_area AS numeric) as num_area
          FROM ${carVegNativaTable}
          WHERE cod_imovel IN (${placeholders})
        ) d
      `, codImoveisCar),
      pool.query(`
        SELECT ROUND(CAST(SUM(num_area) AS numeric), 2) as area_ha
        FROM (
          SELECT DISTINCT ON (gid) CAST(num_area AS numeric) as num_area
          FROM ${carAreaConsolidadaTable}
          WHERE cod_imovel IN (${placeholders})
        ) d
      `, codImoveisCar).catch(e => ({ rows: [], error: e.message })),
    ]);
    res.json({
      cod,
      tables: { carReservaTable, carVegNativaTable, carAreaConsolidadaTable },
      reservaLegal: { rows: reservaLegalResult.rows, rowCount: reservaLegalResult.rowCount },
      vegNativa: { rows: vegNativaResult.rows, rowCount: vegNativaResult.rowCount },
      areaConsolidada: { rows: areaConsolidadaResult.rows, rowCount: areaConsolidadaResult.rowCount, error: areaConsolidadaResult.error },
    });
  } catch (e) {
    res.json({ error: e.message });
  }
});

app.get('/api/test-altitude-grid', async (req, res) => {
  const cod = req.query.cod;
  if (!cod) return res.json({ error: 'use ?cod=...' });
  const uf = cod.substring(0, 2).toLowerCase();
  try {
    const g = await pool.query(`SELECT ST_AsGeoJSON(ST_Multi(ST_Union(geom)))::jsonb as geom FROM car.area_imovel_${uf} WHERE cod_imovel = $1`, [cod]);
    if (!g.rows[0]?.geom) return res.json({ error: 'cod not found' });
    const feature = { type: 'Feature', geometry: g.rows[0].geom };
    const geojsonStr = JSON.stringify(feature);
    const t0 = Date.now();
    try {
      const r = await pool.query(`
        WITH parcel AS (
          SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AS g
        ),
        bbox AS (
          SELECT g, ST_XMin(g) AS minx, ST_XMax(g) AS maxx, ST_YMin(g) AS miny, ST_YMax(g) AS maxy FROM parcel
        ),
        grid AS (
          SELECT b.g AS pg,
                 b.minx + ((b.maxx - b.minx) * i / 35.0) AS lng,
                 b.miny + ((b.maxy - b.miny) * j / 35.0) AS lat
          FROM bbox b, generate_series(0, 35) i, generate_series(0, 35) j
        ),
        inside AS (
          SELECT lng, lat, ST_SetSRID(ST_MakePoint(lng, lat), 4674) AS pt
          FROM grid
          WHERE ST_Contains(pg, ST_SetSRID(ST_MakePoint(lng, lat), 4674))
        )
        SELECT i.lng::float8 AS lng, i.lat::float8 AS lat,
               ST_Value(r.rast, i.pt)::float8 AS m
        FROM inside i
        JOIN altitude_br.altitude_raster r ON ST_Intersects(r.rast, i.pt)
        WHERE ST_Value(r.rast, i.pt) IS NOT NULL
        LIMIT 2000
      `, [geojsonStr]);
      res.json({ cod, ms: Date.now() - t0, count: r.rows.length, sample: r.rows.slice(0, 3) });
    } catch (e) {
      res.json({ cod, ms: Date.now() - t0, error: e.message });
    }
  } catch (e) {
    res.json({ error: e.message });
  }
});

app.get('/api/test-car-pipeline', async (req, res) => {
  // Simula a chamada /api/analises usando a geometria do CAR cod_imovel
  const cod = req.query.cod;
  if (!cod) return res.json({ error: 'use ?cod=...' });
  const uf = cod.substring(0, 2).toLowerCase();
  try {
    const g = await pool.query(`SELECT ST_AsGeoJSON(ST_Multi(ST_Union(geom)))::jsonb as geom FROM car.area_imovel_${uf} WHERE cod_imovel = $1`, [cod]);
    if (!g.rows[0] || !g.rows[0].geom) return res.json({ error: 'cod not found in area_imovel' });
    const feature = { type: 'Feature', properties: { cod_imovel: cod }, geometry: g.rows[0].geom };
    // POST internamente para /api/analises
    const http = require('http');
    const body = JSON.stringify({ geojson: feature, origem: 'car' });
    const port = process.env.PORT || 3001;
    const opts = { hostname: '127.0.0.1', port, path: '/api/analises', method: 'POST', headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) } };
    const r = await new Promise((resolve, reject) => {
      const req = http.request(opts, (resp) => {
        let data = '';
        resp.on('data', c => data += c);
        resp.on('end', () => { try { resolve(JSON.parse(data)); } catch(e) { resolve({ raw: data.slice(0, 2000) }); } });
      });
      req.on('error', reject);
      req.write(body);
      req.end();
    });
    const car = r?.resultados?.car;
    const alt = r?.resultados?.altitude;
    const pd = r?.resultados?.prodes_deter;
    const cf = r?.resultados?.conformidade;
    res.json({
      cod,
      car_summary: {
        imoveis_count: car?.imoveis?.length,
        imoveis_cod: (car?.imoveis || []).map(i => i.cod_imovel),
        area_app_ha: car?.area_app_ha,
        area_reserva_legal_ha: car?.area_reserva_legal_ha,
        area_vegetacao_nativa_ha: car?.area_vegetacao_nativa_ha,
        area_consolidada_ha: car?.area_consolidada_ha,
      },
      altitude_summary: {
        min: alt?.altitude_min, max: alt?.altitude_max, media: alt?.altitude_media,
        grid_pontos: (alt?.altitude_grid || []).length,
      },
      prodes_deter: pd ? {
        prodes_count: (pd.prodes || []).length,
        prodes_area_total_ha: pd.prodes_area_total_ha,
        deter_count: (pd.deter || []).length,
        deter_area_total_ha: pd.deter_area_total_ha,
        debug: pd.debug,
        first_prodes: (pd.prodes || [])[0],
        first_deter: (pd.deter || [])[0],
      } : null,
      conformidade: cf || null,
      response_keys: Object.keys(r?.resultados || {}),
    });
  } catch (e) {
    res.json({ error: e.message, stack: e.stack });
  }
});

app.get('/api/test-car-rl', async (req, res) => {
  const cod = req.query.cod;
  if (!cod) return res.json({ error: 'use ?cod=...' });
  const uf = cod.substring(0, 2).toLowerCase();
  const codes = cod.split(',').map(s => s.trim()).filter(Boolean);
  const placeholders = codes.map((_, i) => `$${i + 1}`).join(', ');
  const out = { codes, uf };
  for (const tab of ['reserva_legal', 'vegetacao_nativa', 'area_consolidada']) {
    try {
      const sql = `SELECT ROUND(CAST(SUM(num_area) AS numeric), 2) as area_ha FROM (SELECT DISTINCT ON (gid) CAST(num_area AS numeric) as num_area FROM car.${tab}_${uf} WHERE cod_imovel IN (${placeholders})) d`;
      const r = await pool.query(sql, codes);
      out[tab] = { rows: r.rows, sql };
    } catch (e) {
      out[tab] = { error: e.message };
    }
  }
  res.json(out);
});

app.get('/api/car-geom', async (req, res) => {
  // Retorna GeoJSON Feature do imóvel CAR para visualizar no mapa
  const cod = (req.query.cod || '').toString();
  if (!cod) return res.status(400).json({ error: 'use ?cod=MT-XXX-...' });
  const uf = cod.substring(0, 2).toLowerCase();
  const table = `car.area_imovel_${uf}`;
  try {
    const r = await pool.query(`
      SELECT ST_AsGeoJSON(ST_Multi(ST_Union(
        CASE WHEN ST_SRID(geom) != 4674 THEN ST_SetSRID(geom, 4674) ELSE geom END
      )))::jsonb AS geom,
      MAX(num_area) AS num_area,
      MAX(ind_status) AS ind_status,
      MAX(ind_tipo) AS ind_tipo,
      MAX(des_condic) AS des_condic
      FROM ${table} WHERE cod_imovel = $1
    `, [cod]);
    if (!r.rows[0] || !r.rows[0].geom) return res.json({ error: 'not found' });
    res.json({
      type: 'Feature',
      geometry: r.rows[0].geom,
      properties: {
        cod_imovel: cod,
        num_area: r.rows[0].num_area,
        ind_status: r.rows[0].ind_status,
        ind_tipo: r.rows[0].ind_tipo,
        des_condic: r.rows[0].des_condic,
      },
    });
  } catch (e) {
    console.warn('[car-geom]', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/test-car', async (req, res) => {
  const cod = req.query.cod;
  if (!cod) return res.json({ error: 'use ?cod=MT-XXX-...' });
  const uf = cod.substring(0, 2).toLowerCase();
  const tables = {
    area_imovel:       `car.area_imovel_${uf}`,
    apps:              `car.apps_${uf}`,
    reserva_legal:     `car.reserva_legal_${uf}`,
    vegetacao_nativa:  `car.vegetacao_nativa_${uf}`,
    area_consolidada:  `car.area_consolidada_${uf}`,
  };
  const out = { cod, uf, tabelas: {} };
  for (const [key, table] of Object.entries(tables)) {
    try {
      // schema
      const cols = await pool.query(`
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema='car' AND table_name=$1
        ORDER BY ordinal_position
      `, [table.split('.')[1]]);

      // count total in table
      const cnt = await pool.query(`SELECT COUNT(*)::bigint as total FROM ${table}`);
      // count for cod_imovel
      const found = await pool.query(`SELECT COUNT(*)::bigint as total FROM ${table} WHERE cod_imovel = $1`, [cod]);
      // sample row
      const sample = await pool.query(`SELECT * FROM ${table} WHERE cod_imovel = $1 LIMIT 3`, [cod]);
      const samp = sample.rows.map(r => {
        const o = {};
        for (const [k, v] of Object.entries(r)) {
          if (k === 'geom') o[k] = '<geometry>';
          else o[k] = v;
        }
        return o;
      });
      out.tabelas[key] = {
        table,
        colunas: cols.rows.map(c => `${c.column_name}:${c.data_type}`),
        total_rows: cnt.rows[0].total,
        rows_para_cod: found.rows[0].total,
        amostra: samp,
      };
    } catch (e) {
      out.tabelas[key] = { table, error: e.message };
    }
  }
  res.json(out);
});

app.get('/api/test-reserva', async (req, res) => {
  const cod = req.query.cod || 'AP-1600105-B80400C38AC04BB3AA90346BF37DFAEC';
  const uf = cod.substring(0, 2).toLowerCase();
  const table = `car.reserva_legal_${uf}`;
  try {
    const r = await pool.query(
      `SELECT gid, cod_imovel, num_area,
              ROUND(CAST(ST_Area(ST_Transform(geom, 32721))/10000 AS numeric), 2) as area_geom_ha
       FROM ${table}
       WHERE cod_imovel = $1`, [cod]);
    res.json({ table, cod, rows: r.rows, total_num_area: r.rows.reduce((s, x) => s + parseFloat(x.num_area || 0), 0) });
  } catch(e) { res.json({ error: e.message, table, cod }); }
});

app.get('/api/db-schema', async (req, res) => {
    const r = {};
    try { const s = await pool.query('SELECT schema_name FROM information_schema.schemata ORDER BY schema_name'); r.schemas = s.rows.map(x=>x.schema_name); } catch(e){r.schemas_err=e.message;}
    try { const t = await pool.query("SELECT table_schema,table_name FROM information_schema.tables WHERE table_name ILIKE '%bacia%' ORDER BY table_schema,table_name"); r.bacia_tables=t.rows; } catch(e){r.bacia_err=e.message;}
    try { const h = await pool.query("SELECT table_name FROM information_schema.tables WHERE table_schema='hidrografia' ORDER BY table_name"); r.hidro_tables=h.rows.map(x=>x.table_name); } catch(e){r.hidro_err=e.message;}
    res.json(r);
});

// Endpoint diagnóstico: inspeciona tabelas geologia_litologia
app.get('/api/test-geologia', async (req, res) => {
  const r = {};
  const tables = ['geol_ponto','ocorrencias_br','geol_linha_dobra','geol_linha_falha','geol_linha_fratura'];

  // Lista tabelas existentes no schema
  try {
    const t = await pool.query("SELECT table_name FROM information_schema.tables WHERE table_schema='geologia_litologia' ORDER BY table_name");
    r.tables_in_schema = t.rows.map(x=>x.table_name);
  } catch(e) { r.tables_err = e.message; }

  // Para cada tabela: colunas, SRID e total de linhas
  for (const tbl of tables) {
    try {
      const cols = await pool.query(`
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema='geologia_litologia' AND table_name=$1
        ORDER BY ordinal_position
      `, [tbl]);
      r[tbl] = { columns: cols.rows };

      // tenta pegar SRID e contagem
      try {
        const info = await pool.query(`
          SELECT COUNT(*) as total,
                 ST_SRID(geom) as srid
          FROM geologia_litologia.${tbl}
          LIMIT 1
        `);
        r[tbl].count = info.rows[0]?.total;
        r[tbl].srid  = info.rows[0]?.srid;
      } catch(e2) { r[tbl].geom_err = e2.message; }

      // tenta pegar nome da coluna de geometria
      try {
        const gc = await pool.query(`
          SELECT f_geometry_column, srid, type
          FROM geometry_columns
          WHERE f_table_schema='geologia_litologia' AND f_table_name=$1
        `, [tbl]);
        r[tbl].geometry_columns = gc.rows;
      } catch(e3) { r[tbl].gc_err = e3.message; }

    } catch(e) { r[tbl] = { err: e.message }; }
  }

  res.json(r);
});
app.get('/api/test-geol-parcela', async (req, res) => {
    const id = req.query.id || '9f6b9aa6-9a3f-451b-b6df-6c03fef1d786';
    try {
          const pr = await pool.query('SELECT ST_AsGeoJSON(geom) as g FROM incra.sigef_ap WHERE parcela_co=$1 LIMIT 1', [id]);
          if (!pr.rows[0]) return res.json({ error: 'parcela nao encontrada', id });
          const geojsonStr = JSON.stringify({ type:'Feature', geometry: JSON.parse(pr.rows[0].g), properties:{} });
          const result = { parcelaId: id };
          const qs = {
                  pontos:   `SELECT COUNT(*) n FROM geologia_litologia.geol_ponto WHERE geom && ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'),4674) AND ST_Intersects(CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom,4674) ELSE geom END, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'),4674))`,
                  falhas:   `SELECT COUNT(*) n FROM geologia_litologia.geol_linha_falha WHERE geom && ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'),4674) AND ST_Intersects(CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom,4674) ELSE geom END, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'),4674))`,
                  dobras:   `SELECT COUNT(*) n FROM geologia_litologia.geol_linha_dobra WHERE geom && ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'),4674) AND ST_Intersects(CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom,4674) ELSE geom END, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'),4674))`,
                  fraturas: `SELECT COUNT(*) n FROM geologia_litologia.geol_linha_fratura WHERE geom && ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'),4674) AND ST_Intersects(CASE WHEN ST_SRID(geom) != ${SRID} THEN ST_SetSRID(geom,4674) ELSE geom END, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'),4674))`,
          };
          for (const [k, sql] of Object.entries(qs)) {
                  try { const q = await pool.query(sql, [geojsonStr]); result[k] = parseInt(q.rows[0].n); }
                  catch (e) { result[k + '_err'] = e.message; }
          }
          res.json(result);
    } catch (e) { res.json({ error: e.message }); }
});

// Catch-all 404
app.use((req, res) => {
  res.status(404).json({ error: 'Endpoint not found' });
});

// Error middleware
app.use((err, req, res, next) => {
  console.error('Unhandled error:', err);
  res.status(500).json({ error: 'Internal server error' });
});

ensureGistIndexes().catch(console.error);
ensureLogisticaTable().catch(console.error);
ensureRodoviasTable().catch(console.error);

// Start server
app.listen(port, () => {
  console.log(`AdGain API server running on port ${port}`);
  console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
});
// rebuild trigger 2026-05-26 03:09:54 UTC
