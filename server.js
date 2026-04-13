require('dotenv').config();

const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');

const app = express();
const port = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json({ limit: '50mb' }));

// Database connection pool
const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  client_encoding: 'UTF8',
});

const SRID = 4674; // SIRGAS 2000

// ============================================================================
// DETECÇÃO MULTI-ESTADO: verifica se o schema sicar (todos os estados) existe
// ============================================================================
// Flags atualizados na inicialização e consultáveis durante as análises
let sicarDisponivel = false;   // sicar.area_imovel existe?
let sicarAppsUnificado = false; // sicar.apps existe (em vez de apps1_mt..apps4_mt)?

async function detectarSchemaSicar() {
  try {
    const res = await pool.query(`
      SELECT COUNT(*) as n
      FROM information_schema.tables
      WHERE table_schema = 'sicar' AND table_name = 'area_imovel'
    `);
    sicarDisponivel = parseInt(res.rows[0].n) > 0;

    if (sicarDisponivel) {
      const appsRes = await pool.query(`
        SELECT COUNT(*) as n
        FROM information_schema.tables
        WHERE table_schema = 'sicar' AND table_name = 'apps'
      `);
      sicarAppsUnificado = parseInt(appsRes.rows[0].n) > 0;
      console.log(`[SICAR] Schema multi-estado disponível. apps unificado: ${sicarAppsUnificado}`);
    } else {
      console.log('[SICAR] Schema não encontrado — usando tabelas MT legadas (car.*)');
    }
  } catch(e) {
    console.error('[SICAR] Erro na detecção:', e.message);
  }
}

// Executar detecção ao iniciar
pool.connect().then(client => {
  client.release();
  detectarSchemaSicar();
}).catch(() => {});

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
  return `CASE WHEN ST_SRID(${geomColumn}) = 0 THEN ST_SetSRID(${geomColumn}, ${expectedSrid}) ELSE ${geomColumn} END`;
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
      ROUND(CAST(SUM(ST_Area(ST_Intersection(${safeGeom}, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})))) /
                     ST_Area(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})) * 100 AS numeric), 2) as percentual
    FROM ${table}
    WHERE ST_Intersects(${safeGeom}, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}))
    GROUP BY nome
    ORDER BY percentual DESC
  `;
}

/**
 * Safely parse GeoJSON feature
 */
function parseGeoJSONFeature(geojson) {
  if (typeof geojson === 'string') {
    return JSON.parse(geojson);
  }
  return geojson;
}

/**
 * Parse XML tables returned by ANA HidroWebService (DiffGram format).
 * Handles both:
 *   <Table1>...</Table1>  — some endpoints
 *   <Table diffgr:id="Table1" ...>...</Table>  — HidroInventario / HidroSerieHistorica
 */
function parseXmlTables(xml) {
  const rows = [];
  // Matches <Table ...> or <Table1 ...> — ANA DiffGram row elements
  const tableRe = /<(Table1?)(?:\s[^>]*)?>([\s\S]*?)<\/\1>/g;
  let tm;
  while ((tm = tableRe.exec(xml)) !== null) {
    const content = tm[2];
    const row = {};
    const fieldRe = /<(\w+)>([\s\S]*?)<\/\1>/g;
    let fm;
    while ((fm = fieldRe.exec(content)) !== null) {
      row[fm[1]] = fm[2].trim();
    }
    if (Object.keys(row).length > 0) rows.push(row);
  }
  return rows;
}

/**
 * Haversine distance in km between two lat/lng points
 */
function haversineKm(lat1, lng1, lat2, lng2) {
  const R = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) ** 2
    + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLng / 2) ** 2;
  return Math.round(R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)) * 10) / 10;
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
 * GET /api/debug-inmet?uf=MT&estacao=A908
 * Testa API do INMET: lista estações e busca série histórica
 */
app.get('/api/debug-inmet', async (req, res) => {
  const uf      = req.query.uf      || 'MT';
  const estacao = req.query.estacao || null;
  try {
    // 1. Lista estações automáticas
    const listResp = await fetch('https://apitempo.inmet.gov.br/estacoes/T', { signal: AbortSignal.timeout(15000) });
    const todas = await listResp.json();
    const doUF  = todas.filter(s => s.SG_ESTADO === uf && !s.DT_FIM_OPERACAO);

    let serie = null;
    if (estacao || doUF[0]) {
      const cd = estacao || doUF[0].CD_ESTACAO;
      const serieResp = await fetch(
        `https://apitempo.inmet.gov.br/estacao/2023-01-01/2023-03-31/${cd}`,
        { signal: AbortSignal.timeout(15000) }
      );
      const dados = await serieResp.json();
      serie = {
        estacao: cd,
        status: serieResp.status,
        registros: Array.isArray(dados) ? dados.length : dados,
        campos: Array.isArray(dados) && dados[0] ? Object.keys(dados[0]) : [],
        primeiro: Array.isArray(dados) ? dados[0] : dados,
      };
    }

    res.json({
      total_estacoes: todas.length,
      estacoes_uf: doUF.length,
      primeira_estacao_uf: doUF[0] || null,
      serie_teste: serie,
    });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

/**
 * POST /api/buscar-feicao
 * Click search with SIGEF→SNCI→CAR priority cascade
 * Finds which parcel contains the clicked point
 */
app.post('/api/buscar-feicao', async (req, res) => {
  try {
    // Accept lat/lng OR latitude/longitude
    const lat  = req.body.lat  ?? req.body.latitude;
    const lng  = req.body.lng  ?? req.body.longitude;
    const camadasAtivas = req.body.camadasAtivas || ['sigef', 'car']; // default: todas

    if (!lat || !lng) {
      return res.status(400).json({ error: 'lat and lng required' });
    }

    const point = `SRID=${SRID};POINT(${lng} ${lat})`;
    let result = { rows: [] };

    // Try SIGEF first — only if sigef layer is active (&& uses GiST index for fast pre-filter)
    if (result.rows.length === 0 && camadasAtivas.includes('sigef')) {
      result = await pool.query(`
        SELECT
          'sigef' as origem,
          parcela_co as cod_imovel,
          nome_area,
          situacao_i,
          ST_AsGeoJSON(geom) as geojson,
          ROUND(CAST(ST_Area(ST_Transform(geom, 32721)) / 10000 AS numeric), 2) as area_ha
        FROM incra.sigef_all
        WHERE geom && ST_GeomFromText($1)
          AND ST_Contains(geom, ST_GeomFromText($1))
        LIMIT 1
      `, [point]);
    }

    // Try SNCI — only if sigef layer is active (sigef includes SNCI)
    if (result.rows.length === 0 && camadasAtivas.includes('sigef')) {
      result = await pool.query(`
        SELECT
          'snci' as origem,
          cod_imovel,
          nome_imove as nome_area,
          num_certif as situacao_i,
          ST_AsGeoJSON(geom) as geojson,
          ROUND(CAST(qtd_area_p::numeric AS numeric), 2) as area_ha
        FROM incra.snci_all
        WHERE geom && ST_GeomFromText($1)
          AND ST_Contains(geom, ST_GeomFromText($1))
        LIMIT 1
      `, [point]);
    }

    // Try CAR — only if car layer is active
    if (result.rows.length === 0 && camadasAtivas.includes('car')) {
      result = await pool.query(`
        SELECT
          'car' as origem,
          cod_imovel,
          cod_imovel as nome_area,
          ind_status as situacao_i,
          ST_AsGeoJSON(geom) as geojson,
          ROUND(CAST(num_area AS numeric), 2) as area_ha
        FROM ${sicarDisponivel ? 'sicar.area_imovel' : 'car.area_imovelmt'}
        WHERE geom && ST_GeomFromText($1)
          AND ST_Contains(geom, ST_GeomFromText($1))
          AND des_condic NOT ILIKE '%cancelado%'
        LIMIT 1
      `, [point]);
    }

    if (result.rows.length === 0) {
      return res.status(404).json({ origem: null, dados: [] });
    }

    const row = result.rows[0];
    // Return format expected by Angular component: { origem, dados: [...] }
    res.json({
      origem: row.origem,
      dados: [{
        cod_imovel:  row.cod_imovel,
        nome_area:   row.nome_area,
        situacao_i:  row.situacao_i,
        area_ha:     row.area_ha,
        geojson:     row.geojson,   // string — component calls JSON.parse on it
      }],
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
    const { geojson } = req.body;

    if (!geojson) {
      return res.status(400).json({ error: 'geojson required in body' });
    }

    // Normalize: always wrap as a GeoJSON Feature so queries can use ->>'geometry'
    let geom = geojson;
    if (geom.type !== 'Feature') {
      geom = { type: 'Feature', geometry: geom, properties: {} };
    }

    // Guarda geometrias individuais ANTES do union (usadas para centroide CAR por parcela)
    let individualGeometries = [];
    if (geom.geometry && geom.geometry.type === 'GeometryCollection') {
      individualGeometries = geom.geometry.geometries;
      // Faz union para todas as outras queries espaciais (bioma, solo, altitude etc.)
      const geomStrs = individualGeometries.map(g => `ST_GeomFromGeoJSON('${JSON.stringify(g)}')`).join(', ');
      const unionResult = await pool.query(`SELECT ST_AsGeoJSON(ST_Union(ARRAY[${geomStrs}])) as geom`);
      geom.geometry = JSON.parse(unionResult.rows[0].geom);
    } else {
      individualGeometries = [geom.geometry];
    }
    const geojsonStr = JSON.stringify(geom);

    // Get centroid and ALL municipalities that intersect the parcel
    let municResult = await pool.query(`
      SELECT
        "NM_MUN" as municipio,
        "SIGLA_UF" as uf,
        ST_AsGeoJSON(ST_Centroid(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}))) as centroid,
        ROUND(CAST(ST_Area(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}), 32721)) / 10000 AS numeric), 2) as area_hectares
      FROM municipios.municipios_2024 m
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(m.geom)=0 THEN ST_SetSRID(m.geom,${SRID}) ELSE m.geom END,
        ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
      )
      ORDER BY "NM_MUN"
    `, [geojsonStr]);

    // municipio principal (primeiro) para uso interno nas queries de serventia etc.
    const municipio = municResult.rows[0] || {
      municipio: 'Desconhecido',
      uf: 'MT',
      centroid: null,
      area_hectares: 0,
    };

    const analyses = {};

    // 9.1 Fundiária (SIGEF + SNCI + CAR cruzado)
    analyses['9.1_fundiaria'] = {
      nome: 'Fundiária',
      sigef: [],
      snci: [],
      car: [],
    };

    // ── SIGEF: busca parcelas que se sobrepõem à área selecionada
    //    Usa geometria INLINE (sem CTE) para melhor pushdown de predicados na view UNION ALL
    //    Tenta primeiro com SRID do SIGEF; se não encontrar, tenta sem SRID
    try {
      const sigefRows = [];
      const sigefSeen = new Set();
      for (const indivGeom of individualGeometries) {
        const geomJson = JSON.stringify(indivGeom);
        console.log(`[SIGEF] buscando sobreposição, geom type=${indivGeom?.type}`);

        // Query direta sem CTE — geometria inline para permitir index pushdown na view
        let r = await pool.query(`
          SELECT
            s.parcela_co,
            s.nome_area,
            s.situacao_i,
            TO_CHAR(s.data_aprov, 'DD/MM/YYYY')  as data_aprov,
            s.codigo_imo,
            s.registro_m,
            TO_CHAR(s.registro_d, 'DD/MM/YYYY')  as registro_d,
            ROUND(CAST(ST_Area(ST_Transform(s.geom, 32721)) / 10000 AS numeric), 2) as area_hectares
          FROM incra.sigef_all s
          WHERE s.geom && ST_SetSRID(ST_GeomFromGeoJSON($1), ${SRID})
            AND ST_Intersects(s.geom, ST_SetSRID(ST_GeomFromGeoJSON($1), ${SRID}))
            AND NOT ST_Touches(s.geom, ST_SetSRID(ST_GeomFromGeoJSON($1), ${SRID}))
          LIMIT 50
        `, [geomJson]);
        console.log(`[SIGEF] query com SRID ${SRID}: ${r.rows.length} rows`);

        // Fallback: se não encontrou, tentar sem SRID (caso haja mismatch)
        if (r.rows.length === 0) {
          console.log(`[SIGEF] tentando fallback sem SRID...`);
          r = await pool.query(`
            SELECT
              s.parcela_co,
              s.nome_area,
              s.situacao_i,
              TO_CHAR(s.data_aprov, 'DD/MM/YYYY')  as data_aprov,
              s.codigo_imo,
              s.registro_m,
              TO_CHAR(s.registro_d, 'DD/MM/YYYY')  as registro_d,
              ROUND(CAST(ST_Area(ST_Transform(ST_SetSRID(s.geom, ${SRID}), 32721)) / 10000 AS numeric), 2) as area_hectares
            FROM incra.sigef_all s
            WHERE ST_SetSRID(s.geom, 0) && ST_GeomFromGeoJSON($1)
              AND ST_Intersects(ST_SetSRID(s.geom, 0), ST_GeomFromGeoJSON($1))
              AND NOT ST_Touches(ST_SetSRID(s.geom, 0), ST_GeomFromGeoJSON($1))
            LIMIT 50
          `, [geomJson]);
          console.log(`[SIGEF] fallback sem SRID: ${r.rows.length} rows`);
        }

        // Fallback 2: se ainda não encontrou, tentar por tabelas individuais do estado
        if (r.rows.length === 0) {
          console.log(`[SIGEF] tentando query por tabelas individuais...`);
          try {
            const tablesRes = await pool.query(`
              SELECT table_name FROM information_schema.tables
              WHERE table_schema = 'incra' AND table_name LIKE 'sigef_%'
              ORDER BY table_name
            `);
            for (const tRow of tablesRes.rows) {
              const tbl = tRow.table_name;
              try {
                const r2 = await pool.query(`
                  SELECT
                    parcela_co, nome_area, situacao_i,
                    TO_CHAR(data_aprov, 'DD/MM/YYYY') as data_aprov,
                    codigo_imo, registro_m,
                    TO_CHAR(registro_d, 'DD/MM/YYYY') as registro_d,
                    ROUND(CAST(ST_Area(ST_Transform(geom, 32721)) / 10000 AS numeric), 2) as area_hectares
                  FROM incra.${tbl}
                  WHERE geom && ST_SetSRID(ST_GeomFromGeoJSON($1), ST_SRID(geom))
                    AND ST_Intersects(geom, ST_SetSRID(ST_GeomFromGeoJSON($1), ST_SRID(geom)))
                    AND NOT ST_Touches(geom, ST_SetSRID(ST_GeomFromGeoJSON($1), ST_SRID(geom)))
                  LIMIT 50
                `, [geomJson]);
                if (r2.rows.length > 0) {
                  console.log(`[SIGEF] tabela incra.${tbl}: ${r2.rows.length} rows!`);
                  r.rows.push(...r2.rows);
                }
              } catch(e2) { /* tabela sem as colunas, ignorar */ }
            }
          } catch(e3) { console.error('[SIGEF] erro listando tabelas:', e3.message); }
        }

        for (const row of r.rows) {
          const key = row.parcela_co || row.nome_area;
          if (key && !sigefSeen.has(key)) { sigefSeen.add(key); sigefRows.push(row); }
        }
      }
      analyses['9.1_fundiaria'].sigef = sigefRows;
      console.log(`[SIGEF] ${sigefRows.length} parcela(s) encontrada(s)`);
    } catch(e) { console.error('SIGEF query error:', e.message, e.stack); }

    // ── SNCI: mesma lógica — geometria inline sem CTE
    try {
      const snciRows = [];
      const snciSeen = new Set();
      for (const indivGeom of individualGeometries) {
        const geomJson = JSON.stringify(indivGeom);

        let r = await pool.query(`
          SELECT
            s.cod_imovel,
            s.nome_imove,
            s.num_certif,
            TO_CHAR(s.data_certi, 'DD/MM/YYYY') as data_certi,
            s.qtd_area_p,
            ROUND(CAST(s.qtd_area_p AS numeric), 2) as area_hectares
          FROM incra.snci_all s
          WHERE s.geom && ST_SetSRID(ST_GeomFromGeoJSON($1), ${SRID})
            AND ST_Intersects(s.geom, ST_SetSRID(ST_GeomFromGeoJSON($1), ${SRID}))
            AND NOT ST_Touches(s.geom, ST_SetSRID(ST_GeomFromGeoJSON($1), ${SRID}))
          LIMIT 50
        `, [geomJson]);
        console.log(`[SNCI] query com SRID ${SRID}: ${r.rows.length} rows`);

        // Fallback sem SRID
        if (r.rows.length === 0) {
          r = await pool.query(`
            SELECT
              s.cod_imovel, s.nome_imove, s.num_certif,
              TO_CHAR(s.data_certi, 'DD/MM/YYYY') as data_certi,
              s.qtd_area_p,
              ROUND(CAST(s.qtd_area_p AS numeric), 2) as area_hectares
            FROM incra.snci_all s
            WHERE ST_SetSRID(s.geom, 0) && ST_GeomFromGeoJSON($1)
              AND ST_Intersects(ST_SetSRID(s.geom, 0), ST_GeomFromGeoJSON($1))
              AND NOT ST_Touches(ST_SetSRID(s.geom, 0), ST_GeomFromGeoJSON($1))
            LIMIT 50
          `, [geomJson]);
          console.log(`[SNCI] fallback sem SRID: ${r.rows.length} rows`);
        }

        for (const row of r.rows) {
          const key = row.cod_imovel;
          if (key && !snciSeen.has(key)) { snciSeen.add(key); snciRows.push(row); }
        }
      }
      analyses['9.1_fundiaria'].snci = snciRows;
      console.log(`[SNCI] ${snciRows.length} imóvel(is) encontrado(s)`);
    } catch(e) { console.error('SNCI query error:', e.message); }

    // ── CAR cruzado: busca CARs que se sobrepõem à área selecionada
    try {
      const carRows = [];
      const carSeen = new Set();
      const carTable = sicarDisponivel ? 'sicar.area_imovel' : 'car.area_imovelmt';
      for (const indivGeom of individualGeometries) {
        const geomJson = JSON.stringify(indivGeom);
        const r = await pool.query(`
          WITH area AS (
            SELECT ST_SetSRID(ST_GeomFromGeoJSON($1), ${SRID}) as geom
          )
          SELECT
            c.cod_imovel,
            c.num_area,
            c.ind_status,
            c.ind_tipo,
            c.des_condic,
            c.municipio,
            c.cod_estado,
            ROUND(CAST(c.num_area AS numeric), 2) as area_hectares
          FROM ${carTable} c, area a
          WHERE c.geom && a.geom
            AND ST_Intersects(c.geom, a.geom)
            AND NOT ST_Touches(c.geom, a.geom)
            AND c.des_condic NOT ILIKE '%cancelado%'
          LIMIT 10
        `, [geomJson]);
        for (const row of r.rows) {
          const key = row.cod_imovel;
          if (key && !carSeen.has(key)) { carSeen.add(key); carRows.push(row); }
        }
      }
      analyses['9.1_fundiaria'].car = carRows;
      console.log(`[CAR cruzado] ${carRows.length} imóvel(is) encontrado(s)`);
    } catch(e) { console.error('CAR cruzado query error:', e.message); }

    // 9.2 Registral (Serventias)
    // Passo 1: tenta sobreposição espacial direta com a parcela
    // Passo 2: se não encontrar, busca pelo nome do município em obs e abrangen
    analyses['9.2_registral'] = {
      nome: 'Registral',
      serventias: [],
      message: '',
    };

    try {
      // Passo 1 — sobreposição espacial
      let serventiaResult = await pool.query(`
        SELECT
          cartorio,
          codigo_cns,
          comarca,
          obs,
          abrangen
        FROM serventias.serventias_brasil
        WHERE ST_Intersects(
          geom,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )
        LIMIT 10
      `, [geojsonStr]);

      if (serventiaResult.rows.length > 0) {
        analyses['9.2_registral'].serventias = serventiaResult.rows;
        analyses['9.2_registral'].message = 'Sobreposição direta';
      } else if (municipio.municipio && municipio.municipio !== 'Desconhecido') {
        // Passo 2 — busca pelo nome do município em obs e abrangen
        let serventiaByMunicResult = await pool.query(`
          SELECT
            cartorio,
            codigo_cns,
            comarca,
            obs,
            abrangen
          FROM serventias.serventias_brasil
          WHERE obs       ILIKE $1
             OR abrangen  ILIKE $1
          LIMIT 10
        `, [`%${municipio.municipio}%`]);
        analyses['9.2_registral'].serventias = serventiaByMunicResult.rows;
        analyses['9.2_registral'].message = `Município: ${municipio.municipio}`;
      }
    } catch(e) { console.error('Serventias query error:', e.message); }

    // 9.3 Solo (Pedologia with percentages)
    analyses['9.3_solo'] = {
      nome: 'Solo',
      data: [],
    };

    try {
      let soloResult = await pool.query(`
        SELECT
          legenda,
          ordem,
          subordem,
          textura,
          relevo,
          ROUND(CAST(SUM(ST_Area(ST_Intersection(
            CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
            ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
          ))) / ST_Area(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})) * 100 AS numeric), 2) as percentual
        FROM solo.pedo_area
        WHERE ST_Intersects(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )
        GROUP BY legenda, ordem, subordem, textura, relevo
        ORDER BY percentual DESC
      `, [geojsonStr]);
      analyses['9.3_solo'].data = soloResult.rows;
    } catch(e) { console.error('Solo query error:', e.message); }

    // 9.4 Bioma (with percentages)
    analyses['9.4_bioma'] = {
      nome: 'Bioma',
      data: [],
    };

    try {
      let biomaResult = await pool.query(`
        SELECT
          "Bioma" as nome,
          ROUND(CAST(SUM(ST_Area(ST_Intersection(
            CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
            ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
          ))) / ST_Area(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})) * 100 AS numeric), 2) as percentual
        FROM bioma.bioma_250
        WHERE ST_Intersects(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )
        GROUP BY "Bioma"
        ORDER BY percentual DESC
      `, [geojsonStr]);
      analyses['9.4_bioma'].data = biomaResult.rows;
    } catch(e) { console.error('Bioma query error:', e.message); }

    // 9.5 Geologia (Litoestratigrafia with percentages)
    analyses['9.5_geologia'] = {
      nome: 'Geologia',
      data: [],
    };

    try {
      let geologiaResult = await pool.query(`
        SELECT
          "NOME"               as nome,
          "SIGLA"              as sigla,
          "LITOTIPOS"          as litotipos,
          "ERA_MIN"            as era_min,
          "AMBIENTE_TECTONICO" as ambiente_tectonico,
          ROUND(CAST(SUM(ST_Area(ST_Intersection(
            CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
            ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
          ))) / ST_Area(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})) * 100 AS numeric), 2) as percentual
        FROM geologia_litologia.litoestratigafia_br
        WHERE ST_Intersects(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )
        GROUP BY "NOME", "SIGLA", "LITOTIPOS", "ERA_MIN", "AMBIENTE_TECTONICO"
        ORDER BY percentual DESC
      `, [geojsonStr]);
      analyses['9.5_geologia'].data = geologiaResult.rows;
    } catch(e) { console.error('Geologia query error:', e.message); }

    // 9.6 Mineração (ANM processes + ocorrências)
    analyses['9.6_mineracao'] = {
      nome: 'Mineração',
      processes: [],
      occurrences: [],
    };

    try {
      let anmResult = await pool.query(`
        SELECT
          "PROCESSO" as numero_processo,
          "FASE"     as fase,
          "NOME"     as titular,
          "SUBS"     as substancia
        FROM anm.anm_mt
        WHERE ST_Intersects(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )
      `, [geojsonStr]);
      analyses['9.6_mineracao'].processes = anmResult.rows;
    } catch(e) { console.error('ANM query error:', e.message); }
    analyses['9.6_mineracao'].occurrences = [];

    // 9.7 Embargos (IBAMA)
    analyses['9.7_embargos'] = {
      nome: 'Embargos',
      data: [],
    };

    try {
      let embargosResult = await pool.query(`
        SELECT
          nome_embar,
          nome_imove,
          dat_embarg,
          qtd_area_e,
          des_infrac,
          operacao
        FROM embargos.embargos_ibama
        WHERE ST_Intersects(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )
      `, [geojsonStr]);
      analyses['9.7_embargos'].data = embargosResult.rows;
    } catch(e) { console.error('Embargos query error:', e.message); }

    // 9.8 Terras Indígenas
    analyses['9.8_terras_indigenas'] = {
      nome: 'Terras Indígenas',
      data: [],
    };

    try {
      let tisResult = await pool.query(`
        SELECT
          terrai_nome as nome,
          etnia_nome as etnia,
          fase_ti,
          superficie_perimetro_ha as superficie,
          ROUND(CAST(
            ST_Area(ST_Intersection(
              geom,
              ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
            )) / ST_Area(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})) * 100
          AS numeric), 2) as percentual_sobreposicao
        FROM terra_indigena.poligonais_portarias
        WHERE ST_Intersects(
          geom,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )
        ORDER BY percentual_sobreposicao DESC
      `, [geojsonStr]);
      analyses['9.8_terras_indigenas'].data = tisResult.rows;
    } catch(e) { console.error('TIs query error:', e.message); }

    // 9.9 Unidades de Conservação
    analyses['9.9_ucs'] = {
      nome: 'Unidades de Conservação',
      data: [],
    };

    try {
      let ucsResult = await pool.query(`
        SELECT
          "NOME_UC1"  as nome,
          "CATEGORI3" as categoria,
          "ESFERA5"   as esfera,
          "ANO_CRIA6" as ano_criacao,
          "ATO_LEGA9" as ato_legal,
          ROUND(CAST(
            ST_Area(ST_Intersection(
              CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
              ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
            )) / ST_Area(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})) * 100
          AS numeric), 2) as percentual_sobreposicao
        FROM unidade_conservacao.unidade_conserv
        WHERE ST_Intersects(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )
        ORDER BY percentual_sobreposicao DESC
      `, [geojsonStr]);
      analyses['9.9_ucs'].data = ucsResult.rows;
    } catch(e) { console.error('UCs query error:', e.message); }

    // 9.10 Hidrografia (bacias, cursos d'água, "rica em água" flag)
    analyses['9.10_hidrografia'] = {
      nome: 'Hidrografia',
      bacias: [],
      cursos_agua_count: 0,
      intensidade_hidrica: 'Sem drenagem superficial',
    };

    // ── Bacias Hidrográficas (nível 6 → 2, do mais específico ao mais geral) ──
    try {
      const baciasResult = await pool.query(`
        WITH parcela AS (
          SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}) as geom
        ),
        area_parcela AS (
          SELECT ST_Area(geom::geography) as area FROM parcela
        ),
        niveis AS (
          SELECT 6 as nivel, b.nome_bacia, b.curso_prin, b.princ_aflu, b.sub_bacias, b.suprabacia,
            ROUND((ST_Area(ST_Intersection(b.geom, p.geom)::geography) / a.area * 100)::numeric, 1) as pct_parcela
          FROM bacias_hidrograficas.bacias_nivel_6 b CROSS JOIN parcela p CROSS JOIN area_parcela a
          WHERE ST_Intersects(b.geom, p.geom)
          UNION ALL
          SELECT 5, b.nome_bacia, b.curso_prin, b.princ_aflu, b.sub_bacias, b.suprabacia,
            ROUND((ST_Area(ST_Intersection(b.geom, p.geom)::geography) / a.area * 100)::numeric, 1)
          FROM bacias_hidrograficas.bacias_nivel_5 b CROSS JOIN parcela p CROSS JOIN area_parcela a
          WHERE ST_Intersects(b.geom, p.geom)
          UNION ALL
          SELECT 4, b.nome_bacia, b.curso_prin, b.princ_aflu, b.sub_bacias, b.suprabacia,
            ROUND((ST_Area(ST_Intersection(b.geom, p.geom)::geography) / a.area * 100)::numeric, 1)
          FROM bacias_hidrograficas.bacias_nivel_4 b CROSS JOIN parcela p CROSS JOIN area_parcela a
          WHERE ST_Intersects(b.geom, p.geom)
          UNION ALL
          SELECT 3, b.nome_bacia, b.curso_prin, b.princ_aflu, b.sub_bacias, b.suprabacia,
            ROUND((ST_Area(ST_Intersection(b.geom, p.geom)::geography) / a.area * 100)::numeric, 1)
          FROM bacias_hidrograficas.bacias_nivel_3 b CROSS JOIN parcela p CROSS JOIN area_parcela a
          WHERE ST_Intersects(b.geom, p.geom)
          UNION ALL
          SELECT 2, b.nome_bacia, b.curso_prin, b.princ_aflu, b.sub_bacias, b.suprabacia,
            ROUND((ST_Area(ST_Intersection(b.geom, p.geom)::geography) / a.area * 100)::numeric, 1)
          FROM bacias_hidrograficas.bacias_nivel_2 b CROSS JOIN parcela p CROSS JOIN area_parcela a
          WHERE ST_Intersects(b.geom, p.geom)
        )
        SELECT * FROM niveis ORDER BY nivel DESC, pct_parcela DESC
      `, [geojsonStr]);

      // Lógica: do nível 6 ao 2 — se parcela 100% em uma só bacia num nível, usa só ela
      const rows = baciasResult.rows;
      const porNivel = {};
      for (const r of rows) {
        if (!porNivel[r.nivel]) porNivel[r.nivel] = [];
        porNivel[r.nivel].push(r);
      }

      let baciasFinais = [];
      for (const nivel of [6, 5, 4, 3, 2]) {
        const grupo = porNivel[nivel] || [];
        if (grupo.length === 0) continue;

        if (grupo.length === 1 && parseFloat(grupo[0].pct_parcela) >= 98) {
          // Parcela totalmente dentro de uma bacia nesse nível — só mostra essa
          baciasFinais = [{ ...grupo[0], pct_parcela: null }];
          break;
        } else {
          // Parcela dividida — mostra todas com percentual e continua para níveis mais gerais
          baciasFinais.push(...grupo);
        }
      }

      analyses['9.10_hidrografia'].bacias = baciasFinais;
    } catch(e) {
      console.error('Bacias query error:', e.message);
      analyses['9.10_hidrografia'].bacias = [];
    }

    // Cursos d'água BHO — contagem dentro da parcela + padrão de drenagem em raio de 25km
    let rp = null;
    let totalParcela = 0;

    // ── Passo A: cursos d'água com dois buffers:
    //   - 250m: "dentro da parcela" — captura rios de divisa que correm no limite
    //   - 350m: área de influência — rios próximos fora da propriedade
    try {
      const cursosParcelaResult = await pool.query(`
        WITH
        parcela AS (
          SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}) as geom
        ),
        -- 250m: pega rios que correm dentro ou como divisa da parcela
        area_250m AS (
          SELECT ST_Buffer(p.geom::geography, 250)::geometry as geom
          FROM parcela p
        ),
        -- 350m: área de influência hídrica mais ampla
        area_350m AS (
          SELECT ST_Buffer(p.geom::geography, 350)::geometry as geom
          FROM parcela p
        ),
        cursos AS (
          SELECT
            nuordemcda::integer as ordem,
            ST_Intersection(c.geom, b250.geom) as geom_250m,
            ST_Intersection(c.geom, b350.geom) as geom_350m
          FROM hidrografia.geoft_bho_2017_curso_dagua c
          CROSS JOIN area_250m  b250
          CROSS JOIN area_350m  b350
          WHERE ST_Intersects(c.geom, b350.geom)
            AND NOT ST_IsEmpty(c.geom)
        )
        SELECT
          COUNT(CASE WHEN NOT ST_IsEmpty(geom_250m) THEN 1 END) as total_cursos,
          ROUND(SUM(CASE WHEN NOT ST_IsEmpty(geom_250m)
            THEN ST_Length(geom_250m::geography) / 1000 ELSE 0 END)::numeric, 2) as comprimento_km,
          ROUND(SUM(CASE WHEN NOT ST_IsEmpty(geom_350m)
            THEN ST_Length(geom_350m::geography) / 1000 ELSE 0 END)::numeric, 2) as comprimento_influencia_km,
          COUNT(CASE WHEN ordem = 1 THEN 1 END)                  as ordem1,
          COUNT(CASE WHEN ordem = 2 THEN 1 END)                  as ordem2,
          COUNT(CASE WHEN ordem >= 3 THEN 1 END)                 as ordem3plus
        FROM cursos
      `, [geojsonStr]);

      rp = cursosParcelaResult.rows[0];
      totalParcela = parseInt(rp.total_cursos) || 0;
      const comprimentoKm = parseFloat(rp.comprimento_km) || 0;
      analyses['9.10_hidrografia'].comprimento_influencia_km = parseFloat(rp.comprimento_influencia_km) || 0;
      analyses['9.10_hidrografia'].cursos_agua_count = totalParcela;

      // Classificação por contagem + comprimento
      let intensidadeHidrica = 'Sem drenagem superficial';
      if (totalParcela === 0) {
        intensidadeHidrica = 'Sem drenagem superficial';
      } else if (totalParcela === 1 || comprimentoKm < 1) {
        intensidadeHidrica = 'Drenagem incipiente';
      } else if (totalParcela <= 3 && comprimentoKm < 8) {
        intensidadeHidrica = 'Drenagem moderada';
      } else if (totalParcela <= 6 || comprimentoKm <= 20) {
        intensidadeHidrica = 'Bem servida de água';
      } else {
        intensidadeHidrica = 'Rica em água';
      }
      analyses['9.10_hidrografia'].intensidade_hidrica = intensidadeHidrica;

      // ── Nomes dos rios que cruzam a parcela (via rio_nomes.NORIOCOMP) ──
      const nomesResult = await pool.query(`
        SELECT DISTINCT "NORIOCOMP" as nome
        FROM hidrografia.rio_nomes
        WHERE ST_Intersects(
          geom,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )
          AND "NORIOCOMP" IS NOT NULL
          AND TRIM("NORIOCOMP") <> ''
        ORDER BY "NORIOCOMP"
      `, [geojsonStr]);

      analyses['9.10_hidrografia'].nomes_rios = nomesResult.rows.map(r => r.nome);
    } catch(e) { console.error('Cursos parcela (Passo A) error:', e.message); }

    // ── Passo B: padrão de drenagem usando raio de 25km ou o próprio polígono se maior ──
    try {
      const cursosRaioResult = await pool.query(`
        WITH parcela_geom AS (
          SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}) as geom
        ),
        area_info AS (
          -- Calcula a diagonal do bounding box da parcela em metros
          SELECT
            geom,
            ST_Distance(
              ST_SetSRID(ST_Point(ST_XMin(ST_Envelope(geom)), ST_YMin(ST_Envelope(geom))), ${SRID})::geography,
              ST_SetSRID(ST_Point(ST_XMax(ST_Envelope(geom)), ST_YMax(ST_Envelope(geom))), ${SRID})::geography
            ) as diagonal_m
          FROM parcela_geom
        ),
        cursos_raw AS (
          -- Se o polígono ultrapassar 25km de diagonal, usa o próprio polígono como área de análise
          -- Caso contrário, usa buffer de 25km ao redor do centroide
          SELECT
            nuordemcda::integer as ordem,
            c.geom
          FROM hidrografia.geoft_bho_2017_curso_dagua c, area_info ai
          WHERE c.geom && ST_Expand(ai.geom, 0.25)
            AND CASE
              WHEN ai.diagonal_m > 25000 THEN
                ST_Intersects(c.geom, ai.geom)
              ELSE
                ST_DWithin(c.geom::geography, ST_Centroid(ai.geom)::geography, 25000)
            END
            AND NOT ST_IsEmpty(c.geom)
        ),
        -- ST_Dump decompõe MultiLineString/GeometryCollection em partes atômicas
        cursos AS (
          SELECT
            ordem,
            (ST_Dump(geom)).geom as geom_line
          FROM cursos_raw
        ),
        azimuths AS (
          SELECT
            ordem,
            ST_Length(geom_line::geography) as comprimento_m,
            -- normaliza azimute para 0-180 (ignora sentido do fluxo)
            MOD(CAST(degrees(ST_Azimuth(
              ST_StartPoint(geom_line),
              ST_EndPoint(geom_line)
            )) + 180 AS numeric), 180) as azimuth
          FROM cursos
          WHERE ST_NPoints(geom_line) >= 2
            AND NOT ST_IsEmpty(geom_line)
            -- garante que ST_StartPoint/ST_EndPoint recebam apenas LineString pura
            AND ST_GeometryType(geom_line) = 'ST_LineString'
        )
        SELECT
          COUNT(*)                                              as total_raio,
          MAX(ordem)                                            as ordem_maxima,
          COUNT(DISTINCT ordem)                                 as num_ordens,
          ROUND(AVG(azimuth)::numeric, 1)                      as azimuth_medio,
          ROUND(COALESCE(STDDEV(azimuth), 0)::numeric, 1)      as desvio_azimuth
        FROM azimuths
      `, [geojsonStr]);

      const rr = cursosRaioResult.rows[0];
      const totalRaio = parseInt(rr.total_raio) || 0;

      if (totalRaio > 0) {
        const desvio    = parseFloat(rr.desvio_azimuth) || 0;
        const ordemMax  = parseInt(rr.ordem_maxima) || 1;
        const numOrdens = parseInt(rr.num_ordens) || 1;

        let padrao   = 'Dendrítico';
        let descricao = 'Afluentes em múltiplas direções sem orientação preferencial — típico de rochas homogêneas e relevo suave.';

        if (desvio < 20) {
          padrao    = 'Paralelo';
          descricao = 'Cursos correm em direções aproximadamente paralelas — indica forte declividade ou estruturas geológicas lineares.';
        } else if (desvio >= 20 && desvio < 35 && numOrdens <= 2) {
          padrao    = 'Subdendrítico';
          descricao = 'Padrão dendrítico com leve tendência direcional — relevo suave com alguma orientação geológica.';
        } else if (desvio >= 20 && desvio < 35 && ordemMax >= 3) {
          padrao    = 'Treliça';
          descricao = 'Afluentes encontram o curso principal em ângulos próximos a 90° — controlado por falhas ou fraturas geológicas.';
        } else if (desvio >= 35 && desvio < 55 && ordemMax >= 4) {
          padrao    = 'Subdendrítico a Dendrítico';
          descricao = 'Rede com hierarquia bem desenvolvida e direções variadas — relevo dissecado com leve controle estrutural.';
        } else if (desvio >= 55) {
          padrao    = 'Dendrítico';
          descricao = 'Alta variação direcional dos cursos — típico de substrato rochoso homogêneo sem controle estrutural dominante.';
        }

        analyses['9.10_hidrografia'].padrao_drenagem = {
          padrao,
          descricao,
          total_cursos:   totalParcela,
          ordem_maxima:   ordemMax,
          comprimento_km: rp ? (parseFloat(rp.comprimento_km) || 0) : 0,
          ordem1:         rp ? (parseInt(rp.ordem1) || 0) : 0,
          ordem2:         rp ? (parseInt(rp.ordem2) || 0) : 0,
          ordem3plus:     rp ? (parseInt(rp.ordem3plus) || 0) : 0,
          raio_analise_km: 25,
          total_raio: totalRaio,
          desvio_azimuth: desvio,
        };
      } else if (totalParcela > 0) {
        // Raio não retornou nada (improvável), usa dados da parcela como fallback
        analyses['9.10_hidrografia'].padrao_drenagem = {
          padrao: 'Indeterminado',
          descricao: 'Dados insuficientes no raio de análise para classificar o padrão de drenagem.',
          total_cursos: totalParcela,
          ordem_maxima: rp && parseInt(rp.ordem1) > 0 ? 1 : 0,
          comprimento_km: rp ? (parseFloat(rp.comprimento_km) || 0) : 0,
          ordem1: rp ? (parseInt(rp.ordem1) || 0) : 0,
          ordem2: rp ? (parseInt(rp.ordem2) || 0) : 0,
          ordem3plus: rp ? (parseInt(rp.ordem3plus) || 0) : 0,
          raio_analise_km: 25,
          total_raio: 0,
          desvio_azimuth: 0,
        };
      }
    } catch(e) { console.error('Cursos agua query error:', e.message); }

    // 9.11 Altitude (raster via ST_PixelAsPoints)
    analyses['9.11_altitude'] = {
      nome: 'Altitude',
      min_m: null,
      max_m: null,
      media_m: null,
      ponto_m: null,
    };

    try {
      const altitudeResult = await pool.query(`
        SELECT
          ROUND(CAST((stats).min  AS numeric), 1) AS min_alt,
          ROUND(CAST((stats).max  AS numeric), 1) AS max_alt,
          ROUND(CAST((stats).mean AS numeric), 1) AS avg_alt
        FROM (
          SELECT ST_SummaryStatsAgg(ST_Clip(r.rast, geom.geom, TRUE), 1, TRUE) AS stats
          FROM altitude_br.altitude_raster r
          CROSS JOIN (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}) AS geom) geom
          WHERE ST_Intersects(r.rast, geom.geom)
          GROUP BY geom.geom
        ) sub
      `, [geojsonStr]);
      if (altitudeResult.rows[0]) {
        analyses['9.11_altitude'].min_m   = altitudeResult.rows[0].min_alt;
        analyses['9.11_altitude'].max_m   = altitudeResult.rows[0].max_alt;
        analyses['9.11_altitude'].media_m = altitudeResult.rows[0].avg_alt;
      }
    } catch(e) { console.error('Altitude query error:', e.message); }

    // 9.12 Carbono (raster via carbono_solo.carbono_2024 usando ST_SummaryStatsAgg + ST_Clip)
    analyses['9.12_carbono'] = {
      nome: 'Carbono',
      min_t_ha: null,
      max_t_ha: null,
      medio_t_ha: null,
      total_toneladas: null,
    };

    try {
      const carbonoResult = await pool.query(`
        SELECT
          ROUND(CAST(
            (ST_SummaryStatsAgg(ST_Clip(r.rast, geom.geom, TRUE), 1, TRUE)).mean
            * (ST_Area(geom.geom::geography) / 10000.0)
          AS numeric), 2) AS estoque_total_t
        FROM carbono_solo.carbono_2024 r
        CROSS JOIN (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}) AS geom) geom
        WHERE ST_Intersects(r.rast, geom.geom)
        GROUP BY geom.geom
      `, [geojsonStr]);
      if (carbonoResult.rows[0]) {
        analyses['9.12_carbono'].total_toneladas = carbonoResult.rows[0].estoque_total_t;
      }
    } catch(e) { console.error('Carbono query error:', e.message); }

    // 9.13 CAR (área, APP, reserva legal, vegetação nativa)
    analyses['9.13_car'] = {
      nome: 'CAR',
      area_imovel: [],
      app_area_hectares: 0,
      app_detalhes: [],
      reserva_legal_hectares: 0,
      vegetacao_nativa_hectares: 0,
      veg_nativa_proposta_ha: 0,
    };

    try {
      // Para cada parcela individualmente calcula o centroide e busca o CAR correspondente.
      // Usa sicar.area_imovel (todos os estados) se disponível, senão car.area_imovelmt (só MT).
      const carParams = individualGeometries.map(g => JSON.stringify(g));
      const carConds = carParams.map((_, i) =>
        `ST_Contains(CASE WHEN ST_SRID(c.geom)=0 THEN ST_SetSRID(c.geom,${SRID}) ELSE c.geom END, ST_SetSRID(ST_Centroid(ST_GeomFromGeoJSON($${i+1}::jsonb)), ${SRID}))`
      ).join(' OR ');
      const carTable = sicarDisponivel ? 'sicar.area_imovel' : 'car.area_imovelmt';
      let carAreaResult = await pool.query(`
        SELECT DISTINCT ON (c.cod_imovel)
          c.cod_imovel,
          c.num_area,
          c.ind_tipo,
          c.ind_status,
          c.des_condic,
          c.dat_criaca,
          c.dat_atuali
        FROM ${carTable} c
        WHERE (${carConds})
          AND c.des_condic NOT ILIKE '%cancelado%'
      `, carParams);
      analyses['9.13_car'].area_imovel = carAreaResult.rows;
    } catch(e) { console.error('CAR area query error:', e.message); }

    // Extrai os cod_imovel de TODOS os CARs encontrados para usar nas queries de APP/RL/VN
    const carCodes = (analyses['9.13_car'].area_imovel || []).map(r => r.cod_imovel).filter(Boolean);
    console.log(`[CAR] ${carCodes.length} imóvel(is) encontrado(s): ${carCodes.join(', ')}`);

    try {
      // APP: busca para TODOS os cod_imovel encontrados (não só o centroide do union).
      // Agrupa detalhes por nom_tema somando as áreas de todos os CARs.
      if (carCodes.length > 0) {
        // Usa sicar.apps (tabela unificada multi-estado) ou as tabelas MT legadas
        const appsCTE = sicarAppsUnificado
          ? `SELECT nom_tema, num_area FROM sicar.apps WHERE cod_imovel = ANY($1)`
          : `SELECT nom_tema, num_area FROM car.apps1_mt WHERE cod_imovel = ANY($1)
             UNION ALL
             SELECT nom_tema, num_area FROM car.apps2_mt WHERE cod_imovel = ANY($1)
             UNION ALL
             SELECT nom_tema, num_area FROM car.apps3_mt WHERE cod_imovel = ANY($1)
             UNION ALL
             SELECT nom_tema, num_area FROM car.apps4_mt WHERE cod_imovel = ANY($1)`;
        let appResult = await pool.query(`
          WITH todas_apps AS (
            ${appsCTE}
          ),
          sem_total AS (
            SELECT nom_tema, num_area FROM todas_apps WHERE nom_tema NOT ILIKE '%total%'
          )
          SELECT
            ROUND(CAST(SUM(num_area) AS numeric), 4) as area_hectares,
            json_agg(
              json_build_object('nom_tema', nom_tema, 'num_area', ROUND(CAST(num_area AS numeric), 4))
              ORDER BY nom_tema
            ) as detalhes
          FROM sem_total
        `, [carCodes]);
        if (appResult.rows[0] && appResult.rows[0].area_hectares) {
          analyses['9.13_car'].app_area_hectares = appResult.rows[0].area_hectares;
          analyses['9.13_car'].app_detalhes       = appResult.rows[0].detalhes || [];
        }
      }
    } catch(e) { console.error('CAR APP query error:', e.message); }

    try {
      // Reserva Legal: filtra por TODOS os cod_imovel encontrados (sem centroide LIMIT 1)
      if (carCodes.length > 0) {
        let reservaLegalResult = await pool.query(`
          WITH parcela AS (
            SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}) as geom
          )
          SELECT
            ROUND(CAST(SUM(ST_Area(ST_Intersection(
              CASE WHEN ST_SRID(rl.geom) = 0 THEN ST_SetSRID(rl.geom, ${SRID}) ELSE rl.geom END,
              p.geom
            ))) / 10000 AS numeric), 2) as area_hectares,
            ROUND(CAST(SUM(rl.num_area) AS numeric), 2) as reserva_proposta_ha
          FROM ${sicarDisponivel ? 'sicar.reserva_legal' : 'car.reserva_legal'} rl
          CROSS JOIN parcela p
          WHERE rl.cod_imovel = ANY($2)
        `, [geojsonStr, carCodes]);
        if (reservaLegalResult.rows[0]) {
          analyses['9.13_car'].reserva_legal_hectares = reservaLegalResult.rows[0].area_hectares || 0;
          analyses['9.13_car'].reserva_proposta_ha    = reservaLegalResult.rows[0].reserva_proposta_ha || 0;
        }
      }
    } catch(e) { console.error('CAR Reserva Legal query error:', e.message); }

    try {
      // Vegetação Nativa: filtra por TODOS os cod_imovel encontrados
      if (carCodes.length > 0) {
        let vegNativaResult = await pool.query(`
          WITH parcela AS (
            SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}) as geom
          )
          SELECT
            ROUND(CAST(SUM(ST_Area(ST_Intersection(
              CASE WHEN ST_SRID(vn.geom) = 0 THEN ST_SetSRID(vn.geom, ${SRID}) ELSE vn.geom END,
              p.geom
            ))) / 10000 AS numeric), 2) as area_hectares,
            ROUND(CAST(SUM(vn.num_area) AS numeric), 2) as veg_nativa_proposta_ha
          FROM ${sicarDisponivel ? 'sicar.vegetacao_nativa' : 'car."vegetação_nativa"'} vn
          CROSS JOIN parcela p
          WHERE vn.cod_imovel = ANY($2)
        `, [geojsonStr, carCodes]);
        if (vegNativaResult.rows[0] && vegNativaResult.rows[0].area_hectares) {
          analyses['9.13_car'].vegetacao_nativa_hectares = vegNativaResult.rows[0].area_hectares;
          analyses['9.13_car'].veg_nativa_proposta_ha    = vegNativaResult.rows[0].veg_nativa_proposta_ha || 0;
        }
      }
    } catch(e) { console.error('CAR Veg Nativa query error:', e.message); }

    // 9.13b Aquíferos — 3 queries independentes para não quebrar se alguma tabela não existir
    analyses['9.13b_aquiferos'] = [];

    const aquiferoTables = [
      { tipo: 'Cárstico',  schema: 'aquifero_br', table: 'aquiferos_carstico'  },
      { tipo: 'Fraturado', schema: 'aquifero_br', table: 'aquiferos_fraturado' },
      { tipo: 'Poroso',    schema: 'aquifero_br', table: 'aquiferos_poroso'    },

    ];

    for (const aq of aquiferoTables) {
      try {
        const r = await pool.query(`
          SELECT $2 as tipo_aquifero,
            "SAQ_NM_NOM" as nome,
            "SAQ_TP_DOM" as dominio,
            "SAQ_DS_LEG" as descricao,
            "SAQ_AR_KM2" as area_km2
          FROM "${aq.schema}"."${aq.table}"
          WHERE ST_Intersects(
            CASE WHEN ST_SRID(geom)=0 THEN ST_SetSRID(geom,${SRID}) ELSE geom END,
            ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
          )
        `, [geojsonStr, aq.tipo]);
        console.log(`[Aquifero] ${aq.tipo}: ${r.rows.length} resultado(s)`);
        analyses['9.13b_aquiferos'].push(...r.rows);
      } catch(e) { console.error(`[Aquifero] ERRO ${aq.tipo}: ${e.message}`); }
    }
    console.log(`[Aquifero] Total retornado: ${analyses['9.13b_aquiferos'].length}`);

    // 9.14 Análises Adicionais (Geologia + Tectônica)
    analyses['9.14_analises_adicionais'] = {
      nome: 'Análises Adicionais',
      geologia: {
        pontos: [],
        linhas_dobra: [],
        linhas_falha: [],
        linhas_fratura: [],
      },
      tectonicas: [],
    };

    try {
      let geolPontoResult = await pool.query(`
        SELECT tipo, descricao
        FROM cprm.geol_ponto
        WHERE ST_Intersects(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )
      `, [geojsonStr]);
      analyses['9.14_analises_adicionais'].geologia.pontos = geolPontoResult.rows;
    } catch(e) { /* tabela cprm.geol_ponto não disponível */ }

    try {
      let geolLinhaDbraResult = await pool.query(`
        SELECT tipo, orientacao
        FROM cprm.geol_linha_dobra
        WHERE ST_Intersects(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )
      `, [geojsonStr]);
      analyses['9.14_analises_adicionais'].geologia.linhas_dobra = geolLinhaDbraResult.rows;
    } catch(e) { /* tabela cprm.geol_linha_dobra não disponível */ }

    try {
      let geolLinhaFalhaResult = await pool.query(`
        SELECT tipo, comprimento_m
        FROM cprm.geol_linha_falha
        WHERE ST_Intersects(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )
      `, [geojsonStr]);
      analyses['9.14_analises_adicionais'].geologia.linhas_falha = geolLinhaFalhaResult.rows;
    } catch(e) { /* tabela cprm.geol_linha_falha não disponível */ }

    try {
      let geolLinhaFraturaResult = await pool.query(`
        SELECT tipo, comprimento_m
        FROM cprm.geol_linha_fratura
        WHERE ST_Intersects(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )
      `, [geojsonStr]);
      analyses['9.14_analises_adicionais'].geologia.linhas_fratura = geolLinhaFraturaResult.rows;
    } catch(e) { /* tabela cprm.geol_linha_fratura não disponível */ }

    try {
      let tectonicaResult = await pool.query(`
        SELECT nome, tipo
        FROM cprm.tectonic_map
        WHERE ST_DWithin(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}),
          0.1
        )
      `, [geojsonStr]);
      analyses['9.14_analises_adicionais'].tectonicas = tectonicaResult.rows;
    } catch(e) { /* tabela cprm.tectonic_map não disponível */ }

    // ── Pluviometria (ANA HidroWeb SNIRH — requer HIDROWEB_TOKEN no .env) ───
    analyses['pluviometria'] = { resumo: null, media_mensal: [], total_anual: [], pendente: false };

    const hidroweb_token = process.env.HIDROWEB_TOKEN;

    if (!hidroweb_token) {
      // Token ainda não configurado — informa no painel sem bloquear as demais análises
      analyses['pluviometria'].pendente = true;
      console.log('[Pluviometria] HIDROWEB_TOKEN não configurado — aguardando cadastro ANA.');
    } else {
      try {
        const centroidPluv = await pool.query(`
          SELECT
            ST_Y(ST_Centroid(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}))) AS lat,
            ST_X(ST_Centroid(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}))) AS lng
        `, [geojsonStr]);

        if (centroidPluv.rows[0]) {
          const { lat, lng } = centroidPluv.rows[0];
          const uf = municipio.uf || 'MT';

          const authHeaders = {
            'Authorization': hidroweb_token,
            'Accept': 'application/json',
          };

          // 1. Inventário de estações pluviométricas do estado
          const invUrl = `https://www.snirh.gov.br/hidroweb/rest/api/estacao/lista`
            + `?tipoEstacao=P&sgUF=${uf}&nmEstacao=&maximoResultados=2000&offset=0`;
          const invResp = await fetch(invUrl, { headers: authHeaders, signal: AbortSignal.timeout(25000) });
          const todasEstacoes = await invResp.json();
          console.log(`[Pluviometria] SNIRH HTTP ${invResp.status}, ${Array.isArray(todasEstacoes) ? todasEstacoes.length : '?'} estações em ${uf}`);

          if (!Array.isArray(todasEstacoes) || todasEstacoes.length === 0) {
            throw new Error(`SNIRH retornou: ${JSON.stringify(todasEstacoes).slice(0, 200)}`);
          }

          // 2. Estação mais próxima
          const comDistancia = todasEstacoes
            .filter(s => s.latitude && s.longitude)
            .map(s => ({
              ...s,
              distancia_km: haversineKm(parseFloat(lat), parseFloat(lng), parseFloat(s.latitude), parseFloat(s.longitude)),
            }))
            .sort((a, b) => a.distancia_km - b.distancia_km);

          const estMaisProxima = comDistancia[0];
          if (!estMaisProxima) throw new Error('Nenhuma estação com coordenadas válidas');

          console.log(`[Pluviometria] Estação mais próxima: ${estMaisProxima.nmEstacao} (${estMaisProxima.distancia_km} km)`);

          // 3. Série histórica mensal (últimos 10 anos)
          const anoAtual   = new Date().getFullYear();
          const dataInicio = `${anoAtual - 10}-01-01`;
          const dataFim    = `${anoAtual - 1}-12-31`;

          const serieUrl = `https://www.snirh.gov.br/hidroweb/rest/api/documento/gerarSerie`
            + `?codigoEstacao=${estMaisProxima.codigoEstacao}&dataInicio=${dataInicio}&dataFim=${dataFim}&tipoDado=CHUVA`;
          const serieResp = await fetch(serieUrl, { headers: authHeaders, signal: AbortSignal.timeout(30000) });
          const serie = await serieResp.json();
          console.log(`[Pluviometria] Série HTTP ${serieResp.status}, registros: ${Array.isArray(serie) ? serie.length : JSON.stringify(serie).slice(0,100)}`);

          if (!Array.isArray(serie) || serie.length === 0) throw new Error('Série histórica vazia');

          // 4. Agregar por mês e por ano
          const MESES = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'];
          const anoTotais    = {};
          const mesTotais    = new Array(12).fill(0);
          const mesContagens = new Array(12).fill(0);

          for (const rec of serie) {
            // O campo de data pode ser dataHora, data, ou similar — tentar os mais comuns
            const dataStr = rec.dataHora || rec.data || rec.dtMedicao || '';
            if (!dataStr) continue;
            const d   = new Date(dataStr);
            const ano = d.getFullYear();
            const mes = d.getMonth();
            const mm  = parseFloat(rec.chuva ?? rec.valor ?? rec.total ?? 0) || 0;
            anoTotais[ano]   = (anoTotais[ano] || 0) + mm;
            mesTotais[mes]   += mm;
            mesContagens[mes]++;
          }

          const mediaMensal = MESES.map((m, i) => ({
            mes:      m,
            media_mm: mesContagens[i] > 0 ? Math.round(mesTotais[i] / mesContagens[i]) : 0,
          }));

          const totalAnual = Object.keys(anoTotais).sort().map(a => ({
            ano: a, total_mm: Math.round(anoTotais[a]),
          }));

          const anuaisValores = Object.values(anoTotais).filter(v => v > 0);
          const mediaAnual    = anuaisValores.length > 0
            ? Math.round(anuaisValores.reduce((a, b) => a + b, 0) / anuaisValores.length) : 0;

          const mesMaisChuvoso = mediaMensal.reduce((a, b) => b.media_mm > a.media_mm ? b : a);
          const mesMaisSeco    = mediaMensal.reduce((a, b) => b.media_mm < a.media_mm ? b : a);

          analyses['pluviometria'].media_mensal = mediaMensal;
          analyses['pluviometria'].total_anual  = totalAnual.slice(-10);
          analyses['pluviometria'].resumo = {
            media_anual_mm:   mediaAnual,
            mes_mais_chuvoso: mesMaisChuvoso,
            mes_mais_seco:    mesMaisSeco,
            fonte:            `ANA HidroWeb — ${estMaisProxima.nmEstacao} (${estMaisProxima.distancia_km} km)`,
            latitude:         parseFloat(estMaisProxima.latitude),
            longitude:        parseFloat(estMaisProxima.longitude),
          };
          console.log(`[Pluviometria] OK — ${estMaisProxima.nmEstacao}, média anual: ${mediaAnual} mm`);
        }
      } catch(e) {
        console.error('[Pluviometria] Erro com token SNIRH:', e.message);
        analyses['pluviometria'].erro = e.message;
      }
    }
    // ────────────────────────────────────────────────────────────────────────

    // Map internal analyses structure to the AnaliseResultados shape expected by the Angular model
    const resultados = {
      fundiaria: {
        sigef: analyses['9.1_fundiaria'].sigef || [],
        snci:  analyses['9.1_fundiaria'].snci  || [],
        car:   analyses['9.1_fundiaria'].car   || [],
      },
      registral: {
        encontrado: (analyses['9.2_registral'].serventias || []).length > 0,
        cartorios:  analyses['9.2_registral'].serventias || [],
        municipio:  { nome: municipio.municipio, uf: municipio.uf },
        mensagem:   analyses['9.2_registral'].message || '',
      },
      solo:              analyses['9.3_solo'].data  || [],
      bioma:             analyses['9.4_bioma'].data || [],
      geologia:          analyses['9.5_geologia'].data || [],
      mineracao: {
        processos_anm: analyses['9.6_mineracao'].processes   || [],
        ocorrencias:   analyses['9.6_mineracao'].occurrences || [],
      },
      embargos:          analyses['9.7_embargos'].data         || [],
      terras_indigenas:  analyses['9.8_terras_indigenas'].data || [],
      unidades_conservacao: analyses['9.9_ucs'].data           || [],
      hidrografia: {
        bacias:             analyses['9.10_hidrografia'].bacias              || [],
        cursos_dagua_count: analyses['9.10_hidrografia'].cursos_agua_count  || 0,
        intensidade_hidrica: analyses['9.10_hidrografia'].intensidade_hidrica || 'Sem drenagem superficial',
        comprimento_influencia_km: analyses['9.10_hidrografia'].comprimento_influencia_km || 0,
        padrao_drenagem:    analyses['9.10_hidrografia'].padrao_drenagem     || null,
        nomes_rios:         analyses['9.10_hidrografia'].nomes_rios          || [],
      },
      altitude: {
        altitude_min:   analyses['9.11_altitude'].min_m   || null,
        altitude_max:   analyses['9.11_altitude'].max_m   || null,
        altitude_media: analyses['9.11_altitude'].media_m || null,
        altitude_ponto: analyses['9.11_altitude'].ponto_m || null,
      },
      carbono: {
        min_t_ha:               analyses['9.12_carbono'].min_t_ha        || null,
        max_t_ha:               analyses['9.12_carbono'].max_t_ha        || null,
        medio_t_ha:             analyses['9.12_carbono'].medio_t_ha      || null,
        estoque_total_toneladas: analyses['9.12_carbono'].total_toneladas || null,
      },
      car: {
        imoveis:                  analyses['9.13_car'].area_imovel              || [],
        area_app_ha:              analyses['9.13_car'].app_area_hectares         || 0,
        app_detalhes:             analyses['9.13_car'].app_detalhes              || [],
        area_reserva_legal_ha:    analyses['9.13_car'].reserva_legal_hectares    || 0,
        reserva_proposta_ha:      analyses['9.13_car'].reserva_proposta_ha       || 0,
        area_vegetacao_nativa_ha: analyses['9.13_car'].vegetacao_nativa_hectares || 0,
        veg_nativa_proposta_ha:   analyses['9.13_car'].veg_nativa_proposta_ha    || 0,
      },
      aquiferos:           analyses['9.13b_aquiferos']          || [],
      analises_adicionais: analyses['9.14_analises_adicionais'] || {},
      pluviometria: {
        resumo:       analyses['pluviometria']?.resumo       || null,
        media_mensal: analyses['pluviometria']?.media_mensal || [],
        total_anual:  analyses['pluviometria']?.total_anual  || [],
        pendente:     analyses['pluviometria']?.pendente     || false,
        erro:         analyses['pluviometria']?.erro         || null,
      },
      municipio: { CD_MUN: '', NM_MUN: municipio.municipio, SIGLA_UF: municipio.uf },
      municipios: municResult.rows.map(r => ({ nm_mun: r.municipio, sigla_uf: r.uf })),
      centroide: municipio.centroid ? (() => { try { const g = JSON.parse(municipio.centroid); return { lat: g.coordinates[1], lng: g.coordinates[0] }; } catch(e) { return null; } })() : null,
      area_total_ha: municipio.area_hectares || 0,
    };

    res.json({
      sucesso: true,
      gerado_em: new Date().toISOString(),
      resultados,
    });
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
    const { bbox, radius = 0.05 } = req.query;

    if (!bbox) {
      return res.status(400).json({ error: 'bbox query parameter required' });
    }

    // Parse bbox: minLng,minLat,maxLng,maxLat
    const [minLng, minLat, maxLng, maxLat] = bbox.split(',').map(Number);

    // Limite dinâmico baseado no zoom: zoom amplo → poucos polígonos; zoom alto → muitos
    const zoom = Math.floor(parseFloat(req.query.zoom)) || 10;
    const bboxDeg = Math.abs(maxLng - minLng);
    //  zoom 7 →  50 polígonos (visão de estado)
    //  zoom 8 → 100
    //  zoom 9 → 200
    // zoom 10 → 400
    // zoom 11 → 600
    // zoom 12+→ 800
    const zoomLimits = { 7: 50, 8: 100, 9: 200, 10: 400, 11: 600 };
    const dynamicLimit = zoomLimits[zoom] || (zoom < 7 ? 30 : 800);
    console.log(`[CAMADA ${camada}] zoom=${zoom} limit=${dynamicLimit} bbox=${bboxDeg.toFixed(2)}°`);
    const bboxGeom = `POLYGON((${minLng} ${minLat}, ${maxLng} ${minLat}, ${maxLng} ${maxLat}, ${minLng} ${maxLat}, ${minLng} ${minLat}))`;

    // Config per camada: table, select expression — columns from Adgain_Mapeamento_Completo.xlsx
    const camadaConfig = {
      sigef:        { table: 'incra.sigef_all',                             select: `parcela_co as fid, nome_area as nome` },
      snci:         { table: 'incra.snci_all',                              select: `cod_imovel as fid, nome_imove as nome` },
      car:          { table: sicarDisponivel ? 'sicar.area_imovel' : 'car.area_imovel_all', select: `cod_imovel as fid, cod_imovel as nome`, extraWhere: `AND des_condic NOT ILIKE '%cancelado%'` },
      bioma:        { table: 'bioma.bioma_250',                             select: `"Bioma" as fid, "Bioma" as nome` },
      tis:          { table: 'terra_indigena.tis_poligonais',               select: `terrai_nom as fid, terrai_nom as nome` },
      ucs:          { table: 'unidade_conservacao.unidade_conserv',         select: `"NOME_UC1" as fid, "NOME_UC1" as nome` },
      embargos:     { table: 'embargos.embargos_ibama',                     select: `nome_embar as fid, nome_imove as nome` },
      anm:          { table: 'anm.anm_mt',                                  select: `"PROCESSO" as fid, "NOME" as nome` },
      serventias:   { table: 'serventias.serventias_brasil',                select: `codigo_cns as fid, cartorio as nome` },
      hidrografia:  { table: 'hidrografia.geoft_bho_2017_curso_dagua',      select: `id as fid, cocursodag as nome` },
    };

    const bboxWkt = `SRID=${SRID};POLYGON((${minLng} ${minLat}, ${maxLng} ${minLat}, ${maxLng} ${maxLat}, ${minLng} ${maxLat}, ${minLng} ${minLat}))`;

    // Centro do viewport — usado para ordenar polígonos do centro para fora
    const centerLng = (minLng + maxLng) / 2;
    const centerLat = (minLat + maxLat) / 2;
    const centerPt  = `ST_SetSRID(ST_MakePoint(${centerLng}, ${centerLat}), ${SRID})`;

    let features = [];

    // Tolerância de simplificação: zoom baixo → mais simplificado; zoom alto → detalhado
    const tolerances = { 7: 0.01, 8: 0.005, 9: 0.002, 10: 0.001, 11: 0.0005 };
    const tolerance = tolerances[zoom] || (zoom < 7 ? 0.02 : 0.0002);

    // SIGEF unifica SIGEF + SNCI (igual ao mapa de cadastro)
    // Usa geom && ST_MakeEnvelope como pré-filtro de bbox (aproveita índice GiST)
    if (camada === 'sigef') {
      const sigefLimit = Math.round(dynamicLimit * 0.6);
      const snciLimit  = Math.round(dynamicLimit * 0.4);
      const envelope = `ST_MakeEnvelope(${minLng}, ${minLat}, ${maxLng}, ${maxLat}, ${SRID})`;
      const [sigefRows, snciRows] = await Promise.all([
        pool.query(`
          SELECT parcela_co as fid, nome_area as nome,
            ST_AsGeoJSON(ST_Simplify(geom, ${tolerance})) as geometry
          FROM incra.sigef_all
          WHERE geom && ${envelope}
            AND ST_Intersects(geom, ${envelope})
          ORDER BY geom <-> ${centerPt}
          LIMIT ${sigefLimit}
        `),
        pool.query(`
          SELECT cod_imovel as fid, nome_imove as nome,
            ST_AsGeoJSON(ST_Simplify(geom, ${tolerance})) as geometry
          FROM incra.snci_all
          WHERE geom && ${envelope}
            AND ST_Intersects(geom, ${envelope})
          ORDER BY geom <-> ${centerPt}
          LIMIT ${snciLimit}
        `)
      ]);
      features = [...sigefRows.rows, ...snciRows.rows]
        .filter(r => r.geometry)
        .map(r => ({ type: 'Feature', id: r.fid, properties: { nome: r.nome }, geometry: JSON.parse(r.geometry) }));
    } else {
      const cfg = camadaConfig[camada];
      if (!cfg) return res.status(400).json({ error: 'Unknown camada: ' + camada });

      const extraWhere = cfg.extraWhere || '';
      const result = await pool.query(`
        SELECT ${cfg.select},
          ST_AsGeoJSON(ST_Simplify(geom, ${tolerance})) as geometry
        FROM ${cfg.table}
        WHERE geom && ST_MakeEnvelope(${minLng}, ${minLat}, ${maxLng}, ${maxLat}, ${SRID})
          AND ST_Intersects(geom, ST_MakeEnvelope(${minLng}, ${minLat}, ${maxLng}, ${maxLat}, ${SRID}))
          ${extraWhere}
        ORDER BY ST_Distance(ST_Centroid(geom), ${centerPt})
        LIMIT ${dynamicLimit}
      `);
      features = result.rows
        .filter(r => r.geometry)
        .map(r => ({ type: 'Feature', id: r.fid, properties: { nome: r.nome }, geometry: JSON.parse(r.geometry) }));
    }

    res.json({ type: 'FeatureCollection', features });
  } catch (error) {
    console.error('Error in /api/camadas/:camada:', error);
    res.status(500).json({ error: error.message });
  }
});

/**
 * POST /api/exportar
 * Export as GeoJSON or KML
 */
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
        SELECT ST_AsKML(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})) as kml
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
    // Aceita tanto { resultados } (enviado pelo frontend) quanto { analyses } (legado)
    const resultados = req.body.resultados || req.body.analyses;
    const municipio  = req.body.municipio || resultados?.municipio?.NM_MUN || 'Desconhecido';
    const geojson    = req.body.geojson;

    if (!resultados) {
      return res.status(400).json({ error: 'resultados required in body' });
    }

    // Import reportService
    const reportService = require('./reportService');

    const pdfBuffer = await reportService.generatePDF({
      analyses: resultados,
      municipio: municipio,
      geojson: geojson,
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
// ANÁLISE IA — Claude Haiku (Anthropic)
// ============================================================================
app.post('/api/analise-ia', async (req, res) => {
  try {
    const { resultados } = req.body;
    if (!resultados) return res.status(400).json({ error: 'resultados obrigatório' });

    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) return res.status(503).json({ error: 'ANTHROPIC_API_KEY não configurada' });

    const Anthropic = require('@anthropic-ai/sdk');
    const client = new Anthropic.default({ apiKey });

    // Monta resumo compacto dos dados para enviar à IA (evita tokens desnecessários)
    const municipio = resultados.municipio?.NM_MUN || 'não identificado';
    const uf        = resultados.municipio?.SIGLA_UF || 'MT';
    const area      = resultados.area_total_ha || 0;

    const sigef     = resultados.fundiaria?.sigef?.length ? `Certificada SIGEF (${resultados.fundiaria.sigef.length} parcela(s))` : 'Sem certificação SIGEF';
    const embargos  = resultados.embargos?.length ? `${resultados.embargos.length} embargo(s)` : 'Sem embargos';
    const ti        = resultados.terras_indigenas?.length ? `Sobreposição com ${resultados.terras_indigenas.length} terra(s) indígena(s)` : 'Sem terras indígenas';
    const uc        = resultados.unidades_conservacao?.length ? `${resultados.unidades_conservacao.length} Unidade(s) de Conservação` : 'Sem UCs';
    const mineracao = resultados.mineracao?.processos_anm?.length ? `${resultados.mineracao.processos_anm.length} processo(s) minerário(s)` : 'Sem processos minerários';
    const hidro     = resultados.hidrografia?.intensidade_hidrica || 'Sem drenagem superficial';
    const cursos    = resultados.hidrografia?.cursos_dagua_count || 0;
    const bacias    = (resultados.hidrografia?.bacias || []).map(b => b.nome_bacia).join(', ') || '—';
    const solos     = (resultados.solo || []).map(s => `${s.legenda} (${s.percentual}%)`).join(', ') || '—';
    const biomas    = (resultados.bioma || []).map(b => `${b.nome || b.Bioma || b.bioma} (${b.percentual}%)`).join(', ') || '—';
    const geologia  = (resultados.geologia || []).map(g => `${g.nome || g.NOME} (${g.percentual}%)`).join(', ') || '—';
    const alt       = resultados.altitude ? `${resultados.altitude.altitude_min}–${resultados.altitude.altitude_max} m (média ${resultados.altitude.altitude_media} m)` : '—';
    const carbono   = resultados.carbono?.estoque_total_toneladas ? `${resultados.carbono.estoque_total_toneladas} t (${resultados.carbono.medio_t_ha || '—'} t/ha)` : '—';
    const aqList    = (resultados.aquiferos || []).map(a => `${a.tipo_aquifero} — ${a.nome}`).join(', ') || 'Não identificado';
    const carApp    = resultados.car?.area_app_ha || 0;
    const carRL     = resultados.car?.area_reserva_legal_ha || resultados.car?.reserva_proposta_ha || 0;
    const carVeg    = resultados.car?.veg_nativa_proposta_ha || resultados.car?.area_vegetacao_nativa_ha || 0;

    const prompt = `Você é um hidrogeólogo sênior com 20 anos de experiência em propriedades rurais do Mato Grosso, especialista em sistemas aquíferos do Brasil Central, perfuração de poços tubulares profundos, irrigação por pivô central e valorização de imóveis rurais.

Com base nos dados técnicos abaixo, redija um laudo técnico completo e profissional em português do Brasil, estruturado nas seguintes seções:

**1. SITUAÇÃO FUNDIÁRIA E CONFORMIDADE AMBIENTAL**
Avalie a regularidade fundiária (SIGEF/SNCI), situação no CAR, APP, reserva legal e quaisquer restrições ambientais, embargos ou sobreposições. Indique o grau de regularidade e o impacto na comercialização ou financiamento da propriedade.

**2. RECURSOS HÍDRICOS SUPERFICIAIS**
Analise as bacias hidrográficas, padrão e densidade de drenagem, disponibilidade hídrica e sazonalidade estimada com base no bioma e geologia. Avalie a viabilidade de captação superficial para irrigação, dessedentação animal e uso agroindustrial. Indique se há potencial para barramentos ou represas.

**3. HIDROGEOLOGIA E POTENCIAL DE AQUÍFEROS**
Esta é a seção mais importante do laudo. Para cada sistema aquífero identificado na propriedade, faça uma análise técnica detalhada incluindo:

- AQUÍFERO POROSO (se presente): Típico em coberturas sedimentares e solos espessos do Cerrado mato-grossense. Descreva a profundidade esperada do nível estático (geralmente 20–80 m), vazão típica (5–30 m³/h), qualidade da água (geralmente boa, com baixa salinidade), custo estimado de perfuração (R$ 150–300/metro), e aptidão para pivô central de baixa a média escala.

- AQUÍFERO FRATURADO (se presente): Associado a rochas cristalinas e metamórficas do embasamento. Explique que a produtividade depende da densidade e abertura de fraturas, com grande variabilidade espacial. Profundidade típica de 60–150 m, vazão de 2–15 m³/h, necessidade de teste de bombeamento para confirmação. Indique risco de poços improdutivos (20–30%) e a importância de estudo geofísico prévio (eletrorresistividade). Avalie custo de R$ 200–400/metro.

- AQUÍFERO CÁRSTICO (se presente): Formado em rochas carbonáticas (calcários/dolomitos), com cavidades e condutos. Alta produtividade potencial (vazões de 20–200 m³/h), mas com distribuição irregular e risco de contaminação. Muito favorável para grandes pivôs. Alerte para necessidade de outorga de uso da água junto à SEMA-MT e ANA.

Para os aquíferos identificados, estime: número de poços recomendados para suportar 1 pivô central de 100 ha (consumo médio de 50–80 m³/h), custo total estimado de implantação de poço completo (perfuração + bomba + casa de bomba + energia), e o impacto na valorização da propriedade (poço com outorga pode valorizar 15–30% o valor da terra).

**4. APTIDÃO AGRÍCOLA E POTENCIAL PRODUTIVO**
Com base em solo, relevo, altitude, bioma e disponibilidade hídrica (superficial + subterrânea combinadas), avalie:
- Culturas recomendadas (soja, milho, algodão, cana, pastagem irrigada, fruticultura)
- Potencial de irrigação por pivô central (quantos pivôs de 100 ha seriam viáveis)
- Estimativa de produtividade comparativa: sequeiro vs. irrigado
- Potencial de dupla safra com irrigação

**5. RISCOS, RESTRIÇÕES E ESTUDOS RECOMENDADOS**
Riscos jurídicos, ambientais e hídricos identificados. Recomende obrigatoriamente:
- Estudo geofísico (se aquífero fraturado presente)
- Teste de bombeamento de longa duração
- Análise físico-química da água
- Processo de outorga na SEMA-MT/ANA
- Outros estudos pertinentes

**6. CONCLUSÃO E VALORIZAÇÃO**
Parecer objetivo classificando: Alta / Moderada / Baixa viabilidade hídrica e agrícola. Destaque o potencial de valorização da propriedade com implantação de infraestrutura hídrica. Conclua com recomendação de investimento.

IMPORTANTE: Seja tecnicamente preciso. Use dados reais de referência do Mato Grosso. Dê estimativas de valores e profundidades quando possível. O laudo será usado por investidores e corretores rurais para tomada de decisão.

═══ DADOS DA PROPRIEDADE ═══
- Município: ${municipio}/${uf}
- Área total: ${area} ha
- Situação fundiária: ${sigef}
- Embargos/Infrações: ${embargos}
- Terras Indígenas: ${ti}
- Unidades de Conservação: ${uc}
- Processos Minerários: ${mineracao}
- Solo: ${solos}
- Bioma: ${biomas}
- Geologia: ${geologia}
- Altitude: ${alt}
- Hidrografia: ${hidro} — ${cursos} curso(s) d'água — Bacias: ${bacias}
- Aquíferos identificados: ${aqList}
- Carbono do solo: ${carbono}
- CAR — APP: ${carApp} ha | Reserva Legal: ${carRL} ha | Vegetação Nativa: ${carVeg} ha`;

    const message = await client.messages.create({
      model: 'claude-haiku-4-5',
      max_tokens: 3500,
      messages: [{ role: 'user', content: prompt }],
    });

    const texto = (message.content[0] && message.content[0].text) ? message.content[0].text : '';
    res.json({ laudo: texto });

  } catch (error) {
    console.error('Erro análise IA:', error.message);
    res.status(500).json({ error: error.message });
  }
});

// ============================================================================
// ERROR HANDLING & SERVER START
// ============================================================================

// Catch-all 404
app.use((req, res) => {
  res.status(404).json({ error: 'Endpoint not found' });
});

// Error middleware
app.use((err, req, res, next) => {
  console.error('Unhandled error:', err);
  res.status(500).json({ error: 'Internal server error' });
});

// Start server
app.listen(port, () => {
  console.log(`AdGain API server running on port ${port}`);
  console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
});
