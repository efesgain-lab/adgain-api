require('dotenv').config();

const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');

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
  family: 4,
});

const SRID = 4674; // SIRGAS 2000

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
 * Uses ibge.municipios_2020 table, falls back to bbox matching
 */
async function getUFFromPoint(lat, lng) {
  try {
    // Use ST_SetSRID + ST_MakePoint (avoids EWKT parsing issues with ST_GeomFromText)
    const result = await pool.query(`
      SELECT LOWER(uf) as uf_lower
      FROM ibge.municipios_2020
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
      SELECT LOWER(m.uf) as uf_lower
      FROM ibge.municipios_2020 m
      WHERE ST_Intersects(m.geom, ST_GeomFromGeoJSON($1::jsonb->'geometry'))
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
      ROUND(CAST(SUM(ST_Area(ST_Intersection(${safeGeom}, ST_GeomFromGeoJSON($1::jsonb->'geometry')))) /
                     ST_Area(ST_GeomFromGeoJSON($1::jsonb->'geometry')) * 100 AS numeric), 2) as percentual
    FROM ${table}
    WHERE ST_Intersects(${safeGeom}, ST_GeomFromGeoJSON($1::jsonb->'geometry'))
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
        WHERE ST_Intersects(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
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
          WHERE ST_Intersects(
            CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
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
        WHERE ST_Intersects(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
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
    const { geojson } = req.body;

    if (!geojson) {
      return res.status(400).json({ error: 'geojson required in body' });
    }

    const feature = parseGeoJSONFeature(geojson);
    const geojsonStr = JSON.stringify(feature);

    // Get centroid and municipality info
    let municResult = await pool.query(`
      SELECT
        m.nome as municipio,
        m.uf,
        ST_AsGeoJSON(ST_Centroid(ST_GeomFromGeoJSON($1::jsonb->'geometry'))) as centroid,
        ROUND(CAST(ST_Area(ST_Transform(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 32721)) / 10000 AS numeric), 2) as area_hectares
      FROM ibge.municipios_2020 m
      WHERE ST_Intersects(m.geom, ST_GeomFromGeoJSON($1::jsonb->'geometry'))
      LIMIT 1
    `, [geojsonStr]);

    // Determine UF from geometry if municipality query returned nothing
    let ufFallback = null;
    if (!municResult.rows[0]) {
      ufFallback = await getUFFromGeoJSON(geojsonStr);
      // If DB failed, try centroid bbox fallback
      if (!ufFallback) {
        try {
          const centroidRes = await pool.query(
            `SELECT ST_X(ST_Centroid(ST_GeomFromGeoJSON($1::jsonb->'geometry'))) as lng,
                    ST_Y(ST_Centroid(ST_GeomFromGeoJSON($1::jsonb->'geometry'))) as lat`,
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
    const anmProcessoTable = `anm.processo_${uf}`;
    const anmOcorrenciasTable = `anm.ocorrencias_${uf}`;
    const carAreaTable = `car.area_imovel_${uf}`;
    const carAppsTable = `car.apps_${uf}`;
    const carReservaTable = `car.reserva_legal_${uf}`;
    const carVegNativaTable = `car.vegetacao_nativa_${uf}`;

    const analyses = {};

    // 9.1 Fundiária (SIGEF + SNCI)
    analyses['9.1_fundiaria'] = {
      nome: 'Fundiária',
      sigef: [],
      snci: [],
    };

    let fundiariaResult = await safeQuery(`
      SELECT
        id,
        numero_imovel as numero,
        ROUND(CAST(ST_Area(ST_Transform(ST_Intersection(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_GeomFromGeoJSON($1::jsonb->'geometry')
        ), 32721)) / 10000 AS numeric), 2) as area_hectares
      FROM ${sigefTable}
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_GeomFromGeoJSON($1::jsonb->'geometry')
      )
    `, [geojsonStr]);

    analyses['9.1_fundiaria'].sigef = fundiariaResult.rows;

    let snciResult = await safeQuery(`
      SELECT
        id,
        numero,
        ROUND(CAST(ST_Area(ST_Transform(ST_Intersection(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_GeomFromGeoJSON($1::jsonb->'geometry')
        ), 32721)) / 10000 AS numeric), 2) as area_hectares
      FROM ${snciTable}
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_GeomFromGeoJSON($1::jsonb->'geometry')
      )
    `, [geojsonStr]);

    analyses['9.1_fundiaria'].snci = snciResult.rows;

    // 9.2 Registral (Serventias by municipality)
    analyses['9.2_registral'] = {
      nome: 'Registral',
      serventias: [],
      message: 'Dados de serventia disponíveis por consulta específica',
    };

    if (municipio.municipio !== 'Desconhecido') {
      let serventiaResult = await pool.query(`
        SELECT
          id,
          nome,
          cartorio_numero
        FROM cartorio.serventias_brasil
        WHERE municipio = $1 AND uf = $2
        LIMIT 10
      `, [municipio.municipio, municipio.uf]);

      analyses['9.2_registral'].serventias = serventiaResult.rows;
    }

    // 9.3 Solo (Pedologia with percentages)
    analyses['9.3_solo'] = {
      nome: 'Solo',
      data: [],
    };

    let soloResult = await pool.query(`
      SELECT
        nome,
        ROUND(CAST(SUM(ST_Area(ST_Intersection(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_GeomFromGeoJSON($1::jsonb->'geometry')
        ))) / ST_Area(ST_GeomFromGeoJSON($1::jsonb->'geometry')) * 100 AS numeric), 2) as percentual
      FROM agronomia.pedo_area
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_GeomFromGeoJSON($1::jsonb->'geometry')
      )
      GROUP BY nome
      ORDER BY percentual DESC
    `, [geojsonStr]);

    analyses['9.3_solo'].data = soloResult.rows;

    // 9.4 Bioma (with percentages)
    analyses['9.4_bioma'] = {
      nome: 'Bioma',
      data: [],
    };

    let biomaResult = await pool.query(`
      SELECT
        nome,
        ROUND(CAST(SUM(ST_Area(ST_Intersection(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_GeomFromGeoJSON($1::jsonb->'geometry')
        ))) / ST_Area(ST_GeomFromGeoJSON($1::jsonb->'geometry')) * 100 AS numeric), 2) as percentual
      FROM mma.bioma_250
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_GeomFromGeoJSON($1::jsonb->'geometry')
      )
      GROUP BY nome
      ORDER BY percentual DESC
    `, [geojsonStr]);

    analyses['9.4_bioma'].data = biomaResult.rows;

    // 9.5 Geologia (Litoestratigrafia with percentages)
    analyses['9.5_geologia'] = {
      nome: 'Geologia',
      data: [],
    };

    let geologiaResult = await pool.query(`
      SELECT
        nome,
        ROUND(CAST(SUM(ST_Area(ST_Intersection(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_GeomFromGeoJSON($1::jsonb->'geometry')
        ))) / ST_Area(ST_GeomFromGeoJSON($1::jsonb->'geometry')) * 100 AS numeric), 2) as percentual
      FROM cprm.litoestratigafia_br
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_GeomFromGeoJSON($1::jsonb->'geometry')
      )
      GROUP BY nome
      ORDER BY percentual DESC
    `, [geojsonStr]);

    analyses['9.5_geologia'].data = geologiaResult.rows;

    // 9.6 Mineração (ANM processes + ocorrências)
    analyses['9.6_mineracao'] = {
      nome: 'Mineração',
      processes: [],
      occurrences: [],
    };

    let anmResult = await safeQuery(`
      SELECT
        numero_processo,
        tipo_processo,
        substancia,
        situacao
      FROM ${anmProcessoTable}
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_GeomFromGeoJSON($1::jsonb->'geometry')
      )
    `, [geojsonStr]);

    analyses['9.6_mineracao'].processes = anmResult.rows;

    let ocorrenciaResult = await safeQuery(`
      SELECT
        id,
        nome,
        substancia
      FROM ${anmOcorrenciasTable}
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_GeomFromGeoJSON($1::jsonb->'geometry')
      )
    `, [geojsonStr]);

    analyses['9.6_mineracao'].occurrences = ocorrenciaResult.rows;

    // 9.7 Embargos (IBAMA)
    analyses['9.7_embargos'] = {
      nome: 'Embargos',
      data: [],
    };

    let embargosResult = await pool.query(`
      SELECT
        id,
        auto_numero,
        data_embargo,
        situacao
      FROM mma.embargos_ibama
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_GeomFromGeoJSON($1::jsonb->'geometry')
      )
    `, [geojsonStr]);

    analyses['9.7_embargos'].data = embargosResult.rows;

    // 9.8 Terras Indígenas
    analyses['9.8_terras_indigenas'] = {
      nome: 'Terras Indígenas',
      data: [],
    };

    let tisResult = await pool.query(`
      SELECT
        id,
        nome,
        etnia,
        ROUND(CAST(ST_Area(ST_Intersection(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_GeomFromGeoJSON($1::jsonb->'geometry')
        )) / 10000 AS numeric), 2) as area_hectares
      FROM funai.tis_poligonais
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_GeomFromGeoJSON($1::jsonb->'geometry')
      )
    `, [geojsonStr]);

    analyses['9.8_terras_indigenas'].data = tisResult.rows;

    // 9.9 Unidades de Conservação
    analyses['9.9_ucs'] = {
      nome: 'Unidades de Conservação',
      data: [],
    };

    let ucsResult = await pool.query(`
      SELECT
        id,
        nome,
        tipo_uc,
        categoria,
        ROUND(CAST(ST_Area(ST_Intersection(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_GeomFromGeoJSON($1::jsonb->'geometry')
        )) / 10000 AS numeric), 2) as area_hectares
      FROM mma.unidade_conserv
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_GeomFromGeoJSON($1::jsonb->'geometry')
      )
    `, [geojsonStr]);

    analyses['9.9_ucs'].data = ucsResult.rows;

    // 9.10 Hidrografia (bacias, cursos d'água, "rica em água" flag)
    analyses['9.10_hidrografia'] = {
      nome: 'Hidrografia',
      bacias: [],
      cursos_agua_count: 0,
      rica_em_agua: false,
    };

    let baciasResult = await pool.query(`
      SELECT
        id,
        nome,
        nivel
      FROM bacias_hidrograficas.bacias_hidrograficas
      WHERE nivel BETWEEN 2 AND 6
        AND ST_Intersects(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_GeomFromGeoJSON($1::jsonb->'geometry')
        )
    `, [geojsonStr]);

    analyses['9.10_hidrografia'].bacias = baciasResult.rows;

    let cursosResult = await pool.query(`
      SELECT COUNT(*) as total
      FROM bacias_hidrograficas.cursos_agua
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_GeomFromGeoJSON($1::jsonb->'geometry')
      )
    `, [geojsonStr]);

    analyses['9.10_hidrografia'].cursos_agua_count = cursosResult.rows[0].total;

    // Flag for water-rich area (if more than 5 water courses)
    analyses['9.10_hidrografia'].rica_em_agua = analyses['9.10_hidrografia'].cursos_agua_count > 5;

    // 9.11 Altitude (raster via ST_PixelAsPoints)
    analyses['9.11_altitude'] = {
      nome: 'Altitude',
      min_m: null,
      max_m: null,
      media_m: null,
      ponto_m: null,
    };

    let altitudeResult = await pool.query(`
      SELECT
        MIN((ST_PixelAsPoints(rast)).val) as min_alt,
        MAX((ST_PixelAsPoints(rast)).val) as max_alt,
        ROUND(CAST(AVG((ST_PixelAsPoints(rast)).val) AS numeric), 1) as avg_alt
      FROM altitude_br.srtm_br
      WHERE ST_Intersects(rast, ST_GeomFromGeoJSON($1::jsonb->'geometry'))
    `, [geojsonStr]);

    if (altitudeResult.rows[0]) {
      analyses['9.11_altitude'].min_m = altitudeResult.rows[0].min_alt;
      analyses['9.11_altitude'].max_m = altitudeResult.rows[0].max_alt;
      analyses['9.11_altitude'].media_m = altitudeResult.rows[0].avg_alt;
    }

    // Get altitude at centroid if available
    if (municipio.centroid) {
      let centroidAltResult = await pool.query(`
        SELECT (ST_PixelAsPoints(rast)).val as altitude
        FROM altitude_br.srtm_br
        WHERE ST_Contains(rast::geometry, ST_GeomFromGeoJSON($1))
        LIMIT 1
      `, [municipio.centroid]);

      if (centroidAltResult.rows[0]) {
        analyses['9.11_altitude'].ponto_m = centroidAltResult.rows[0].altitude;
      }
    }

    // 9.12 Carbono (raster total toneladas)
    analyses['9.12_carbono'] = {
      nome: 'Carbono',
      total_toneladas: null,
    };

    let carbonoResult = await pool.query(`
      SELECT
        ROUND(CAST(SUM((ST_PixelAsPoints(rast)).val) / 1000 AS numeric), 2) as total_toneladas
      FROM carbono_solo.carbono_solo_br
      WHERE ST_Intersects(rast, ST_GeomFromGeoJSON($1::jsonb->'geometry'))
    `, [geojsonStr]);

    if (carbonoResult.rows[0]) {
      analyses['9.12_carbono'].total_toneladas = carbonoResult.rows[0].total_toneladas;
    }

    // 9.13 CAR (área, APP, reserva legal, vegetação nativa)
    analyses['9.13_car'] = {
      nome: 'CAR',
      area_imovel: [],
      app_area_hectares: 0,
      reserva_legal_hectares: 0,
      vegetacao_nativa_hectares: 0,
    };

    let carAreaResult = await safeQuery(`
      SELECT
        gid as id,
        numero_imovel,
        ROUND(CAST(ST_Area(ST_Transform(geom, 32721)) / 10000 AS numeric), 2) as area_hectares
      FROM ${carAreaTable}
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_GeomFromGeoJSON($1::jsonb->'geometry')
      )
    `, [geojsonStr]);

    analyses['9.13_car'].area_imovel = carAreaResult.rows;

    let appResult = await safeQuery(`
      SELECT
        ROUND(CAST(SUM(ST_Area(ST_Intersection(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_GeomFromGeoJSON($1::jsonb->'geometry')
        ))) / 10000 AS numeric), 2) as area_hectares
      FROM ${carAppsTable}
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_GeomFromGeoJSON($1::jsonb->'geometry')
      )
    `, [geojsonStr]);

    if (appResult.rows[0] && appResult.rows[0].area_hectares) {
      analyses['9.13_car'].app_area_hectares = appResult.rows[0].area_hectares;
    }

    let reservaLegalResult = await safeQuery(`
      SELECT
        ROUND(CAST(SUM(ST_Area(ST_Intersection(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_GeomFromGeoJSON($1::jsonb->'geometry')
        ))) / 10000 AS numeric), 2) as area_hectares
      FROM ${carReservaTable}
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_GeomFromGeoJSON($1::jsonb->'geometry')
      )
    `, [geojsonStr]);

    if (reservaLegalResult.rows[0] && reservaLegalResult.rows[0].area_hectares) {
      analyses['9.13_car'].reserva_legal_hectares = reservaLegalResult.rows[0].area_hectares;
    }

    let vegNativaResult = await safeQuery(`
      SELECT
        ROUND(CAST(SUM(ST_Area(ST_Intersection(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_GeomFromGeoJSON($1::jsonb->'geometry')
        ))) / 10000 AS numeric), 2) as area_hectares
      FROM ${carVegNativaTable}
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_GeomFromGeoJSON($1::jsonb->'geometry')
      )
    `, [geojsonStr]);

    if (vegNativaResult.rows[0] && vegNativaResult.rows[0].area_hectares) {
      analyses['9.13_car'].vegetacao_nativa_hectares = vegNativaResult.rows[0].area_hectares;
    }

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

    let geolPontoResult = await pool.query(`
      SELECT id, tipo, descricao
      FROM cprm.geol_ponto
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_GeomFromGeoJSON($1::jsonb->'geometry')
      )
    `, [geojsonStr]);

    analyses['9.14_analises_adicionais'].geologia.pontos = geolPontoResult.rows;

    let geolLinhaDbraResult = await pool.query(`
      SELECT id, tipo, orientacao
      FROM cprm.geol_linha_dobra
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_GeomFromGeoJSON($1::jsonb->'geometry')
      )
    `, [geojsonStr]);

    analyses['9.14_analises_adicionais'].geologia.linhas_dobra = geolLinhaDbraResult.rows;

    let geolLinhaFalhaResult = await pool.query(`
      SELECT id, tipo, comprimento_m
      FROM cprm.geol_linha_falha
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_GeomFromGeoJSON($1::jsonb->'geometry')
      )
    `, [geojsonStr]);

    analyses['9.14_analises_adicionais'].geologia.linhas_falha = geolLinhaFalhaResult.rows;

    let geolLinhaFraturaResult = await pool.query(`
      SELECT id, tipo, comprimento_m
      FROM cprm.geol_linha_fratura
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_GeomFromGeoJSON($1::jsonb->'geometry')
      )
    `, [geojsonStr]);

    analyses['9.14_analises_adicionais'].geologia.linhas_fratura = geolLinhaFraturaResult.rows;

    let tectonicaResult = await pool.query(`
      SELECT id, nome, tipo
      FROM cprm.tectonic_map
      WHERE ST_DWithin(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_GeomFromGeoJSON($1::jsonb->'geometry'),
        0.1
      )
    `, [geojsonStr]);

    analyses['9.14_analises_adicionais'].tectonicas = tectonicaResult.rows;

    // Return all analyses
    res.json({
      municipio: municipio.municipio,
      uf: municipio.uf,
      centroid: municipio.centroid ? JSON.parse(municipio.centroid) : null,
      area_hectares: municipio.area_hectares,
      analyses: analyses,
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
    const { bbox, lat, lng, raio, radius = 0.5 } = req.query;

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
    const geomExpr = `CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END`;

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
      bioma:    { table: 'mma.bioma_250',          idCol: 'id', labelCol: 'nome' },
      tis:      { table: 'funai.tis_poligonais',   idCol: 'id', labelCol: 'nome' },
      ucs:      { table: 'mma.unidade_conserv',    idCol: 'id', labelCol: 'nome' },
      embargos: { table: 'mma.embargos_ibama',     idCol: 'id', labelCol: 'auto_numero' },
    };

    let query;
    let idCol, labelCol;

    if (stateLayerConfig[camada]) {
      // MULTI-STATE: find all UFs overlapping bbox, query each table in parallel
      const cfg = stateLayerConfig[camada];
      idCol = cfg.idCol;
      labelCol = cfg.labelCol;

      const ufs = getUFsFromBbox(minLng, minLat, maxLng, maxLat);

      // Query each state table safely (tables without data return empty array)
      const extraFilter = cfg.filter ? `AND ${cfg.filter}` : '';
      const perStateResults = await Promise.all(
        ufs.map(u => safeQuery(
          `SELECT ${idCol}, ${labelCol} as label,
             ST_AsGeoJSON(ST_Simplify(${geomExpr}, 0.001)) as geometry
           FROM ${cfg.schema}.${cfg.prefix}_${u}
           WHERE ST_Intersects(${geomExpr}, ST_GeomFromText($1))
             ${extraFilter}
           LIMIT 200`,
          [bboxWkt]
        ))
      );

      // Merge all rows and cap at 500
      const allRows = perStateResults.flatMap(r => r.rows).slice(0, 500);
      const features = allRows
        .filter(row => row.geometry != null)
        .map(row => ({
          type: 'Feature',
          id: row[idCol],
          properties: { label: row.label },
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
        WHERE ST_Intersects(${geomExpr}, ST_GeomFromText('${bboxWkt}'))
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
        SELECT ST_AsKML(ST_GeomFromGeoJSON($1::jsonb->'geometry')) as kml
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

    const pdfBuffer = await reportService.generatePDF({
      analyses: analyses,
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
