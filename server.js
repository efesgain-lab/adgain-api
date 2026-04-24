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
      WHERE ST_Intersects(m.geom, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))
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
      ROUND(CAST(SUM(ST_Area(ST_Intersection(${safeGeom}, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)))) /
                     ST_Area(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)) * 100 AS numeric), 2) as percentual
    FROM ${table}
    WHERE ST_Intersects(${safeGeom}, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))
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

/**
 * Fetches pluviometric data from ANA HidroWebservice for a given centroid.
 * Finds nearest conventional rainfall station within ±1.5° bounding box,
 * fetches 10 years of daily data and aggregates into monthly averages + annual totals.
 * Never throws — returns error object on failure.
 */
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
// CHIRPS — ClimateSERV (NASA SERVIR) — precipitação histórica 30 anos
// ============================================================================
async function fetchPluviometriaCHIRPS(lat, lng) {
  const MONTHS_PT = ['Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez'];
  const endYear   = new Date().getFullYear() - 1;
  const startYear = endYear - 29; // 30 anos

  try {
    const geom = JSON.stringify({ type: 'Point', coordinates: [lng, lat] });
    const qs = new URLSearchParams({
      datatype:          '26',           // CHIRPS Final
      begintime:         '01/01/' + startYear,
      endtime:           '12/31/' + endYear,
      intervaltype:      '0',            // mensal
      operationtype:     '5',            // average
      callback:          '',
      dateType_Category: 'default',
      geometry:          geom,
    });

    const submitUrl = 'https://climateserv.servirglobal.net/api/submitDataRequest/?' + qs;
    const submitResp = await fetch(submitUrl, { signal: AbortSignal.timeout(20000) });
    if (!submitResp.ok) throw new Error('ClimateSERV submit HTTP ' + submitResp.status);
    const submitJson = await submitResp.json();
    const requestId  = Array.isArray(submitJson) ? submitJson[0] : submitJson;
    if (!requestId) throw new Error('ClimateSERV: sem requestId');

    // Polling (máx 60s)
    let dataPoints = null;
    for (let attempt = 0; attempt < 5; attempt++) {
      await new Promise(r => setTimeout(r, 2000));
      const pollResp = await fetch(
        'https://climateserv.servirglobal.net/api/getDataFromRequest/?id=' + requestId,
        { signal: AbortSignal.timeout(15000) }
      );
      const pollJson = await pollResp.json();
      const pts = pollJson?.[0]?.data;
      if (Array.isArray(pts) && pts.length > 0) { dataPoints = pts; break; }
    }
    if (!dataPoints) throw new Error('CHIRPS: timeout aguardando dados');

    // Agregar por mês e por ano
    const monthSums   = new Array(12).fill(0);
    const monthCounts = new Array(12).fill(0);
    const yearTotals  = {};
    for (const pt of dataPoints) {
      const d   = new Date(pt.date);
      const mon = d.getMonth();
      const yr  = d.getFullYear();
      const val = parseFloat(pt.value);
      if (isNaN(val) || val < 0) continue;
      monthSums[mon]   += val;
      monthCounts[mon] += 1;
      yearTotals[yr]    = (yearTotals[yr] || 0) + val;
    }

    const media_mensal = MONTHS_PT.map((mes, i) => ({
      mes,
      media_mm: monthCounts[i] > 0 ? Math.round(monthSums[i] / monthCounts[i]) : 0,
    }));

    const yrArr = Object.values(yearTotals);
    const media_anual_30anos = yrArr.length > 0
      ? Math.round(yrArr.reduce((s, v) => s + v, 0) / yrArr.length)
      : 0;

    return {
      pendente: false,
      erro: null,
      fonte: 'CHIRPS ' + startYear + '-' + endYear + ' (' + yrArr.length + ' anos)',
      media_mensal,
      total_anual: media_anual_30anos,
      media_anual_30anos,
    };
  } catch (e) {
    console.warn('[CHIRPS] erro:', e.message);
    return { pendente: false, erro: 'CHIRPS: ' + e.message, media_mensal: null, total_anual: null, media_anual_30anos: null };
  }
}

// ============================================================================
// SoilGrids v2.0 — ISRIC — propriedades do solo por camada
// ============================================================================
async function fetchSoilGrids(lat, lng) {
  try {
    const props  = ['clay', 'sand', 'silt', 'soc', 'phh2o', 'nitrogen', 'bdod'];
    const depths = ['0-5cm', '5-15cm', '15-30cm', '30-60cm'];

    const qs = new URLSearchParams();
    qs.append('lon', lng.toFixed(6));
    qs.append('lat', lat.toFixed(6));
    props.forEach(p  => qs.append('property', p));
    depths.forEach(d => qs.append('depth', d));
    ['mean'].forEach(v => qs.append('value', v));

    const resp = await fetch('https://rest.isric.org/soilgrids/v2.0/properties/query?' + qs, {
      signal: AbortSignal.timeout(20000),
    });
    if (!resp.ok) throw new Error('SoilGrids HTTP ' + resp.status);
    const data = await resp.json();

    // Fatores de conversão SoilGrids → unidade final
    const factor = { clay: 0.1, sand: 0.1, silt: 0.1, soc: 0.1, phh2o: 0.1, nitrogen: 0.01, bdod: 0.01 };
    const label  = { clay: 'argila_%', sand: 'areia_%', silt: 'silte_%', soc: 'carbono_organico_g_kg', phh2o: 'ph', nitrogen: 'nitrogenio_g_kg', bdod: 'densidade_g_cm3' };

    const camadas = {};
    for (const layer of (data?.properties?.layers || [])) {
      for (const dep of (layer.depths || [])) {
        const d   = dep.label;
        const val = dep.values?.mean;
        if (!camadas[d]) camadas[d] = {};
        camadas[d][label[layer.name]] = val != null ? Math.round(val * factor[layer.name] * 10) / 10 : null;
      }
    }

    return { pendente: false, erro: null, camadas };
  } catch (e) {
    console.warn('[SoilGrids] erro:', e.message);
    return { pendente: false, erro: 'SoilGrids: ' + e.message, camadas: null };
  }
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
    // origem: 'sigef' | 'snci' | 'car' — indica a camada de origem da parcela selecionada
    const { geojson, origem } = req.body;

    if (!geojson) {
      return res.status(400).json({ error: 'geojson required in body' });
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
        ROUND(CAST(ST_Area(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674), 32721)) / 10000 AS numeric), 2) as area_hectares
      FROM municipios.municipios_2024 m
      WHERE ST_Intersects(m.geom, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))
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
    const anmProcessoTable = `anm.anm_${uf}`;
    const anmOcorrenciasTable = `anm.anm_${uf}`;
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
    const samplePtsCTE = `
      WITH parcel_geom AS (
        SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID}) AS g
      ),
      sample_pts AS (
        SELECT DISTINCT unnest(ARRAY[
          ST_PointOnSurface(g),
          CASE WHEN ST_Contains(g, ST_SetSRID(ST_MakePoint(ST_XMin(g)+(ST_XMax(g)-ST_XMin(g))*0.25, ST_YMin(g)+(ST_YMax(g)-ST_YMin(g))*0.25),${SRID}))
               THEN ST_SetSRID(ST_MakePoint(ST_XMin(g)+(ST_XMax(g)-ST_XMin(g))*0.25, ST_YMin(g)+(ST_YMax(g)-ST_YMin(g))*0.25),${SRID})
               ELSE ST_PointOnSurface(g) END,
          CASE WHEN ST_Contains(g, ST_SetSRID(ST_MakePoint(ST_XMin(g)+(ST_XMax(g)-ST_XMin(g))*0.75, ST_YMin(g)+(ST_YMax(g)-ST_YMin(g))*0.25),${SRID}))
               THEN ST_SetSRID(ST_MakePoint(ST_XMin(g)+(ST_XMax(g)-ST_XMin(g))*0.75, ST_YMin(g)+(ST_YMax(g)-ST_YMin(g))*0.25),${SRID})
               ELSE ST_PointOnSurface(g) END,
          CASE WHEN ST_Contains(g, ST_SetSRID(ST_MakePoint(ST_XMin(g)+(ST_XMax(g)-ST_XMin(g))*0.25, ST_YMin(g)+(ST_YMax(g)-ST_YMin(g))*0.75),${SRID}))
               THEN ST_SetSRID(ST_MakePoint(ST_XMin(g)+(ST_XMax(g)-ST_XMin(g))*0.25, ST_YMin(g)+(ST_YMax(g)-ST_YMin(g))*0.75),${SRID})
               ELSE ST_PointOnSurface(g) END,
          CASE WHEN ST_Contains(g, ST_SetSRID(ST_MakePoint(ST_XMin(g)+(ST_XMax(g)-ST_XMin(g))*0.75, ST_YMin(g)+(ST_YMax(g)-ST_YMin(g))*0.75),${SRID}))
               THEN ST_SetSRID(ST_MakePoint(ST_XMin(g)+(ST_XMax(g)-ST_XMin(g))*0.75, ST_YMin(g)+(ST_YMax(g)-ST_YMin(g))*0.75),${SRID})
               ELSE ST_PointOnSurface(g) END
        ]) AS pt
        FROM parcel_geom
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
        WHERE ST_Intersects(
          CASE WHEN ST_SRID(t.geom) = 0 THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )
        AND ST_Area(ST_Intersection(
          CASE WHEN ST_SRID(t.geom) = 0 THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )) / NULLIF(ST_Area(CASE WHEN ST_SRID(t.geom) = 0 THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END), 0) >= 0.3
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
        WHERE ST_Intersects(
          CASE WHEN ST_SRID(t.geom) = 0 THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )
        AND ST_Area(ST_Intersection(
          CASE WHEN ST_SRID(t.geom) = 0 THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )) / NULLIF(ST_Area(CASE WHEN ST_SRID(t.geom) = 0 THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END), 0) >= 0.3
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
        WHERE ST_Intersects(
          CASE WHEN ST_SRID(t.geom) = 0 THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )
          AND ST_Area(ST_Intersection(
            CASE WHEN ST_SRID(t.geom) = 0 THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
            ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
          )) / NULLIF(ST_Area(CASE WHEN ST_SRID(t.geom) = 0 THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END), 0) >= 0.3
        ORDER BY ST_Area(ST_Transform(ST_Intersection(
          CASE WHEN ST_SRID(t.geom) = 0 THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
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
        WHERE ST_Intersects(
          CASE WHEN ST_SRID(t.geom) = 0 THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )
          AND ST_Area(ST_Intersection(
            CASE WHEN ST_SRID(t.geom) = 0 THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
            ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
          )) / NULLIF(ST_Area(CASE WHEN ST_SRID(t.geom) = 0 THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END), 0) >= 0.3
        ORDER BY ST_Area(ST_Transform(ST_Intersection(
          CASE WHEN ST_SRID(t.geom) = 0 THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
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

    // 9.3 Solo (Pedologia with percentages + description fields)
    analyses['9.3_solo'] = { nome: 'Solo', data: [] };
    let soloResult = await safeQuery(`
      SELECT
        nom_unidad as codigo,
        legenda as nome,
        ordem, subordem, textura, relevo, componente,
        ROUND(CAST(SUM(ST_Area(ST_Intersection(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
        ))) / ST_Area(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)) * 100 AS numeric), 2) as percentual
      FROM solo.pedo_area
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
      )
      GROUP BY nom_unidad, legenda, ordem, subordem, textura, relevo, componente
      ORDER BY percentual DESC
    `, [geojsonStr]);
    analyses['9.3_solo'].data = soloResult.rows;

    // 9.4 Bioma (with percentages)
    // Use CTE with ST_Union to avoid double-counting when multiple polygons share the same bioma name
    analyses['9.4_bioma'] = { nome: 'Bioma', data: [] };
    let biomaResult = await safeQuery(`
      WITH bioma_union AS (
        SELECT "Bioma" as nome,
               ST_Union(CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END) as geom
        FROM bioma.bioma_250
        WHERE ST_Intersects(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
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
    `, [geojsonStr]);
    analyses['9.4_bioma'].data = biomaResult.rows;

    // 9.5 Geologia — litoestratigafia_br (polígonos completos do Brasil, CPRM/SGB)
    analyses['9.5_geologia'] = { nome: 'Geologia', data: [] };
    let geologiaResult = await safeQuery(`
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
            CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
            ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
          )) /
          ST_Area(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)) * 100
        AS numeric), 2) as percentual
      FROM geologia_litologia.litoestratigafia_br
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
      )
      ORDER BY percentual DESC
    `, [geojsonStr]);
    analyses['9.5_geologia'].data = geologiaResult.rows;

    // 9.5b — pontos/ocorrências/estruturas são preenchidos pelo bloco 9.14 mais abaixo
    analyses['9.5_geologia'].pontos      = [];
    analyses['9.5_geologia'].ocorrencias = [];
    analyses['9.5_geologia'].dobras      = [];
    analyses['9.5_geologia'].falhas      = [];
    analyses['9.5_geologia'].fraturas    = [];

    // 9.6 Mineração (ANM) — colunas: PROCESSO, FASE, NOME, SUBS
    analyses['9.6_mineracao'] = {
      nome: 'Mineração',
      processes: [],
      occurrences: [],
    };

    let anmResult = await safeQuery(`
      SELECT DISTINCT ON ("PROCESSO")
        "PROCESSO"  as numero_processo,
        "FASE"      as fase,
        "NOME"      as titular,
        "SUBS"      as substancia
      FROM ${anmProcessoTable}
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
      )
      ORDER BY "PROCESSO"
    `, [geojsonStr]);

    analyses['9.6_mineracao'].processes = anmResult.rows;
    analyses['9.6_mineracao'].occurrences = [];

    // 9.7 Embargos (IBAMA)
    analyses['9.7_embargos'] = {
      nome: 'Embargos',
      data: [],
    };

    let embargosResult = await safeQuery(`
      SELECT id, num_auto_i as auto_numero, nome_embar, cpf_cnpj_e, nome_imove, dat_embarg, qtd_area_e, des_infrac, des_tad, sit_desmat as situacao
      FROM embargos.embargos_ibama
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
      )
    `, [geojsonStr]);
    analyses['9.7_embargos'].data = embargosResult.rows;

    // 9.8 Terras Indígenas
    analyses['9.8_terras_indigenas'] = { nome: 'Terras Indígenas', data: [] };
    let tisResult = await safeQuery(`
      SELECT id, terrai_nom as nome, etnia_nome as etnia,
        ROUND(CAST(ST_Area(ST_Intersection(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
        )) / 10000 AS numeric), 2) as area_hectares
      FROM terra_indigena.tis_poligonais
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
      )
    `, [geojsonStr]);
    analyses['9.8_terras_indigenas'].data = tisResult.rows;

    // 9.9 Unidades de Conservação
    analyses['9.9_ucs'] = { nome: 'Unidades de Conservação', data: [] };
    let ucsResult = await safeQuery(`
      SELECT id,
        "NOME_UC1" as nome,
        "CATEGORI3" as categoria,
        "GRUPO4" as grupo,
        "ESFERA5" as esfera,
        "ANO_CRIA6" as ano_criacao,
        "ATO_LEGA9" as ato_legal,
        ROUND(CAST(ST_Area(ST_Intersection(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
        )) / ST_Area(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)) * 100 AS numeric), 2) as sobreposicao_percentual
      FROM unidade_conservacao.unidade_conserv
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
      )
    `, [geojsonStr]);
    analyses['9.9_ucs'].data = ucsResult.rows;

    // 9.10 Hidrografia
    analyses['9.10_hidrografia'] = { nome: 'Hidrografia', bacias: [], cursos_agua_count: 0, rica_em_agua: false, padrao_drenagem: null, nomes_rios: [], comprimento_influencia_km: 0 };

    // Bacias hidrográficas — tenta bacias_nivel_2 a bacias_nivel_6 no schema bacias_hidrograficas
    const baciasRows = [];
    for (const nivel of [2, 3, 4, 5, 6]) {
      try {
        const r = await pool.query(`
          SELECT nome_bacia, curso_prin, princ_aflu, sub_bacias, suprabacia, ${nivel} as nivel
          FROM bacias_hidrograficas.bacias_nivel_${nivel}
          WHERE ST_Intersects(
            CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
            ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
          )
          LIMIT 1
        `, [geojsonStr]);
        console.log(`[bacia] nivel=${nivel} rows=${r.rows.length}`);
        if (r.rows.length > 0) baciasRows.push(...r.rows);
      } catch (e) {
        console.error(`[bacia] nivel=${nivel} ERROR: ${e.message}`);
      }
    }
    analyses['9.10_hidrografia'].bacias = baciasRows;

    let cursosResult = await safeQuery(`
      SELECT COUNT(*) as total
      FROM hidrografia.geoft_bho_2017_curso_dagua
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
      )
    `, [geojsonStr]);
    analyses['9.10_hidrografia'].cursos_agua_count = parseInt(cursosResult.rows[0]?.total || 0);
    analyses['9.10_hidrografia'].rica_em_agua = analyses['9.10_hidrografia'].cursos_agua_count > 5;

    // Padrao de drenagem (hidrografia.geoft_bho_2017_curso_dagua) — analise em raio de 5km
    const RAIO_DRENAGEM_KM = 25;
    const padraoAgg = await safeQuery(`
      WITH parcel AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AS g),
      buffered AS (SELECT ST_Buffer(ST_Centroid(g)::geography, ${RAIO_DRENAGEM_KM}*1000)::geometry AS bg FROM parcel),
      courses AS (
        SELECT MOD(degrees(ST_Azimuth(ST_StartPoint(c.geom), ST_EndPoint(c.geom)))::numeric, 180::numeric) AS az
        FROM hidrografia.geoft_bho_2017_curso_dagua c, buffered b
        WHERE ST_Intersects(c.geom, b.bg)
      )
      SELECT COUNT(*)::int AS total, COALESCE(ROUND(STDDEV(az)::numeric, 1), 0) AS std_az FROM courses
    `, [geojsonStr]);
    const compParcelRes = await safeQuery(`
      SELECT COALESCE(SUM(ST_Length(ST_Intersection(c.geom, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))::geography)), 0) AS len_m
      FROM hidrografia.geoft_bho_2017_curso_dagua c
      WHERE ST_Intersects(c.geom, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))
    `, [geojsonStr]);
    // Nomes de rios: hidrografia.rio_nomes (NORIOCOMP) — ST_DWithin 100m
    const nomesRes = await safeQuery(`
      SELECT DISTINCT "NORIOCOMP"::text AS nome
      FROM hidrografia.rio_nomes
      WHERE ST_DWithin(
              geom::geography,
              ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)::geography,
              100
            )
        AND "NORIOCOMP" IS NOT NULL AND TRIM("NORIOCOMP"::text) <> ''
      LIMIT 50
    `, [geojsonStr]);
    const ordensRes = await safeQuery(`
      SELECT nuordemcda AS ordem, COUNT(*)::int AS cnt
      FROM hidrografia.geoft_bho_2017_curso_dagua
      WHERE ST_Intersects(geom, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))
      GROUP BY nuordemcda ORDER BY nuordemcda DESC
    `, [geojsonStr]);
    const totalRaio = parseInt(padraoAgg.rows[0]?.total || 0);
    const stdAz = parseFloat(padraoAgg.rows[0]?.std_az || 0);
    const lenKm = parseFloat(((compParcelRes.rows[0]?.len_m || 0) / 1000).toFixed(2));
    let padrao = 'Indefinido', descricao = 'Sem dados suficientes para definir o padrao';
    if (totalRaio >= 3) {
      if (stdAz < 25) { padrao = 'Paralelo'; descricao = "Cursos d'agua fluem em direcoes semelhantes, sugerindo controle estrutural ou relevo inclinado uniforme."; }
      else if (stdAz < 50) { padrao = 'Subparalelo'; descricao = 'Padrao intermediario - orientacao parcialmente consistente.'; }
      else { padrao = 'Dendritico'; descricao = 'Cursos ramificam-se como galhos de arvore - tipico em litologia homogenea e relevo suave.'; }
    }
    const ordens = ordensRes.rows || [];
    const ordemMax = ordens.reduce((m, r) => Math.max(m, parseInt(r.ordem || 0)), 0);
    const ordem1 = parseInt(ordens.find(r => parseInt(r.ordem) === 1)?.cnt || 0);
    const ordem2 = parseInt(ordens.find(r => parseInt(r.ordem) === 2)?.cnt || 0);
    const ordem3plus = ordens.filter(r => parseInt(r.ordem) >= 3).reduce((s, r) => s + parseInt(r.cnt || 0), 0);
    analyses['9.10_hidrografia'].padrao_drenagem = {
      padrao, descricao,
      raio_analise_km: RAIO_DRENAGEM_KM,
      total_raio: totalRaio,
      desvio_azimuth: stdAz,
      comprimento_km: lenKm,
      ordem_maxima: ordemMax,
      ordem1, ordem2, ordem3plus
    };
    analyses['9.10_hidrografia'].comprimento_influencia_km = lenKm;
    analyses['9.10_hidrografia'].nomes_rios = (nomesRes.rows || []).map(r => r.nome).filter(Boolean);

    // 9.11 Altitude
    analyses['9.11_altitude'] = { nome: 'Altitude', min_m: null, max_m: null, media_m: null, ponto_m: null };
    let altitudeResult = await safeQuery(`
      SELECT
        MIN(v.val) as min_alt,
        MAX(v.val) as max_alt,
        ROUND(CAST(AVG(v.val) AS numeric), 1) as avg_alt
      FROM altitude_br.altitude_raster r,
           LATERAL ST_PixelAsPoints(ST_Clip(r.rast, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))) v
      WHERE ST_Intersects(r.rast, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))
        AND v.val IS NOT NULL
    `, [geojsonStr]);
    if (altitudeResult.rows[0]) {
      analyses['9.11_altitude'].min_m = altitudeResult.rows[0].min_alt;
      analyses['9.11_altitude'].max_m = altitudeResult.rows[0].max_alt;
      analyses['9.11_altitude'].media_m = altitudeResult.rows[0].avg_alt;
    }
    if (municipio.centroid) {
      let centroidAltResult = await safeQuery(`
        SELECT ST_Value(r.rast, ST_SetSRID(ST_GeomFromGeoJSON($1), 4674)) as altitude
        FROM altitude_br.altitude_raster r
        WHERE ST_Intersects(r.rast, ST_SetSRID(ST_GeomFromGeoJSON($1), 4674))
        LIMIT 1
      `, [municipio.centroid]);
      if (centroidAltResult.rows[0]) {
        analyses['9.11_altitude'].ponto_m = centroidAltResult.rows[0].altitude;
      }
    }

    // Fallback: se altitude (min/max/media) nao retornou dados, usa ponto_m
    if (analyses['9.11_altitude'].min_m === null && analyses['9.11_altitude'].ponto_m !== null) {
      analyses['9.11_altitude'].min_m = analyses['9.11_altitude'].ponto_m;
      analyses['9.11_altitude'].max_m = analyses['9.11_altitude'].ponto_m;
      analyses['9.11_altitude'].media_m = analyses['9.11_altitude'].ponto_m;
    }

    // 9.12 Carbono do Solo
    // Raster armazena Mg C/ha (toneladas/ha por pixel).
    // Total (Mg C) = SUM(val × pixel_area_ha)
    // pixel_area_ha = |ScaleX × ScaleY| em graus² × cos(lat) × 111320² / 10000
    analyses['9.12_carbono'] = { nome: 'Carbono', total_toneladas: null };
    let carbonoResult = await safeQuery(`
      WITH parcel_geom AS (
        SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674) AS g
      ),
      clipped AS (
        SELECT
          ST_Clip(r.rast, pg.g, true) AS rast_clip,
          ABS(ST_ScaleX(r.rast)) * ABS(ST_ScaleY(r.rast))
            * COS(RADIANS(ST_Y(ST_Centroid(ST_Envelope(r.rast)))))
            * 111320.0 * 111320.0 / 10000.0 AS pixel_area_ha
        FROM carbono_solo.carbono_2024 r, parcel_geom pg
        WHERE ST_Intersects(r.rast, pg.g)
      )
      SELECT ROUND(CAST(SUM(v.val * c.pixel_area_ha) AS numeric), 2) AS total_toneladas
      FROM clipped c,
           LATERAL ST_PixelAsPoints(c.rast_clip) v
      WHERE v.val IS NOT NULL AND v.val > 0
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
        WHERE ST_Intersects(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
        )
        ORDER BY ST_Area(ST_Transform(ST_Intersection(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
        ), 32721)) DESC
        LIMIT 1
      `, [geojsonStr]);
    } else {
      // Parcela SIGEF/SNCI selecionada → 4 pontos + filtro de sobreposição real
      // Vizinhos que só confinam têm interseção linear (área = 0) e são excluídos
      carAreaResult = await safeQuery(`
        ${samplePtsCTE}
        SELECT DISTINCT ON (t.gid) t.gid as id,
          t.cod_imovel, t.num_area, t.ind_tipo, t.ind_status, t.des_condic, t.dat_criaca, t.dat_atuali,
          ROUND(CAST(ST_Area(ST_Transform(t.geom, 32721)) / 10000 AS numeric), 2) as area_hectares
        FROM ${carAreaTable} t, sample_pts sp
        WHERE ST_Contains(
          CASE WHEN ST_SRID(t.geom) = 0 THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
          sp.pt
        )
        AND ST_Area(ST_Intersection(
          CASE WHEN ST_SRID(t.geom) = 0 THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
        )) / NULLIF(ST_Area(CASE WHEN ST_SRID(t.geom) = 0 THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END), 0) >= 0.3
          AND ST_Area(ST_Intersection(
            CASE WHEN ST_SRID(t.geom) = 0 THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END,
            ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), ${SRID})
          )) / NULLIF(ST_Area(CASE WHEN ST_SRID(t.geom) = 0 THEN ST_SetSRID(t.geom, ${SRID}) ELSE t.geom END), 0) >= 0.3
      `, [geojsonStr]);
    }

    analyses['9.13_car'].area_imovel = carAreaResult.rows;

    let appResult = await safeQuery(`
      SELECT
        ROUND(CAST(SUM(ST_Area(ST_Intersection(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
        ))) / 10000 AS numeric), 2) as area_hectares
      FROM ${carAppsTable}
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
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

      // Reserva Legal: DISTINCT ON (gid) para evitar duplicatas na tabela CAR
      let reservaLegalResult = await safeQuery(`
        SELECT ROUND(CAST(SUM(num_area) AS numeric), 2) as area_ha
        FROM (
          SELECT DISTINCT ON (gid) CAST(num_area AS numeric) as num_area
          FROM ${carReservaTable}
          WHERE cod_imovel IN (${placeholders})
        ) d
      `, codImoveisCar);

      if (reservaLegalResult.rows[0] && reservaLegalResult.rows[0].area_ha) {
        analyses['9.13_car'].reserva_legal_hectares = parseFloat(reservaLegalResult.rows[0].area_ha);
      }

      // Vegetação Nativa: DISTINCT ON (gid) para evitar duplicatas
      let vegNativaResult = await safeQuery(`
        SELECT ROUND(CAST(SUM(num_area) AS numeric), 2) as area_ha
        FROM (
          SELECT DISTINCT ON (gid) CAST(num_area AS numeric) as num_area
          FROM ${carVegNativaTable}
          WHERE cod_imovel IN (${placeholders})
        ) d
      `, codImoveisCar);

      if (vegNativaResult.rows[0] && vegNativaResult.rows[0].area_ha) {
        analyses['9.13_car'].vegetacao_nativa_hectares = parseFloat(vegNativaResult.rows[0].area_ha);
      }

      // Área Consolidada: DISTINCT ON (gid) para evitar duplicatas
      let areaConsolidadaResult;
      try {
        areaConsolidadaResult = await pool.query(`
          SELECT ROUND(CAST(SUM(num_area) AS numeric), 2) as area_ha
          FROM (
            SELECT DISTINCT ON (gid) CAST(num_area AS numeric) as num_area
            FROM ${carAreaConsolidadaTable}
            WHERE cod_imovel IN (${placeholders})
          ) d
        `, codImoveisCar);
      } catch (e) {
        console.error('[DEBUG] areaConsolidada ERRO:', e.message, '| tabela:', carAreaConsolidadaTable);
        areaConsolidadaResult = { rows: [] };
      }

      if (areaConsolidadaResult.rows[0] && areaConsolidadaResult.rows[0].area_ha) {
        analyses['9.13_car'].area_consolidada_hectares = parseFloat(areaConsolidadaResult.rows[0].area_ha);
      }
    }

    // 9.13b Aquíferos
    analyses['9.13b_aquiferos'] = { nome: 'Aquíferos', data: [] };
    // Try hidrogeologia schema first, then sgb, then hidrogeo
    let aquiferosResult = await safeQuery(`
      SELECT tipo_aquife as tipo, sistema_aqu as sistema, dominio as dominio,
             descricao, area_km2 as area_sistema_km2
      FROM hidrogeologia.sistemas_aquiferos
      WHERE ST_Intersects(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
        ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
      )
      LIMIT 5
    `, [geojsonStr]);
    if (aquiferosResult.rows.length === 0) {
      aquiferosResult = await safeQuery(`
        SELECT tipo, sistema, dominio, descricao,
               ROUND(CAST(ST_Area(ST_Transform(geom, 32721)) / 1000000 AS numeric), 3) as area_sistema_km2
        FROM hidrogeologia.aquiferos
        WHERE ST_Intersects(
          CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END,
          ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674)
        )
        LIMIT 5
      `, [geojsonStr]);
    }
    analyses['9.13b_aquiferos'].data = aquiferosResult.rows;

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

    // Afloramentos geológicos (geol_ponto) — SRID 4674
    let geolPontoResult = await safeQuery(`
      SELECT cd_fcim, ds_afl1, nm_unidade as nome, tipo_pto as tipo, fonte
      FROM geologia_litologia.geol_ponto
      WHERE ST_Intersects(CASE WHEN ST_SRID(geom)=0 THEN ST_SetSRID(geom,${SRID}) ELSE geom END, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))
    `, [geojsonStr]);
    analyses['9.14_analises_adicionais'].geologia.pontos = geolPontoResult.rows;
    analyses['9.5_geologia'].pontos = geolPontoResult.rows.map(r => ({ cod_fcim: r.cd_fcim, ds_afl1: r.ds_afl1 }));

    // Ocorrências minerais — tabela com acento "ocorrências_br", SRID 4326
    let ocorrenciasResult = await safeQuery(`
      SELECT "SUBSTANCIAS" as substancias, "ROCHAS_HOSPEDEIRAS" as rochas_hospedeiras
      FROM geologia_litologia."ocorrências_br"
      WHERE ST_Intersects(geom, ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674), 4326))
    `, [geojsonStr]);
    analyses['9.14_analises_adicionais'].geologia.ocorrencias = ocorrenciasResult.rows;
    analyses['9.5_geologia'].ocorrencias = ocorrenciasResult.rows;

    // Dobras — SRID 4674
    let geolLinhaDbraResult = await safeQuery(`
      SELECT cd_fcim, classif, caract_eix, caime_eix
      FROM geologia_litologia.geol_linha_dobra
      WHERE ST_Intersects(CASE WHEN ST_SRID(geom)=0 THEN ST_SetSRID(geom,${SRID}) ELSE geom END, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))
    `, [geojsonStr]);
    analyses['9.14_analises_adicionais'].geologia.linhas_dobra = geolLinhaDbraResult.rows;
    analyses['9.5_geologia'].dobras = geolLinhaDbraResult.rows;

    // Falhas — SRID 4674
    let geolLinhaFalhaResult = await safeQuery(`
      SELECT cd_fcim, classif, forma, estm_merg, sentido
      FROM geologia_litologia.geol_linha_falha
      WHERE ST_Intersects(CASE WHEN ST_SRID(geom)=0 THEN ST_SetSRID(geom,${SRID}) ELSE geom END, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))
    `, [geojsonStr]);
    analyses['9.14_analises_adicionais'].geologia.linhas_falha = geolLinhaFalhaResult.rows;
    analyses['9.5_geologia'].falhas = geolLinhaFalhaResult.rows;

    // Fraturas — SRID 4674
    let geolLinhaFraturaResult = await safeQuery(`
      SELECT cd_fcim, classif, forma, mergulho
      FROM geologia_litologia.geol_linha_fratura
      WHERE ST_Intersects(CASE WHEN ST_SRID(geom)=0 THEN ST_SetSRID(geom,${SRID}) ELSE geom END, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))
    `, [geojsonStr]);
    analyses['9.14_analises_adicionais'].geologia.linhas_fratura = geolLinhaFraturaResult.rows;
    analyses['9.5_geologia'].fraturas = geolLinhaFraturaResult.rows;

    // Tectônicas — todas as tabelas do schema tectonic_map
    const tectonicGeoFilter = `ST_DWithin(CASE WHEN ST_SRID(geom)=0 THEN ST_SetSRID(geom,${SRID}) ELSE geom END, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674), 1.0)`;
    const tectonicGeoIntersect = `ST_Intersects(CASE WHEN ST_SRID(geom)=0 THEN ST_SetSRID(geom,${SRID}) ELSE geom END, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'), 4674))`;

    const plateBoundaryResult = await safeQuery(`
      SELECT id, feature, type as tipo FROM tectonic_map.plate_boundary WHERE ${tectonicGeoFilter}
    `, [geojsonStr]);
    analyses['9.14_analises_adicionais'].tectonicas.plate_boundary = plateBoundaryResult.rows;

    const continentStructureResult = await safeQuery(`
      SELECT id, type as tipo, name FROM tectonic_map.continent_structure WHERE ${tectonicGeoFilter}
    `, [geojsonStr]);
    analyses['9.14_analises_adicionais'].tectonicas.continent_structure = continentStructureResult.rows;

    const cratonResult = await safeQuery(`
      SELECT id, type as tipo FROM tectonic_map.craton_terranes_limits WHERE ${tectonicGeoFilter}
    `, [geojsonStr]);
    analyses['9.14_analises_adicionais'].tectonicas.craton_terranes_limits = cratonResult.rows;

    const dykesResult = await safeQuery(`
      SELECT id, type as tipo, age FROM tectonic_map.dykes WHERE ${tectonicGeoIntersect}
    `, [geojsonStr]);
    analyses['9.14_analises_adicionais'].tectonicas.dykes = dykesResult.rows;

    const eclogitesResult = await safeQuery(`
      SELECT id, type as tipo, long_dec as longitude, lat_dec as latitude FROM tectonic_map.eclogites
      WHERE ${tectonicGeoFilter}
    `, [geojsonStr]);
    analyses['9.14_analises_adicionais'].tectonicas.eclogites = eclogitesResult.rows;

    const isopachsResult = await safeQuery(`
      SELECT id, type as tipo, depth_base FROM tectonic_map.isopachs WHERE ${tectonicGeoIntersect}
    `, [geojsonStr]);
    analyses['9.14_analises_adicionais'].tectonicas.isopachs = isopachsResult.rows;

    const kimberlitesResult = await safeQuery(`
      SELECT id, source, age, long_dec as longitude, lat_dec as latitude FROM tectonic_map.kimberlites
      WHERE ${tectonicGeoFilter}
    `, [geojsonStr]);
    analyses['9.14_analises_adicionais'].tectonicas.kimberlites = kimberlitesResult.rows;

    const paleozResult = await safeQuery(`
      SELECT id, type as tipo FROM tectonic_map.paleoz_erosional_border WHERE ${tectonicGeoIntersect}
    `, [geojsonStr]);
    analyses['9.14_analises_adicionais'].tectonicas.paleoz_erosional_border = paleozResult.rows;

    const sutureResult = await safeQuery(`
      SELECT id, type as tipo, obs FROM tectonic_map.suture_zones WHERE ${tectonicGeoIntersect}
    `, [geojsonStr]);
    analyses['9.14_analises_adicionais'].tectonicas.suture_zones = sutureResult.rows;

    // 9.15 Pluviometria (CHIRPS 30 anos) + 9.16 Solo (SoilGrids)
    let pluviometria = { pendente: false, erro: 'Coordenadas não disponíveis', media_mensal: null };
    let solo = { pendente: false, erro: 'Coordenadas não disponíveis', camadas: null };
    try {
      const centroidForPluvio = municipio.centroid ? JSON.parse(municipio.centroid) : null;
      if (centroidForPluvio) {
        const pLat = centroidForPluvio.coordinates[1];
        const pLng = centroidForPluvio.coordinates[0];

        // Paralelo: CHIRPS e SoilGrids
        const [chirpsResult, soilResult] = await Promise.all([
          fetchPluviometriaCHIRPS(pLat, pLng),
          fetchSoilGrids(pLat, pLng),
        ]);

        pluviometria = {
          pendente: false,
          erro: chirpsResult.erro || null,
          fonte: chirpsResult.fonte || null,
          media_mensal: chirpsResult.media_mensal || null,
          total_anual: chirpsResult.total_anual || null,
          media_anual_30anos: chirpsResult.media_anual_30anos || null,
        };

        solo = soilResult;
      }
    } catch (e) {
      console.warn('[Pluviometria/Solo] erro:', e.message);
      pluviometria = { pendente: false, erro: 'Erro interno: ' + e.message, media_mensal: null };
    }
      console.warn('[Pluviometria/Solo] erro:', e.message);
      pluviometria = { pendente: false, erro: 'Erro interno: ' + e.message, resumo: null, media_mensal: null };
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
      },
      altitude: {
        altitude_min:    analyses['9.11_altitude'].min_m,
        altitude_max:    analyses['9.11_altitude'].max_m,
        altitude_media:  analyses['9.11_altitude'].media_m,
        altitude_ponto:  analyses['9.11_altitude'].ponto_m,
      },
      carbono: {
        estoque_total_toneladas: analyses['9.12_carbono'].total_toneladas,
        min_t_ha: null, max_t_ha: null, medio_t_ha: null,
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
      aquiferos: analyses['9.13b_aquiferos'].data,
      analises_adicionais: analyses['9.14_analises_adicionais'],
      pluviometria,
      solo,
      municipio:  { NM_MUN: municipio.municipio, SIGLA_UF: municipio.uf, CD_MUN: '' },
      municipios: [{ nm_mun: municipio.municipio, sigla_uf: municipio.uf }],
      centroide:  centroidParsed ? { lat: centroidParsed.coordinates[1], lng: centroidParsed.coordinates[0] } : { lat: 0, lng: 0 },
      area_total_ha: parseFloat(municipio.area_hectares) || 0,
    };

    res.json({ sucesso: true, gerado_em: new Date().toISOString(), resultados });
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
    const geomExpr = `CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, ${SRID}) ELSE geom END`;

    // Zoom-based tuning — keeps original ST_Intersects/bboxWkt intact (safe for SRID=0 tables)
    const simplifyTol   = zoom >= 14 ? 0.00005 : zoom >= 12 ? 0.0003 : zoom >= 10 ? 0.001 : zoom >= 8 ? 0.004 : 0.01;
    const limitPerState = zoom >= 12 ? 150 : zoom >= 10 ? 80 : zoom >= 8 ? 40 : 20;
    const limitTotal    = zoom >= 12 ? 400 : zoom >= 10 ? 200 : zoom >= 8 ? 100 : 60;

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
      tis:      { table: 'terra_indigena.tis_poligonais',      idCol: 'id', labelCol: 'terrai_nom' },
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
             WHERE ST_Intersects(${geomExpr}, ST_GeomFromText($1))
             ORDER BY dist_center ASC LIMIT ${limitPerState}`,
            [bboxWkt, centerLng, centerLat]
          ),
          safeQuery(
            `SELECT ${snciCfg.idCol}, ${snciCfg.labelCol} as label, 'snci' as source,
               ST_AsGeoJSON(ST_Simplify(${geomExpr}, ${simplifyTol})) as geometry,
               ST_Distance(ST_Centroid(${geomExpr}), ST_SetSRID(ST_MakePoint($2, $3), ${SRID})) as dist_center
             FROM ${snciCfg.schema}.${snciCfg.prefix}_${u}
             WHERE ST_Intersects(${geomExpr}, ST_GeomFromText($1))
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
           WHERE ST_Intersects(${geomExpr}, ST_GeomFromText($1))
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
                  pontos:   `SELECT COUNT(*) n FROM geologia_litologia.geol_ponto WHERE ST_Intersects(CASE WHEN ST_SRID(geom)=0 THEN ST_SetSRID(geom,4674) ELSE geom END, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'),4674))`,
                  falhas:   `SELECT COUNT(*) n FROM geologia_litologia.geol_linha_falha WHERE ST_Intersects(CASE WHEN ST_SRID(geom)=0 THEN ST_SetSRID(geom,4674) ELSE geom END, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'),4674))`,
                  dobras:   `SELECT COUNT(*) n FROM geologia_litologia.geol_linha_dobra WHERE ST_Intersects(CASE WHEN ST_SRID(geom)=0 THEN ST_SetSRID(geom,4674) ELSE geom END, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'),4674))`,
                  fraturas: `SELECT COUNT(*) n FROM geologia_litologia.geol_linha_fratura WHERE ST_Intersects(CASE WHEN ST_SRID(geom)=0 THEN ST_SetSRID(geom,4674) ELSE geom END, ST_SetSRID(ST_GeomFromGeoJSON($1::jsonb->'geometry'),4674))`,
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

// Start server
app.listen(port, () => {
  console.log(`AdGain API server running on port ${port}`);
  console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
});
