#!/usr/bin/env node
/*
  Importa CONAB SICARM pro Postgres.
  USO:
    1) Baixe XLSX SICARM em https://www.gov.br/conab/pt-br/assuntos/armazenagem/sicarm
       (ou via SIC: solicitação Lei de Acesso à Informação)
    2) Converta pra CSV UTF-8 separador vírgula
    3) node scripts/import_sicarm.js path/to/sicarm.csv

  Procura colunas: cnpj | razao_social | municipio | uf | tipo |
                   capacidade(_t) | produtos | situacao | latitude | longitude
*/
require('dotenv').config();
const fs = require('fs');
const { Pool } = require('pg');

const csvPath = process.argv[2];
if (!csvPath || !fs.existsSync(csvPath)) {
  console.error('Uso: node scripts/import_sicarm.js <caminho_csv>');
  process.exit(1);
}

const pool = new Pool({
  host: process.env.DB_HOST, port: parseInt(process.env.DB_PORT || '5432'),
  database: process.env.DB_NAME, user: process.env.DB_USER, password: process.env.DB_PASS,
  ssl: process.env.DB_SSL === 'false' ? false : { rejectUnauthorized: false }, family: 4,
});

function parseCSVLine(line) {
  const out = []; let cur = '', inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') { inQ = !inQ; continue; }
    if (c === ',' && !inQ) { out.push(cur); cur = ''; continue; }
    cur += c;
  }
  out.push(cur);
  return out.map(s => s.trim());
}
function norm(s) {
  return (s || '').toLowerCase()
    .replace(/[áàâãä]/g,'a').replace(/[éèêë]/g,'e').replace(/[íìîï]/g,'i')
    .replace(/[óòôõö]/g,'o').replace(/[úùûü]/g,'u').replace(/ç/g,'c')
    .replace(/[^a-z0-9]/g,'_');
}

(async () => {
  const csv = fs.readFileSync(csvPath, 'utf-8');
  const lines = csv.split(/\r?\n/).filter(l => l.trim());
  if (lines.length < 2) { console.error('CSV vazio'); process.exit(1); }
  const header = parseCSVLine(lines[0]).map(norm);
  const findCol = (...alts) => {
    for (const a of alts) {
      const i = header.findIndex(h => h === norm(a) || h.includes(norm(a)));
      if (i >= 0) return i;
    }
    return -1;
  };
  const idx = {
    cnpj: findCol('cnpj'),
    razao: findCol('razao_social', 'nome', 'denominacao'),
    municipio: findCol('municipio', 'cidade'),
    uf: findCol('uf', 'estado'),
    tipo: findCol('tipo'),
    cap: findCol('capacidade_t', 'capacidade_estatica', 'capacidade'),
    produtos: findCol('produtos', 'produto'),
    situacao: findCol('situacao', 'status'),
    lat: findCol('latitude', 'lat'),
    lng: findCol('longitude', 'lng', 'long'),
  };
  console.log('Colunas mapeadas:', idx);
  if (idx.lat < 0 || idx.lng < 0) {
    console.error('ERRO: CSV precisa ter latitude E longitude');
    process.exit(1);
  }
  console.log('Linhas:', lines.length - 1);
  console.log('Criando schema/tabela...');
  await pool.query(`
    CREATE SCHEMA IF NOT EXISTS silos;
    CREATE EXTENSION IF NOT EXISTS postgis;
    DROP TABLE IF EXISTS silos.armazens_brasil;
    CREATE TABLE silos.armazens_brasil (
      id SERIAL PRIMARY KEY,
      cnpj TEXT, razao_social TEXT, municipio TEXT, uf VARCHAR(2),
      tipo TEXT, capacidade_t NUMERIC, produtos TEXT, situacao TEXT,
      lat DOUBLE PRECISION, lng DOUBLE PRECISION,
      geom GEOMETRY(POINT, 4674)
    );
    CREATE INDEX gist_silos_armazens_brasil_geom ON silos.armazens_brasil USING GIST(geom);
    CREATE INDEX idx_silos_armazens_brasil_uf ON silos.armazens_brasil(uf);
  `);
  console.log('Inserindo registros...');
  let ok = 0, skip = 0;
  for (let i = 1; i < lines.length; i++) {
    const row = parseCSVLine(lines[i]);
    const lat = parseFloat((row[idx.lat] || '').replace(',', '.'));
    const lng = parseFloat((row[idx.lng] || '').replace(',', '.'));
    if (isNaN(lat) || isNaN(lng) || Math.abs(lat) > 90 || Math.abs(lng) > 180) { skip++; continue; }
    const cap = parseFloat((row[idx.cap] || '0').replace(/[^0-9.,]/g, '').replace(',', '.')) || null;
    try {
      await pool.query(`
        INSERT INTO silos.armazens_brasil (cnpj, razao_social, municipio, uf, tipo, capacidade_t, produtos, situacao, lat, lng, geom)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, ST_SetSRID(ST_MakePoint($10, $9), 4674))
      `, [
        row[idx.cnpj] || null, row[idx.razao] || null, row[idx.municipio] || null,
        (row[idx.uf] || '').toUpperCase().slice(0, 2) || null,
        row[idx.tipo] || null, cap, row[idx.produtos] || null, row[idx.situacao] || null, lat, lng,
      ]);
      ok++;
    } catch (e) { skip++; }
    if (i % 500 === 0) console.log('  ' + i + '...');
  }
  console.log(`\n✅ Concluído: ${ok} inseridos, ${skip} ignorados`);
  await pool.end();
})();
