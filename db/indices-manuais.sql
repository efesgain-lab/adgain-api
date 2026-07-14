-- ═══════════════════════════════════════════════════════════════════
-- Índices criados MANUALMENTE no Supabase (fora de migração automática)
-- Se o banco for recriado/restaurado, reaplicar este arquivo.
-- ═══════════════════════════════════════════════════════════════════

-- 13/07/2026 — Índice espacial no raster de carbono.
-- Sem ele, TODA análise varria os 346.092 tiles (4,6 GB) sequencialmente
-- para achar os ~4 que tocam a parcela: a query de carbono levava 40-104s
-- e era o gargalo dominante do /api/analises. Com o índice: ~1,4s.
CREATE INDEX IF NOT EXISTS idx_carbono_2024_convexhull
  ON carbono_solo.carbono_2024
  USING GIST (ST_ConvexHull(rast));
ANALYZE carbono_solo.carbono_2024;

-- Diagnóstico usado para descobrir (via pg_stat_activity ao vivo):
--   queries 'WITH parcel_geom ...' com waits IO/IPC por 1min+ = parallel
--   seq scan do raster. Confirmação: pg_indexes sem NENHUM índice na tabela.

-- ═══════════════════════════════════════════════════════════════════
-- AUDITORIA DE ÍNDICES (14/07/2026) — rodar no SQL Editor do Supabase.
-- Lista, em TODOS os schemas de dados, as tabelas SEM NENHUM índice,
-- com tamanho e se têm coluna raster/geom (as maiores primeiro = os
-- próximos "carbonos escondidos").
-- ═══════════════════════════════════════════════════════════════════
SELECT
  t.schemaname,
  t.tablename,
  pg_size_pretty(pg_total_relation_size(quote_ident(t.schemaname)||'.'||quote_ident(t.tablename))) AS tamanho,
  pg_total_relation_size(quote_ident(t.schemaname)||'.'||quote_ident(t.tablename)) AS bytes,
  EXISTS (SELECT 1 FROM information_schema.columns c
          WHERE c.table_schema = t.schemaname AND c.table_name = t.tablename
            AND c.udt_name IN ('raster','geometry','geography')) AS tem_geo
FROM pg_tables t
LEFT JOIN pg_indexes i
  ON i.schemaname = t.schemaname AND i.tablename = t.tablename
WHERE t.schemaname NOT IN ('pg_catalog','information_schema','auth','storage','realtime','vault','extensions','graphql','graphql_public','pgsodium','supabase_functions','net','pgbouncer')
GROUP BY 1, 2
HAVING count(i.indexname) = 0
ORDER BY bytes DESC
LIMIT 40;

-- Para cada RASTER sem índice encontrado acima, aplicar (trocar schema.tabela):
--   CREATE INDEX IF NOT EXISTS idx_<tabela>_convexhull
--     ON <schema>.<tabela> USING GIST (ST_ConvexHull(rast));
--   ANALYZE <schema>.<tabela>;
-- Para cada tabela com GEOMETRY sem índice:
--   CREATE INDEX IF NOT EXISTS idx_<tabela>_geom
--     ON <schema>.<tabela> USING GIST (geom);
--   ANALYZE <schema>.<tabela>;

-- PENDENTE:
--   - Tabela car.apps_mt NÃO EXISTE no banco (APPs do CAR vazias para MT) — importar.
