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

-- PENDENTE (verificar em próxima sessão):
--   - Conferir se outros rasters têm índice: altitude_br.altitude_raster,
--     e demais tabelas de carbono_solo/altitude_br. Query de auditoria:
--     SELECT t.schemaname, t.tablename
--     FROM pg_tables t
--     LEFT JOIN pg_indexes i ON i.schemaname=t.schemaname AND i.tablename=t.tablename
--     WHERE t.schemaname IN ('carbono_solo','altitude_br','hidrografia','bioma','solo')
--     GROUP BY 1,2 HAVING count(i.indexname) = 0;
--   - Tabela car.apps_mt NÃO EXISTE no banco (APPs do CAR vazias para MT) — importar.
