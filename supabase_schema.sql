-- XAUUSD Signal Bot v8.1.0 — Supabase schema
CREATE TABLE IF NOT EXISTS signals (
    signal_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ts              TIMESTAMPTZ NOT NULL,
    action          TEXT NOT NULL CHECK (action IN ('BUY','SELL','WAIT')),
    spot            NUMERIC(12,2),
    entry_price     NUMERIC(12,2),
    sl              NUMERIC(12,2),
    tp1             NUMERIC(12,2),
    tp2             NUMERIC(12,2),
    score           NUMERIC(5,1),
    regime          TEXT,
    kelly_fraction  NUMERIC(6,4),
    lot_size        NUMERIC(8,2),
    risk_usd        NUMERIC(10,2),
    max_hold_hours  NUMERIC(6,1) DEFAULT 48,
    notes           TEXT,
    status          TEXT NOT NULL DEFAULT 'OPEN'
                    CHECK (status IN ('OPEN','WIN_TP1','WIN_TP2','LOSS','TIMEOUT')),
    r_multiple      NUMERIC(8,4),
    closed_at       TIMESTAMPTZ,
    closed_price    NUMERIC(12,2),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_signals_ts     ON signals(ts DESC);
CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(status);
CREATE INDEX IF NOT EXISTS idx_signals_action ON signals(action);
-- Optional: RLS (jika key anon dipakai)
ALTER TABLE signals ENABLE ROW LEVEL SECURITY;
-- Service role bypasses RLS otomatis; ini hanya untuk keamanan anon key:
DROP POLICY IF EXISTS signals_insert ON signals;
CREATE POLICY signals_insert ON signals FOR INSERT WITH CHECK (true);
CREATE POLICY signals_update ON signals FOR UPDATE USING (true);
CREATE POLICY signals_select ON signals FOR SELECT USING (true);
