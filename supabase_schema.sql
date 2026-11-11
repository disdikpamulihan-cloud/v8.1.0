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
                    CHECK (status IN ('OPEN','WIN_TP1','WIN_TP2','LOSS','TIMEOUT','SKIPPED')),
    r_multiple      NUMERIC(8,4),
    closed_at       TIMESTAMPTZ,
    closed_price    NUMERIC(12,2),
    confirmed       INTEGER NOT NULL DEFAULT 0,  -- 0=unknown, 1=taken, 2=skipped
    confirm_ts      TIMESTAMPTZ,
    trading_mode    TEXT NOT NULL DEFAULT 'paper',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_signals_ts          ON signals(ts DESC);
CREATE INDEX IF NOT EXISTS idx_signals_status      ON signals(status);
CREATE INDEX IF NOT EXISTS idx_signals_confirmed   ON signals(confirmed);
CREATE INDEX IF NOT EXISTS idx_signals_action      ON signals(action);

ALTER TABLE signals ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS signals_all ON signals;
CREATE POLICY signals_all ON signals FOR ALL USING (true) WITH CHECK (true);
