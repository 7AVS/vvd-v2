-- =============================================================================
-- Schema discovery — try each approach until one works
-- =============================================================================

-- Option 1: SHOW TABLE (returns DDL)
SHOW TABLE DG6V01.TACTIC_EVNT_IP_AR_HIST;

-- Option 2: HELP COLUMN (lists columns with types)
HELP COLUMN DG6V01.TACTIC_EVNT_IP_AR_HIST.*;

-- Option 3: DBC.COLUMNS view (system catalog — always works)
SELECT ColumnName, ColumnType, ColumnLength, DecimalTotalDigits, DecimalFractionalDigits, Nullable
FROM DBC.COLUMNS
WHERE DatabaseName = 'DG6V01'
  AND TableName = 'TACTIC_EVNT_IP_AR_HIST'
ORDER BY ColumnId;

-- Option 4: SELECT TOP 1 (see actual data + implicit types)
SELECT TOP 1 * FROM DG6V01.TACTIC_EVNT_IP_AR_HIST;

-- =============================================================================
-- Repeat for each table — use whichever option works above
-- =============================================================================

-- Card table
SELECT ColumnName, ColumnType, ColumnLength, DecimalTotalDigits, DecimalFractionalDigits, Nullable
FROM DBC.COLUMNS
WHERE DatabaseName = 'DDWV01'
  AND TableName = 'VISA_DR_CRD_DIY'
ORDER BY ColumnId;

-- POS table
SELECT ColumnName, ColumnType, ColumnLength, DecimalTotalDigits, DecimalFractionalDigits, Nullable
FROM DBC.COLUMNS
WHERE DatabaseName = 'DDWV05'
  AND TableName = 'CLNT_CRD_POS_LOG'
ORDER BY ColumnId;

-- Token list
SELECT ColumnName, ColumnType, ColumnLength, DecimalTotalDigits, DecimalFractionalDigits, Nullable
FROM DBC.COLUMNS
WHERE DatabaseName = 'DL_DECMAN'
  AND TableName = 'TOKEN_LIST'
ORDER BY ColumnId;

-- IMT tactic table
SELECT ColumnName, ColumnType, ColumnLength, DecimalTotalDigits, DecimalFractionalDigits, Nullable
FROM DBC.COLUMNS
WHERE DatabaseName = 'DTZV01'
  AND TableName = 'TACTIC_EVNT_IP_AR_H60M'
ORDER BY ColumnId;

-- IMT success events
SELECT ColumnName, ColumnType, ColumnLength, DecimalTotalDigits, DecimalFractionalDigits, Nullable
FROM DBC.COLUMNS
WHERE DatabaseName = 'DDWV01'
  AND TableName = 'EXT_CDP_CHNL_EVNT'
ORDER BY ColumnId;
