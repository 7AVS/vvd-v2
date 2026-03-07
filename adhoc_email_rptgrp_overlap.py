# Check tactic ID overlap between VDT email RPT_GRP_CDs
from pyspark.sql import functions as F

for grp in ["PVDTAG01", "PVDTAG03", "PVDTAG11", "PVDTAG13"]:
    ids = result_df.filter(F.col("RPT_GRP_CD") == grp).select("TACTIC_ID").distinct().count()
    print(f"{grp}: {ids} tactic IDs")

t1 = set(str(r.TACTIC_ID) for r in result_df.filter(F.col("RPT_GRP_CD") == "PVDTAG01").select("TACTIC_ID").distinct().collect())
t3 = set(str(r.TACTIC_ID) for r in result_df.filter(F.col("RPT_GRP_CD") == "PVDTAG03").select("TACTIC_ID").distinct().collect())
print(f"\nPVDTAG01 & PVDTAG03 overlap: {len(t1 & t3)} of {len(t1)} / {len(t3)}")

# --- Size check on vendor feedback tables ---
# Run these to see if pulling everything is feasible
cursor = EDW.cursor()

cursor.execute("SELECT COUNT(*) FROM DTZV01.VENDOR_FEEDBACK_MASTER")
print(f"\nVENDOR_FEEDBACK_MASTER: {cursor.fetchone()[0]:,} rows")

cursor.execute("SELECT COUNT(*) FROM DTZV01.VENDOR_FEEDBACK_EVENT")
print(f"VENDOR_FEEDBACK_EVENT: {cursor.fetchone()[0]:,} rows")

cursor.execute("SELECT MIN(disposition_dt_tm), MAX(disposition_dt_tm) FROM DTZV01.VENDOR_FEEDBACK_EVENT")
r = cursor.fetchone()
print(f"Event date range: {r[0]} to {r[1]}")

cursor.close()
