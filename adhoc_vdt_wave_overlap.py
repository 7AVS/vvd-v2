# VDT Wave Overlap: Are AG11/AG13 clients a subset of AG01/AG03?
# Run in Lumina after Cells 1-2 (tactic_df must be persisted)

from pyspark.sql.functions import trim, col

vdt = tactic_df.filter(col("MNE") == "VDT").filter(trim(col("TST_GRP_CD")) == "TG4")

ag01 = vdt.filter(trim(col("RPT_GRP_CD")) == "PVDTAG01").select("CLNT_NO").distinct()
ag03 = vdt.filter(trim(col("RPT_GRP_CD")) == "PVDTAG03").select("CLNT_NO").distinct()
ag11 = vdt.filter(trim(col("RPT_GRP_CD")) == "PVDTAG11").select("CLNT_NO").distinct()
ag13 = vdt.filter(trim(col("RPT_GRP_CD")) == "PVDTAG13").select("CLNT_NO").distinct()

# Overlap counts
ag11_in_ag01 = ag11.join(ag01, "CLNT_NO", "inner").count()
ag13_in_ag03 = ag13.join(ag03, "CLNT_NO", "inner").count()
ag11_total = ag11.count()
ag13_total = ag13.count()

print(f"AG11 total: {ag11_total}")
print(f"AG11 in AG01: {ag11_in_ag01} ({ag11_in_ag01/ag11_total*100:.1f}%)")
print(f"AG13 total: {ag13_total}")
print(f"AG13 in AG03: {ag13_in_ag03} ({ag13_total and ag13_in_ag03/ag13_total*100:.1f}%)")

# Also check: any AG11 clients in AG02 (DO)?
ag02 = vdt.filter(trim(col("RPT_GRP_CD")) == "PVDTAG02").select("CLNT_NO").distinct()
ag11_in_ag02 = ag11.join(ag02, "CLNT_NO", "inner").count()
print(f"AG11 in AG02 (DO): {ag11_in_ag02}")
