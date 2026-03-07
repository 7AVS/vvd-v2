# Ad hoc: check if email RPT_GRP_CDs share tactic IDs (explains duplicate row counts)
# Run after Cell 4 (result_df persisted)

from pyspark.sql import functions as F

# Get email RPT_GRP_CDs
channel_map = (
    result_df.select("RPT_GRP_CD", "TACTIC_CELL_CD").distinct()
    .filter(F.col("TACTIC_CELL_CD").contains("EM"))
    .select("RPT_GRP_CD").distinct().collect()
)
email_grps = sorted([str(r.RPT_GRP_CD) for r in channel_map])

# Collect tactic IDs per group
grp_ids = {}
for grp in email_grps:
    ids = set(
        str(r.TACTIC_ID) for r in
        result_df.filter(F.col("RPT_GRP_CD") == grp)
        .select("TACTIC_ID").distinct().collect()
    )
    grp_ids[grp] = ids
    print(f"{grp}: {len(ids)} tactic IDs")

# Check pairwise overlap
print(f"\n{'Group A':<14} {'Group B':<14} {'A size':>7} {'B size':>7} {'Overlap':>8} {'% of A':>7}")
print("-" * 60)
for i, a in enumerate(email_grps):
    for b in email_grps[i+1:]:
        overlap = len(grp_ids[a] & grp_ids[b])
        if overlap > 0:
            pct = overlap / len(grp_ids[a]) * 100
            print(f"{a:<14} {b:<14} {len(grp_ids[a]):>7} {len(grp_ids[b]):>7} {overlap:>8} {pct:>6.1f}%")

if not any(len(grp_ids[a] & grp_ids[b]) > 0 for i, a in enumerate(email_grps) for b in email_grps[i+1:]):
    print("No overlap — all groups have unique tactic IDs.")
