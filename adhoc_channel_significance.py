# Adhoc: channel-level significance — MNE × RPT_GRP_CD rates + lift + z-test
import pandas as pd

ch_agg = (
    result_df
    .groupBy("MNE", "RPT_GRP_CD", "TST_GRP_CD")
    .agg(
        F.count("*").alias("deployments"),
        F.countDistinct("CLNT_NO").alias("clients"),
        F.sum("SUCCESS").alias("successes"),
    )
    .collect()
)

ch_data = {}
for r in ch_agg:
    key = (str(r.MNE), str(r.RPT_GRP_CD))
    ch_data.setdefault(key, {})[str(r.TST_GRP_CD)] = {
        "deployments": int(r.deployments),
        "clients": int(r.clients),
        "successes": int(r.successes),
    }

ch_rows = []
for (mne, rpt), groups in sorted(ch_data.items()):
    a = groups.get(ACTION_GROUP)
    c = groups.get(CONTROL_GROUP)
    if not a or not c:
        continue
    a_rate = a["successes"] / a["deployments"] * 100 if a["deployments"] > 0 else 0
    c_rate = c["successes"] / c["deployments"] * 100 if c["deployments"] > 0 else 0
    abs_lift = a_rate - c_rate
    rel_lift = ((a_rate - c_rate) / c_rate * 100) if c_rate > 0 else 0
    p_val = compute_sig(a["successes"], a["deployments"], c["successes"], c["deployments"])
    sig = "99.9%" if p_val < 0.001 else "99%" if p_val < 0.01 else "95%" if p_val < 0.05 else "No"
    ch_rows.append({
        "MNE": mne,
        "RPT_GRP_CD": rpt,
        "action_deployments": a["deployments"],
        "action_clients": a["clients"],
        "action_successes": a["successes"],
        "control_deployments": c["deployments"],
        "control_clients": c["clients"],
        "control_successes": c["successes"],
        "action_rate_pct": round(a_rate, 4),
        "control_rate_pct": round(c_rate, 4),
        "abs_lift_pp": round(abs_lift, 4),
        "rel_lift_pct": round(rel_lift, 2),
        "p_value": round(p_val, 6),
        "sig_flag": sig,
    })

ch_df = pd.DataFrame(ch_rows).sort_values(["MNE", "RPT_GRP_CD"]).reset_index(drop=True)

print("=" * 90)
print(f"CHANNEL SIGNIFICANCE: {len(ch_df)} rows (MNE × RPT_GRP_CD)")
print("=" * 90)
print(f"{'MNE':<6} {'RPT_GRP_CD':<14} {'A_Deploys':>10} {'A_Rate':>8} {'C_Rate':>8} {'Lift':>8} {'p-val':>9} {'Sig':>6}")
print("-" * 76)
for _, r in ch_df.iterrows():
    print(f"{r['MNE']:<6} {r['RPT_GRP_CD']:<14} {r['action_deployments']:>10,} "
          f"{r['action_rate_pct']:>7.2f}% {r['control_rate_pct']:>7.2f}% "
          f"{r['abs_lift_pp']:>+7.2f}% {r['p_value']:>9.4f} {r['sig_flag']:>6}")

download_csv(ch_df, "vvd_v3_channel_significance.csv")
