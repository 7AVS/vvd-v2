# Adhoc: channel-level significance — MNE × TACTIC_CELL_CD rates + lift + z-test
# Control groups have TACTIC_CELL_CD=XX (not communicated), so we compute
# Action rates per channel and compare each against the MNE-level Control rate.
import pandas as pd

# Action: group by MNE × TACTIC_CELL_CD
action_agg = (
    result_df
    .filter(F.trim("TST_GRP_CD") == ACTION_GROUP)
    .filter(F.trim("TACTIC_CELL_CD") != "XX")
    .withColumn("CHANNEL", F.trim("TACTIC_CELL_CD"))
    .groupBy("MNE", "CHANNEL")
    .agg(
        F.count("*").alias("deployments"),
        F.countDistinct("CLNT_NO").alias("clients"),
        F.sum("SUCCESS").alias("successes"),
    )
    .collect()
)

# Control: group by MNE only (channel-agnostic holdout)
control_agg = (
    result_df
    .filter(F.trim("TST_GRP_CD") == CONTROL_GROUP)
    .groupBy("MNE")
    .agg(
        F.count("*").alias("deployments"),
        F.countDistinct("CLNT_NO").alias("clients"),
        F.sum("SUCCESS").alias("successes"),
    )
    .collect()
)

ctrl = {}
for r in control_agg:
    ctrl[str(r.MNE)] = {
        "deployments": int(r.deployments),
        "clients": int(r.clients),
        "successes": int(r.successes),
    }

ch_rows = []
for r in action_agg:
    mne = str(r.MNE)
    channel = str(r.CHANNEL)
    c = ctrl.get(mne)
    if not c:
        continue
    a = {"deployments": int(r.deployments), "clients": int(r.clients), "successes": int(r.successes)}
    a_rate = a["successes"] / a["deployments"] * 100 if a["deployments"] > 0 else 0
    c_rate = c["successes"] / c["deployments"] * 100 if c["deployments"] > 0 else 0
    abs_lift = a_rate - c_rate
    rel_lift = ((a_rate - c_rate) / c_rate * 100) if c_rate > 0 else 0
    p_val = compute_sig(a["successes"], a["deployments"], c["successes"], c["deployments"])
    sig = "99.9%" if p_val < 0.001 else "99%" if p_val < 0.01 else "95%" if p_val < 0.05 else "No"
    ch_rows.append({
        "MNE": mne,
        "CHANNEL": channel,
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

ch_df = pd.DataFrame(ch_rows).sort_values(["MNE", "CHANNEL"]).reset_index(drop=True)

print("=" * 90)
print(f"CHANNEL SIGNIFICANCE: {len(ch_df)} rows (MNE × CHANNEL)")
print("=" * 90)
print(f"{'MNE':<6} {'CHANNEL':<10} {'A_Deploys':>10} {'A_Rate':>8} {'C_Rate':>8} {'Lift':>8} {'p-val':>9} {'Sig':>6}")
print("-" * 76)
for _, r in ch_df.iterrows():
    print(f"{r['MNE']:<6} {r['CHANNEL']:<10} {r['action_deployments']:>10,} "
          f"{r['action_rate_pct']:>7.2f}% {r['control_rate_pct']:>7.2f}% "
          f"{r['abs_lift_pp']:>+7.2f}% {r['p_value']:>9.4f} {r['sig_flag']:>6}")

download_csv(ch_df, "vvd_v2_channel_significance.csv")
