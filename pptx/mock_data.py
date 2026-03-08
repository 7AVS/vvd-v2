"""
Generate mock CSVs that match the pipeline output format.
Run this locally to create test data for deck building.
"""

import csv
import os
import math
import random

random.seed(42)

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


# ── Campaign configs (realistic values from Run 1) ──────────────

MOCK_CAMPAIGNS = {
    "VCN": {
        "name": "Contextual Notification",
        "clients_action": 2166516, "clients_control": 119867,
        "success_rate_action": 0.59, "success_rate_control": 0.39,
        "window_days": 90, "p_value": 0.0001,
    },
    "VDA": {
        "name": "Black Friday",
        "clients_action": 59211, "clients_control": 3270,
        "success_rate_action": 0.80, "success_rate_control": 0.48,
        "window_days": 30, "p_value": 0.0001,
    },
    "VDT": {
        "name": "Activation Trigger",
        "clients_action": 385000, "clients_control": 42000,
        "success_rate_action": 58.2, "success_rate_control": 54.1,
        "window_days": 90, "p_value": 0.0001,
    },
    "VUI": {
        "name": "Usage Trigger",
        "clients_action": 1200000, "clients_control": 63000,
        "success_rate_action": 12.5, "success_rate_control": 11.8,
        "window_days": 90, "p_value": 0.023,
    },
    "VUT": {
        "name": "Tokenization Usage",
        "clients_action": 890000, "clients_control": 47000,
        "success_rate_action": 3.1, "success_rate_control": 3.4,
        "window_days": 90, "p_value": 0.15,
    },
    "VAW": {
        "name": "Add To Wallet",
        "clients_action": 320000, "clients_control": 80000,
        "success_rate_action": 5.8, "success_rate_control": 4.2,
        "window_days": 30, "p_value": 0.0001,
    },
}


def _vintage_curve(base_rate, window_days, noise=0.02):
    """Generate a realistic S-curve for vintage data."""
    points = []
    for day in range(0, window_days + 1):
        t = day / window_days
        # Logistic S-curve
        rate = base_rate * (1 / (1 + math.exp(-8 * (t - 0.3))))
        rate += random.gauss(0, noise * base_rate)
        rate = max(0, rate)
        points.append(round(rate, 4))
    return points


def generate_vintage_curves_csv():
    """Write vintage_curves.csv matching Cell 10 output format."""
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, "vintage_curves.csv")

    cohorts = ["2024Q3", "2024Q4", "2025Q1", "2025Q2"]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "MNE", "COHORT", "TST_GRP_CD", "RPT_GRP_CD",
            "METRIC", "DAY", "WINDOW_DAYS", "CLIENT_CNT",
            "SUCCESS_CNT", "RATE"
        ])

        for mne, cfg in MOCK_CAMPAIGNS.items():
            window = cfg["window_days"]

            for cohort in cohorts:
                for group, base_rate in [
                    ("TG4", cfg["success_rate_action"]),
                    ("TG7", cfg["success_rate_control"]),
                ]:
                    # Add some cohort variation
                    cohort_factor = 1.0 + random.gauss(0, 0.05)
                    rates = _vintage_curve(base_rate * cohort_factor, window)
                    total_clients = (cfg["clients_action"] if group == "TG4"
                                     else cfg["clients_control"]) // len(cohorts)

                    for day, rate in enumerate(rates):
                        success_cnt = int(total_clients * rate / 100)
                        writer.writerow([
                            mne, cohort, group, f"P{mne}AG01",
                            "PRIMARY", day, window,
                            total_clients, success_cnt, round(rate, 4)
                        ])

    print(f"  {path} ({len(MOCK_CAMPAIGNS) * len(cohorts) * 2} series)")
    return path


def generate_campaign_summary_csv():
    """Write campaign_summary.csv matching pipeline output."""
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, "campaign_summary.csv")

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "MNE", "TST_GRP_CD", "total_clients", "success_count",
            "success_rate", "lift", "p_value", "ci_lower", "ci_upper"
        ])

        for mne, cfg in MOCK_CAMPAIGNS.items():
            lift = cfg["success_rate_action"] - cfg["success_rate_control"]
            ci_half = abs(lift) * 0.15

            # Action row
            success_a = int(cfg["clients_action"] * cfg["success_rate_action"] / 100)
            writer.writerow([
                mne, "TG4", cfg["clients_action"], success_a,
                round(cfg["success_rate_action"], 4),
                round(lift, 4), cfg["p_value"],
                round(lift - ci_half, 4), round(lift + ci_half, 4)
            ])

            # Control row
            success_c = int(cfg["clients_control"] * cfg["success_rate_control"] / 100)
            writer.writerow([
                mne, "TG7", cfg["clients_control"], success_c,
                round(cfg["success_rate_control"], 4),
                0, 1.0, 0, 0
            ])

    print(f"  {path}")
    return path


def generate_srm_csv():
    """Write srm_check.csv."""
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, "srm_check.csv")

    ratios = {
        "VCN": (0.95, 0.9474), "VDA": (0.95, 0.9476),
        "VDT": (0.90, 0.9016), "VUI": (0.95, 0.9501),
        "VUT": (0.95, 0.9498), "VAW": (0.80, 0.8001),
    }

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["MNE", "COHORT", "expected_ratio", "actual_ratio",
                         "chi2", "p_value", "warning"])

        for mne, (expected, actual) in ratios.items():
            for cohort in ["2024Q3", "2024Q4", "2025Q1", "2025Q2"]:
                chi2 = random.uniform(0.01, 3.0)
                p = random.uniform(0.05, 0.99)
                # VCN always has SRM warning
                if mne == "VCN":
                    chi2 = random.uniform(50, 200)
                    p = 0.0000
                warning = "YES" if p < 0.01 else "NO"
                writer.writerow([
                    mne, cohort, expected, round(actual + random.gauss(0, 0.002), 4),
                    round(chi2, 2), round(p, 4), warning
                ])

    print(f"  {path}")
    return path


def generate_all():
    """Generate all mock CSVs."""
    print("Generating mock data:")
    generate_vintage_curves_csv()
    generate_campaign_summary_csv()
    generate_srm_csv()
    print("Done.")


if __name__ == "__main__":
    generate_all()
