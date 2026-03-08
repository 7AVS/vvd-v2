"""
Track A — Plotly PNG Charts Embedded as Images
High-res PNG export → embed on slides.
Charts are NOT editable but look identical to notebook output.
"""

import os
from collections import defaultdict

from pptx.util import Inches

from theme import HEX, FONT_FAMILY, get_plotly_template


def init_plotly():
    """Register VVD brand template as Plotly default."""
    import plotly.io as pio
    pio.templates["vvd_brand"] = get_plotly_template()
    pio.templates.default = "vvd_brand"


def save_chart(fig, path, width=1200, height=675, scale=3):
    """Export Plotly figure as high-res PNG."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fig.write_image(path, width=width, height=height, scale=scale)


def add_image_to_slide(slide, image_path,
                       left=Inches(0.3), top=Inches(1.5),
                       width=Inches(6), height=Inches(3.5)):
    """Place a PNG image on a slide."""
    return slide.shapes.add_picture(image_path, left, top, width, height)


# ── Vintage Curve (Plotly) ───────────────────────────────────────

def make_vintage_fig(vintage_data, mne, metric="PRIMARY"):
    """
    Create a Plotly vintage curve figure for a specific MNE + metric.
    Returns a go.Figure.
    """
    import plotly.graph_objects as go

    rows = [r for r in vintage_data
            if r["MNE"] == mne and r["METRIC"] == metric]

    if not rows:
        return None

    # Separate Action (TG4) and Control (TG7)
    action_by_day = defaultdict(list)
    control_by_day = defaultdict(list)

    for r in rows:
        day = int(r["DAY"])
        rate = float(r["RATE"])
        if r["TST_GRP_CD"].strip() == "TG4":
            action_by_day[day].append(rate)
        elif r["TST_GRP_CD"].strip() == "TG7":
            control_by_day[day].append(rate)

    days = sorted(action_by_day.keys())

    fig = go.Figure()

    # Action — solid
    fig.add_trace(go.Scatter(
        x=days,
        y=[sum(action_by_day[d]) / len(action_by_day[d]) for d in days],
        name="Action (TG4)",
        mode="lines+markers",
        line=dict(color=HEX["ocean"], width=3),
        marker=dict(size=4),
    ))

    # Control — dashed
    if control_by_day:
        ctrl_days = sorted(control_by_day.keys())
        fig.add_trace(go.Scatter(
            x=ctrl_days,
            y=[sum(control_by_day[d]) / len(control_by_day[d]) for d in ctrl_days],
            name="Control (TG7)",
            mode="lines+markers",
            line=dict(color=HEX["gray"], width=2, dash="dash"),
            marker=dict(size=3),
        ))

    fig.update_layout(
        title=f"{mne} — Vintage Curve ({metric.title()})",
        xaxis_title="Days Since Treatment Start",
        yaxis_title="Cumulative Success Rate (%)",
        legend=dict(x=0.02, y=0.98),
        margin=dict(l=60, r=30, t=50, b=50),
    )

    return fig


# ── Lift Bar (Plotly) ────────────────────────────────────────────

def make_lift_fig(summary_data, mnes=None):
    """
    Create a Plotly bar chart of lift by campaign.
    Bars colored by sign: positive = tundra, negative = sunburst.
    """
    import plotly.graph_objects as go

    if mnes is None:
        mnes = ["VCN", "VDA", "VDT", "VUI", "VUT", "VAW"]

    rows = [r for r in summary_data
            if r["MNE"] in mnes and r["TST_GRP_CD"].strip() == "TG4"]
    rows.sort(key=lambda r: mnes.index(r["MNE"]))

    if not rows:
        return None

    lifts = [float(r["lift"]) for r in rows]
    colors = [HEX["tundra"] if l >= 0 else HEX["sunburst"] for l in lifts]

    fig = go.Figure(go.Bar(
        x=[r["MNE"] for r in rows],
        y=lifts,
        marker_color=colors,
        text=[f"{l:+.2f}pp" for l in lifts],
        textposition="outside",
    ))

    fig.update_layout(
        title="Campaign Lift (Action vs Control)",
        xaxis_title="Campaign",
        yaxis_title="Lift (pp)",
        margin=dict(l=60, r=30, t=50, b=50),
    )

    return fig


# ── Batch Export ─────────────────────────────────────────────────

def export_all_charts(vintage_data, summary_data, output_dir="pptx/data/charts"):
    """
    Generate all PNGs for Track A.
    Call this once, then build_deck places images.
    """
    init_plotly()
    from theme import CAMPAIGNS

    os.makedirs(output_dir, exist_ok=True)

    for mne in CAMPAIGNS:
        # Vintage curves
        for metric in ["PRIMARY", "SECONDARY"]:
            fig = make_vintage_fig(vintage_data, mne, metric)
            if fig:
                save_chart(fig, f"{output_dir}/{mne}_vintage_{metric.lower()}.png")

        # Email metrics (if present)
        for metric in ["email_open", "email_click", "email_sent"]:
            fig = make_vintage_fig(vintage_data, mne, metric)
            if fig:
                save_chart(fig, f"{output_dir}/{mne}_vintage_{metric}.png")

    # Portfolio lift bar
    fig = make_lift_fig(summary_data)
    if fig:
        save_chart(fig, f"{output_dir}/portfolio_lift.png")

    print(f"Charts exported to {output_dir}/")
