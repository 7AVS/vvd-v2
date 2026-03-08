"""
VVD v2 — PowerPoint Deck Builder

Reads CSVs from data/, builds a 4-slide dashboard deck.
Supports both Track A (Plotly images) and Track B (native charts).

Usage:
    python build_deck.py              # Track B (native, default)
    python build_deck.py --track-a    # Track A (Plotly images)
    python build_deck.py --both       # Both tracks, separate files
"""

import os
import sys
import csv

from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.enum.text import PP_ALIGN
from pptx.enum.shapes import MSO_SHAPE

from theme import (
    DARK_BLUE, BRIGHT_BLUE, OCEAN, WARM_YELLOW, TUNDRA, SUNBURST,
    GRAY, COOL_WHITE, WHITE, BLACK, FONT_FAMILY,
    SLIDE_WIDTH, SLIDE_HEIGHT, CAMPAIGNS, SLIDE_GROUPS,
)
from tables import add_kpi_card, add_text_box, add_bullet_list, add_summary_table
from track_b import read_csv, add_vintage_curve, add_lift_bar_chart

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
OUTPUT_DIR = os.path.dirname(__file__)


def load_data():
    """Load all CSVs from data/."""
    vintage = read_csv(os.path.join(DATA_DIR, "vintage_curves.csv"))
    summary = read_csv(os.path.join(DATA_DIR, "campaign_summary.csv"))
    srm = read_csv(os.path.join(DATA_DIR, "srm_check.csv"))
    return vintage, summary, srm


def new_presentation():
    """Create a blank 16:9 presentation."""
    prs = Presentation()
    prs.slide_width = SLIDE_WIDTH
    prs.slide_height = SLIDE_HEIGHT
    return prs


def add_blank_slide(prs):
    """Add a blank slide."""
    return prs.slides.add_slide(prs.slide_layouts[6])


# ── Slide Builders ───────────────────────────────────────────────

def build_title_slide(prs, summary):
    """Slide 1: Title + Executive Summary."""
    slide = add_blank_slide(prs)

    # Title bar
    title_bar = slide.shapes.add_shape(
        MSO_SHAPE.RECTANGLE,
        Inches(0), Inches(0), SLIDE_WIDTH, Inches(1.2)
    )
    title_bar.fill.solid()
    title_bar.fill.fore_color.rgb = DARK_BLUE
    title_bar.line.fill.background()

    add_text_box(slide, "VVD Campaign Measurement Results",
                 Inches(0.5), Inches(0.15), Inches(12), Inches(0.5),
                 font_size=Pt(32), font_color=WHITE, bold=True,
                 align=PP_ALIGN.LEFT)

    add_text_box(slide, "Virtual Visa Debit — v2 Analysis | 6 Campaigns | 2024-2026",
                 Inches(0.5), Inches(0.7), Inches(12), Inches(0.35),
                 font_size=Pt(14), font_color=COOL_WHITE,
                 align=PP_ALIGN.LEFT)

    # KPI cards row
    action_rows = [r for r in summary if r["TST_GRP_CD"].strip() == "TG4"]
    total_clients = sum(int(r["total_clients"]) for r in action_rows)
    avg_lift = sum(float(r["lift"]) for r in action_rows) / len(action_rows)
    sig_count = sum(1 for r in action_rows if float(r["p_value"]) < 0.05)

    kpi_top = Inches(1.5)
    kpi_w = Inches(2.8)
    kpi_h = Inches(1.3)
    kpi_gap = Inches(0.3)

    add_kpi_card(slide, "Total Clients (Action)", f"{total_clients:,}",
                 left=Inches(0.5), top=kpi_top, width=kpi_w, height=kpi_h)

    add_kpi_card(slide, "Avg Lift", f"{avg_lift:+.2f}pp",
                 sentiment="positive" if avg_lift > 0 else "negative",
                 left=Inches(0.5) + kpi_w + kpi_gap, top=kpi_top,
                 width=kpi_w, height=kpi_h)

    add_kpi_card(slide, "Campaigns Significant", f"{sig_count} / {len(action_rows)}",
                 left=Inches(0.5) + (kpi_w + kpi_gap) * 2, top=kpi_top,
                 width=kpi_w, height=kpi_h)

    add_kpi_card(slide, "Campaigns Measured", str(len(action_rows)),
                 left=Inches(0.5) + (kpi_w + kpi_gap) * 3, top=kpi_top,
                 width=kpi_w, height=kpi_h)

    # Summary table
    add_summary_table(slide, summary, left=Inches(0.5), top=Inches(3.2),
                      width=Inches(12.3))

    # Bottom insights
    add_text_box(slide, "Key Findings",
                 Inches(0.5), Inches(5.8), Inches(12), Inches(0.3),
                 font_size=Pt(14), bold=True)

    findings = [
        "VCN & VDA (acquisition): Consistent positive lift — continue campaigns",
        "VDT (activation): Strong lift but small addressable population (~5% need activation)",
        "VUI (usage): Marginal lift — optimization opportunity",
        "VUT (tokenization): Negative ROI — recommend discontinuation",
        "VAW (add to wallet): Positive lift at scale — expand",
    ]
    add_bullet_list(slide, findings,
                    Inches(0.5), Inches(6.1), Inches(12), Inches(1.3),
                    font_size=Pt(10))

    return slide


def build_campaign_slide(prs, vintage, summary, group_key, track="b"):
    """Build a dense dashboard slide for a campaign group."""
    group = SLIDE_GROUPS[group_key]
    mnes = group["mnes"]
    slide = add_blank_slide(prs)

    # Header bar
    header = slide.shapes.add_shape(
        MSO_SHAPE.RECTANGLE,
        Inches(0), Inches(0), SLIDE_WIDTH, Inches(0.7)
    )
    header.fill.solid()
    header.fill.fore_color.rgb = DARK_BLUE
    header.line.fill.background()

    add_text_box(slide, group["title"],
                 Inches(0.3), Inches(0.1), Inches(8), Inches(0.45),
                 font_size=Pt(22), font_color=WHITE, bold=True)

    # Campaign subtitle
    subtitle_parts = [f"{m} — {CAMPAIGNS[m]['name']}" for m in mnes]
    add_text_box(slide, " | ".join(subtitle_parts),
                 Inches(8), Inches(0.15), Inches(5), Inches(0.4),
                 font_size=Pt(11), font_color=COOL_WHITE, align=PP_ALIGN.RIGHT)

    # ── Layout: 2 campaigns side by side ─────────────────────────
    # Each campaign gets: vintage curve (left) + KPIs + findings (right)
    n_campaigns = len(mnes)
    campaign_height = 3.0 if n_campaigns == 2 else 5.5
    y_start = 0.85

    for idx, mne in enumerate(mnes):
        y_offset = Inches(y_start + idx * (campaign_height + 0.2))

        # Campaign label
        label_bar = slide.shapes.add_shape(
            MSO_SHAPE.RECTANGLE,
            Inches(0.3), y_offset, Inches(12.7), Inches(0.3)
        )
        label_bar.fill.solid()
        label_bar.fill.fore_color.rgb = BRIGHT_BLUE
        label_bar.line.fill.background()

        add_text_box(slide, f"{mne} — {CAMPAIGNS[mne]['name']}",
                     Inches(0.5), y_offset, Inches(6), Inches(0.3),
                     font_size=Pt(12), font_color=WHITE, bold=True)

        chart_top = y_offset + Inches(0.4)

        # Vintage curve (left side)
        if track == "b":
            add_vintage_curve(
                slide, vintage, mne, metric="PRIMARY",
                left=Inches(0.3), top=chart_top,
                width=Inches(6.5), height=Inches(campaign_height - 0.7)
            )
        else:
            # Track A: embed PNG
            img_path = os.path.join(DATA_DIR, "charts", f"{mne}_vintage_primary.png")
            if os.path.exists(img_path):
                slide.shapes.add_picture(
                    img_path, Inches(0.3), chart_top,
                    Inches(6.5), Inches(campaign_height - 0.7)
                )

        # KPI cards (right side)
        action_row = next(
            (r for r in summary
             if r["MNE"] == mne and r["TST_GRP_CD"].strip() == "TG4"), None
        )
        control_row = next(
            (r for r in summary
             if r["MNE"] == mne and r["TST_GRP_CD"].strip() == "TG7"), None
        )

        if action_row:
            lift = float(action_row["lift"])
            p_val = float(action_row["p_value"])
            rate_a = float(action_row["success_rate"])
            rate_c = float(control_row["success_rate"]) if control_row else 0

            kpi_left = Inches(7.2)
            kpi_w = Inches(1.8)
            kpi_h = Inches(0.9)

            add_kpi_card(slide, "Lift", f"{lift:+.2f}pp",
                         sentiment="positive" if lift > 0 else "negative",
                         left=kpi_left, top=chart_top,
                         width=kpi_w, height=kpi_h)

            add_kpi_card(slide, "Action Rate", f"{rate_a:.2f}%",
                         left=kpi_left + kpi_w + Inches(0.2), top=chart_top,
                         width=kpi_w, height=kpi_h)

            add_kpi_card(slide, "Control Rate", f"{rate_c:.2f}%",
                         left=kpi_left + (kpi_w + Inches(0.2)) * 2, top=chart_top,
                         width=kpi_w, height=kpi_h)

            # Significance badge
            sig = "***" if p_val < 0.001 else "**" if p_val < 0.01 else "*" if p_val < 0.05 else "ns"
            sig_sentiment = "positive" if p_val < 0.05 else "negative"
            add_kpi_card(slide, "p-value", f"{p_val:.4f}" if p_val >= 0.0001 else "<0.0001",
                         sublabel=sig,
                         sentiment=sig_sentiment if p_val < 0.05 else None,
                         left=kpi_left, top=chart_top + kpi_h + Inches(0.15),
                         width=kpi_w, height=kpi_h)

            # Client count
            add_kpi_card(slide, "Clients (Action)",
                         f"{int(action_row['total_clients']):,}",
                         left=kpi_left + kpi_w + Inches(0.2),
                         top=chart_top + kpi_h + Inches(0.15),
                         width=kpi_w * 2 + Inches(0.2), height=kpi_h)

        # Insight bullets (bottom right)
        insight_top = chart_top + Inches(2.0)
        insights = _get_campaign_insights(mne, action_row)
        add_bullet_list(slide, insights,
                        Inches(7.2), insight_top, Inches(5.8), Inches(0.8),
                        font_size=Pt(9))

    return slide


def _get_campaign_insights(mne, action_row):
    """Return insight bullets for a campaign. Customize per campaign."""
    if not action_row:
        return ["No data available"]

    lift = float(action_row["lift"])
    p_val = float(action_row["p_value"])

    base = []
    if p_val < 0.05:
        base.append(f"Statistically significant lift of {lift:+.2f}pp")
    else:
        base.append(f"Lift of {lift:+.2f}pp — NOT statistically significant")

    # Campaign-specific
    notes = {
        "VCN": ["Persistent SRM warnings (p=0.0000) across all cohorts",
                "Largest campaign by volume — consider SRM investigation"],
        "VDA": ["Seasonal campaign (Black Friday) — strong but time-limited",
                "Highest lift among acquisition campaigns"],
        "VDT": ["Only ~5% of cards need manual activation (95% auto-activate)",
                "High lift but small addressable market"],
        "VUI": ["Broad reach — largest usage population",
                "Marginal lift suggests optimization needed"],
        "VUT": ["Negative ROI — recommend discontinuation",
                "Control performs equal or better than Action"],
        "VAW": ["Positive lift at scale",
                "Consider expanding budget allocation"],
    }

    return base + notes.get(mne, [])


# ── Main ─────────────────────────────────────────────────────────

def build_deck(track="b", output_name=None):
    """Build the complete deck."""
    print(f"Building deck (Track {'A' if track == 'a' else 'B'})...")

    vintage, summary, srm = load_data()
    prs = new_presentation()

    # Slide 1: Title + Exec Summary
    build_title_slide(prs, summary)

    # Slides 2-4: Campaign groups
    for group_key in ["acquisition", "activation_usage", "provisioning"]:
        build_campaign_slide(prs, vintage, summary, group_key, track=track)

    # Save
    if output_name is None:
        output_name = f"VVD_v2_Report_Track{'A' if track == 'a' else 'B'}.pptx"

    output_path = os.path.join(OUTPUT_DIR, output_name)
    prs.save(output_path)
    print(f"Saved: {output_path}")
    print(f"  {len(prs.slides)} slides")
    return output_path


if __name__ == "__main__":
    if "--track-a" in sys.argv:
        build_deck(track="a")
    elif "--both" in sys.argv:
        build_deck(track="a")
        build_deck(track="b")
    else:
        build_deck(track="b")
