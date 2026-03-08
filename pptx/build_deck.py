"""
VVD v2 — PowerPoint Deck Builder

4-slide spotlight deck for Payment LOB Power Pack meeting.
~10 minutes, narrative-driven.

Slides:
  1. Campaign Overview (funnel table + KPI cards)
  2. Top of Funnel — Acquisition & Activation (VCN, VDA, VDT)
  3. Bottom of Funnel — Usage & Wallet Provisioning (VUI, VUT, VAW)
  4. Recommendations & Next Investigations

Usage:
    python build_deck.py              # Track B (native, default)
    python build_deck.py --track-a    # Track A (Plotly images)
    python build_deck.py --both       # Both tracks, separate files
"""

import os
import sys

from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.enum.text import PP_ALIGN
from pptx.enum.shapes import MSO_SHAPE

from theme import (
    DARK_BLUE, BRIGHT_BLUE, OCEAN, WARM_YELLOW, TUNDRA, SUNBURST,
    GRAY, COOL_WHITE, WHITE, BLACK, LIGHT_GRAY, FONT_FAMILY,
    SLIDE_WIDTH, SLIDE_HEIGHT, CAMPAIGNS, SLIDE_GROUPS,
)
from tables import add_kpi_card, add_text_box, add_bullet_list, add_summary_table
from track_b import read_csv, add_vintage_curve

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
OUTPUT_DIR = os.path.dirname(__file__)


# ── Hardcoded campaign data (from Lumina runs) ──────────────────

CAMPAIGN_DATA = {
    "VCN": {
        "lift": 0.18, "sig": "***", "clients": 1_999_540, "revenue": "$281K",
        "channel": "MB", "split": "95/5",
        "insight": "Persistent SRM \u2014 investigation needed",
    },
    "VDA": {
        "lift": 0.31, "sig": "***", "clients": 894_779, "revenue": "$218K",
        "channel": "IM+EM_IM", "split": "95/5",
        "insight": "Strongest relative lift among acquisition (+29.9%)",
    },
    "VDT": {
        "lift": 4.65, "sig": "***", "clients": 132_015, "revenue": "$480K",
        "channel": "EM", "split": "90/10",
        "insight": "95% of cards auto-activate \u2014 shrinking TAM",
    },
    "VUI": {
        "lift": 0.78, "sig": "ns", "clients": 158_895, "revenue": "N/A",
        "channel": "EM_IM+EM+IM", "split": "95/5",
        "insight": "Not significant \u2014 config change recommended Oct 2025",
    },
    "VUT": {
        "lift": 0.02, "sig": "ns", "clients": 784_849, "revenue": "N/A",
        "channel": "EM", "split": "95/5",
        "insight": "Zero lift \u2014 recommend discontinuation",
    },
    "VAW": {
        "lift": 2.62, "sig": "***", "clients": 412_568, "revenue": "$846K*",
        "channel": "IM", "split": "80/20",
        "insight": "Strongest campaign \u2014 expand budget",
    },
}


def load_data():
    """Load CSVs from data/."""
    vintage_path = os.path.join(DATA_DIR, "vintage_curves.csv")
    summary_path = os.path.join(DATA_DIR, "campaign_summary.csv")
    vintage = read_csv(vintage_path) if os.path.exists(vintage_path) else []
    summary = read_csv(summary_path) if os.path.exists(summary_path) else []
    return vintage, summary


def new_presentation():
    """Create a blank 16:9 presentation."""
    prs = Presentation()
    prs.slide_width = SLIDE_WIDTH
    prs.slide_height = SLIDE_HEIGHT
    return prs


def add_blank_slide(prs):
    """Add a blank slide."""
    return prs.slides.add_slide(prs.slide_layouts[6])


# ── Slide 1: Campaign Overview ──────────────────────────────────

def build_overview_slide(prs):
    """Slide 1: Campaign overview with funnel table and KPI cards."""
    slide = add_blank_slide(prs)

    # Title bar
    title_bar = slide.shapes.add_shape(
        MSO_SHAPE.RECTANGLE,
        Inches(0), Inches(0), SLIDE_WIDTH, Inches(1.2)
    )
    title_bar.fill.solid()
    title_bar.fill.fore_color.rgb = DARK_BLUE
    title_bar.line.fill.background()

    add_text_box(slide, "VVD Campaign Measurement \u2014 2025-2026",
                 Inches(0.5), Inches(0.15), Inches(12), Inches(0.5),
                 font_size=Pt(30), font_color=WHITE, bold=True,
                 align=PP_ALIGN.LEFT)

    add_text_box(slide, "6 campaigns | 3.4M clients | $499K\u2013$1.8M incremental NIBT",
                 Inches(0.5), Inches(0.7), Inches(12), Inches(0.35),
                 font_size=Pt(14), font_color=COOL_WHITE,
                 align=PP_ALIGN.LEFT)

    # ── Funnel table ──
    table_top = Inches(1.5)
    headers = ["Campaign", "MNE", "Goal", "Channel", "Split", "Funnel"]
    mnes_order = ["VCN", "VDA", "VDT", "VUI", "VUT", "VAW"]
    n_rows = len(mnes_order) + 1
    n_cols = len(headers)
    row_height = Inches(0.35)

    table_shape = slide.shapes.add_table(
        n_rows, n_cols,
        Inches(0.5), table_top, Inches(12.3), row_height * n_rows
    )
    table = table_shape.table

    # Set column widths
    col_widths = [Inches(3.0), Inches(1.0), Inches(2.8), Inches(2.2), Inches(1.3), Inches(2.0)]
    for j, w in enumerate(col_widths):
        table.columns[j].width = w

    # Header row
    for j, header in enumerate(headers):
        cell = table.cell(0, j)
        cell.text = header
        _style_table_cell(cell, bold=True, font_color=WHITE, fill_color=DARK_BLUE)

    # Data rows
    for i, mne in enumerate(mnes_order, start=1):
        cd = CAMPAIGN_DATA[mne]
        cm = CAMPAIGNS[mne]
        funnel = "Top" if cm["group"] == "top_funnel" else "Bottom"
        row_data = [cm["name"], mne, cm["goal"], cd["channel"], cd["split"], funnel]

        row_bg = COOL_WHITE if i % 2 == 0 else WHITE
        for j, val in enumerate(row_data):
            cell = table.cell(i, j)
            cell.text = val
            _style_table_cell(cell, fill_color=row_bg)

    # ── KPI cards row ──
    kpi_top = Inches(4.6)
    kpi_w = Inches(2.8)
    kpi_h = Inches(1.1)
    kpi_gap = Inches(0.3)
    kpi_left = Inches(0.5)

    total_clients = sum(d["clients"] for d in CAMPAIGN_DATA.values())

    add_kpi_card(slide, "Total Clients", f"{total_clients:,}",
                 left=kpi_left, top=kpi_top, width=kpi_w, height=kpi_h)

    add_kpi_card(slide, "Significant", "4 / 6",
                 sentiment="positive",
                 left=kpi_left + kpi_w + kpi_gap, top=kpi_top,
                 width=kpi_w, height=kpi_h)

    add_kpi_card(slide, "Conservative NIBT", "$499K",
                 sublabel="VCN + VDA",
                 sentiment="positive",
                 left=kpi_left + (kpi_w + kpi_gap) * 2, top=kpi_top,
                 width=kpi_w, height=kpi_h)

    add_kpi_card(slide, "Extended NIBT", "$1.83M",
                 sublabel="+ VDT, VAW*",
                 sentiment="positive",
                 left=kpi_left + (kpi_w + kpi_gap) * 3, top=kpi_top,
                 width=kpi_w, height=kpi_h)

    # Footnote
    add_text_box(slide, "NIBT: $78.21 per incremental client. *VAW uses acquisition proxy \u2014 not validated for provisioning.",
                 Inches(0.5), Inches(5.9), Inches(12), Inches(0.3),
                 font_size=Pt(8), font_color=GRAY, align=PP_ALIGN.LEFT)

    return slide


# ── Slides 2-3: Campaign Group (3-across layout) ────────────────

def build_campaign_slide(prs, vintage, summary, group_key, track="b"):
    """Build a 3-across campaign slide for a funnel group."""
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
                 Inches(0.3), Inches(0.08), Inches(7), Inches(0.45),
                 font_size=Pt(22), font_color=WHITE, bold=True)

    subtitle = " | ".join(mnes)
    add_text_box(slide, subtitle,
                 Inches(8), Inches(0.15), Inches(5), Inches(0.4),
                 font_size=Pt(13), font_color=COOL_WHITE, align=PP_ALIGN.RIGHT)

    # ── 3-across layout ──
    col_width = Inches(4.1)
    col_gap = Inches(0.15)
    x_start = Inches(0.25)

    for idx, mne in enumerate(mnes):
        x_offset = x_start + (col_width + col_gap) * idx
        cd = CAMPAIGN_DATA[mne]
        cm = CAMPAIGNS[mne]
        is_star = (mne == "VAW")
        is_muted = (mne == "VUT")

        # Campaign label bar
        label_color = TUNDRA if is_star else GRAY if is_muted else BRIGHT_BLUE
        label_bar = slide.shapes.add_shape(
            MSO_SHAPE.RECTANGLE,
            x_offset, Inches(0.85), col_width, Inches(0.3)
        )
        label_bar.fill.solid()
        label_bar.fill.fore_color.rgb = label_color
        label_bar.line.fill.background()

        add_text_box(slide, f"{mne} \u2014 {cm['name']}",
                     x_offset + Inches(0.1), Inches(0.85),
                     col_width - Inches(0.2), Inches(0.3),
                     font_size=Pt(11), font_color=WHITE, bold=True)

        # Vintage curve chart
        chart_top = Inches(1.25)
        chart_w = Inches(3.8)
        chart_h = Inches(2.0)

        if track == "b":
            add_vintage_curve(
                slide, vintage, mne, metric="PRIMARY",
                left=x_offset + Inches(0.1), top=chart_top,
                width=chart_w, height=chart_h
            )
        else:
            img_path = os.path.join(DATA_DIR, "charts", f"{mne}_vintage_primary.png")
            if os.path.exists(img_path):
                slide.shapes.add_picture(
                    img_path, x_offset + Inches(0.1), chart_top,
                    chart_w, chart_h
                )

        # ── KPI cards below chart ──
        kpi_row_top = Inches(3.4)
        kpi_w = Inches(1.2)
        kpi_h = Inches(0.7)
        kpi_gap_x = Inches(0.1)

        # Lift card
        lift_val = cd["lift"]
        lift_sentiment = "positive" if lift_val > 0 and cd["sig"] != "ns" else "negative" if cd["sig"] == "ns" else "positive"
        add_kpi_card(slide, "Lift", f"{lift_val:+.2f}pp",
                     sentiment=lift_sentiment,
                     left=x_offset, top=kpi_row_top,
                     width=kpi_w, height=kpi_h)

        # Significance badge
        sig_sentiment = "positive" if cd["sig"] != "ns" else "negative"
        add_kpi_card(slide, "Sig.", cd["sig"],
                     sentiment=sig_sentiment,
                     left=x_offset + kpi_w + kpi_gap_x, top=kpi_row_top,
                     width=kpi_w, height=kpi_h)

        # Revenue card
        rev_sentiment = "positive" if cd["revenue"] not in ("N/A",) else None
        add_kpi_card(slide, "NIBT", cd["revenue"],
                     sentiment=rev_sentiment,
                     left=x_offset + (kpi_w + kpi_gap_x) * 2, top=kpi_row_top,
                     width=kpi_w, height=kpi_h)

        # Insight bullet
        add_text_box(slide, f"\u2022 {cd['insight']}",
                     x_offset, Inches(4.25),
                     col_width, Inches(0.4),
                     font_size=Pt(9))

        # Client count
        add_text_box(slide, f"N = {cd['clients']:,}",
                     x_offset, Inches(4.6),
                     col_width, Inches(0.25),
                     font_size=Pt(8), font_color=GRAY)

        # Star highlight for VAW
        if is_star:
            star_bar = slide.shapes.add_shape(
                MSO_SHAPE.RECTANGLE,
                x_offset, Inches(4.9), col_width, Inches(0.03)
            )
            star_bar.fill.solid()
            star_bar.fill.fore_color.rgb = TUNDRA
            star_bar.line.fill.background()

    return slide


# ── Slide 4: Recommendations ────────────────────────────────────

def build_recommendations_slide(prs):
    """Slide 4: Recommendations and next investigations."""
    slide = add_blank_slide(prs)

    # Header bar
    header = slide.shapes.add_shape(
        MSO_SHAPE.RECTANGLE,
        Inches(0), Inches(0), SLIDE_WIDTH, Inches(0.7)
    )
    header.fill.solid()
    header.fill.fore_color.rgb = DARK_BLUE
    header.line.fill.background()

    add_text_box(slide, "Recommendations & Next Investigations",
                 Inches(0.3), Inches(0.08), Inches(10), Inches(0.45),
                 font_size=Pt(22), font_color=WHITE, bold=True)

    # ── Left column: Campaign Actions ──
    left_col_x = Inches(0.5)
    col_w = Inches(5.8)

    add_text_box(slide, "Campaign Actions",
                 left_col_x, Inches(0.9), col_w, Inches(0.35),
                 font_size=Pt(16), bold=True)

    actions = [
        ("VCN: Continue", TUNDRA, "Positive lift, high volume"),
        ("VDA: Continue", TUNDRA, "Strongest relative lift"),
        ("VDT: Continue \u2014 monitor shrinking TAM", WARM_YELLOW, "95% auto-activate"),
        ("VUI: Optimize \u2014 verify config changes", WARM_YELLOW, "Not significant"),
        ("VUT: Discontinue \u2014 redirect budget to VAW", SUNBURST, "Zero lift"),
        ("VAW: Expand \u2014 scale with VUT budget", TUNDRA, "Strongest absolute campaign"),
    ]

    y = Inches(1.35)
    action_row_h = Inches(0.6)
    for label, color, sublabel in actions:
        # Color indicator bar
        indicator = slide.shapes.add_shape(
            MSO_SHAPE.RECTANGLE,
            left_col_x, y, Inches(0.15), action_row_h - Inches(0.08)
        )
        indicator.fill.solid()
        indicator.fill.fore_color.rgb = color
        indicator.line.fill.background()

        add_text_box(slide, label,
                     left_col_x + Inches(0.25), y,
                     col_w - Inches(0.25), Inches(0.3),
                     font_size=Pt(12), bold=True)

        add_text_box(slide, sublabel,
                     left_col_x + Inches(0.25), y + Inches(0.28),
                     col_w - Inches(0.25), Inches(0.25),
                     font_size=Pt(9), font_color=GRAY)

        y += action_row_h

    # ── Right column: Next Investigations ──
    right_col_x = Inches(7.0)
    right_col_w = Inches(5.8)

    add_text_box(slide, "Next Investigations",
                 right_col_x, Inches(0.9), right_col_w, Inches(0.35),
                 font_size=Pt(16), bold=True)

    investigations = [
        "SRM root cause on VCN/VDA \u2014 randomization investigation",
        "VUT/VAW attribution overlap \u2014 same goal, opposite results",
        "VUI configuration status \u2014 were Oct 2025 recommendations implemented?",
        "Revenue methodology \u2014 $78.21 applicability beyond acquisition",
        "Card type segmentation \u2014 which cards need manual activation?",
    ]

    y = Inches(1.35)
    inv_row_h = Inches(0.55)
    for i, text in enumerate(investigations, 1):
        # Number badge
        badge = slide.shapes.add_shape(
            MSO_SHAPE.OVAL,
            right_col_x, y + Inches(0.03), Inches(0.3), Inches(0.3)
        )
        badge.fill.solid()
        badge.fill.fore_color.rgb = OCEAN
        badge.line.fill.background()
        tf = badge.text_frame
        tf.word_wrap = False
        p = tf.paragraphs[0]
        p.text = str(i)
        p.font.size = Pt(11)
        p.font.bold = True
        p.font.name = FONT_FAMILY
        p.font.color.rgb = WHITE
        p.alignment = PP_ALIGN.CENTER

        add_text_box(slide, text,
                     right_col_x + Inches(0.4), y,
                     right_col_w - Inches(0.4), Inches(0.45),
                     font_size=Pt(11))

        y += inv_row_h

    # ── Bottom: Revenue summary bar ──
    rev_bar_top = Inches(5.5)
    rev_bar = slide.shapes.add_shape(
        MSO_SHAPE.RECTANGLE,
        Inches(0.5), rev_bar_top, Inches(12.3), Inches(0.7)
    )
    rev_bar.fill.solid()
    rev_bar.fill.fore_color.rgb = COOL_WHITE
    rev_bar.line.color.rgb = OCEAN
    rev_bar.line.width = Pt(1.5)

    add_text_box(slide,
                 "Conservative: $499K (VCN+VDA)   |   With Activation: $979K (+VDT)   |   Maximum: $1.83M (+VAW*)",
                 Inches(0.8), rev_bar_top + Inches(0.08),
                 Inches(11.8), Inches(0.35),
                 font_size=Pt(14), bold=True, align=PP_ALIGN.CENTER)

    add_text_box(slide,
                 "* $78.21 per incremental client. VAW uses acquisition proxy \u2014 not validated for provisioning.",
                 Inches(0.8), rev_bar_top + Inches(0.4),
                 Inches(11.8), Inches(0.25),
                 font_size=Pt(8), font_color=GRAY, align=PP_ALIGN.CENTER)

    return slide


# ── Helper ───────────────────────────────────────────────────────

def _style_table_cell(cell, font_size=Pt(9), bold=False,
                      font_color=None, fill_color=None):
    """Apply styling to a table cell."""
    from pptx.enum.text import MSO_ANCHOR
    cell.vertical_anchor = MSO_ANCHOR.MIDDLE
    for paragraph in cell.text_frame.paragraphs:
        paragraph.font.size = font_size
        paragraph.font.name = FONT_FAMILY
        paragraph.font.bold = bold
        paragraph.alignment = PP_ALIGN.CENTER
        if font_color:
            paragraph.font.color.rgb = font_color
    if fill_color:
        cell.fill.solid()
        cell.fill.fore_color.rgb = fill_color


# ── Main ─────────────────────────────────────────────────────────

def build_deck(track="b", output_name=None):
    """Build the complete 4-slide deck."""
    print(f"Building deck (Track {'A' if track == 'a' else 'B'})...")

    vintage, summary = load_data()
    prs = new_presentation()

    # Slide 1: Campaign Overview
    build_overview_slide(prs)

    # Slides 2-3: Campaign groups
    for group_key in ["top_funnel", "bottom_funnel"]:
        build_campaign_slide(prs, vintage, summary, group_key, track=track)

    # Slide 4: Recommendations
    build_recommendations_slide(prs)

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
