"""
VVD v2 PowerPoint Theme
Brand colors, fonts, chart styling constants.
Sourced from Vintage/Vvd RBC_COLOR_SCHEME.md + VISUAL_FRAMEWORK docs.
"""

from pptx.dml.color import RGBColor
from pptx.util import Pt, Inches, Emu

# ── RBC Brand Colors ──────────────────────────────────────────────
DARK_BLUE    = RGBColor(0x00, 0x31, 0x68)
BRIGHT_BLUE  = RGBColor(0x00, 0x51, 0xA5)
OCEAN        = RGBColor(0x00, 0x91, 0xDA)
WARM_YELLOW  = RGBColor(0xFF, 0xC7, 0x2C)
TUNDRA       = RGBColor(0x07, 0xAF, 0xBF)
SUNBURST     = RGBColor(0xFC, 0xA3, 0x11)
GRAY         = RGBColor(0x9E, 0xA2, 0xA2)
COOL_WHITE   = RGBColor(0xE7, 0xEE, 0xF1)
WHITE        = RGBColor(0xFF, 0xFF, 0xFF)
BLACK        = RGBColor(0x33, 0x33, 0x33)
LIGHT_GRAY   = RGBColor(0xF0, 0xF0, 0xF0)

# ── Chart Color Sequence (for multi-series) ──────────────────────
CHART_COLORS = [BRIGHT_BLUE, WARM_YELLOW, OCEAN, TUNDRA, SUNBURST, GRAY]

# ── Semantic Colors ──────────────────────────────────────────────
ACTION_COLOR  = OCEAN         # solid line
CONTROL_COLOR = GRAY          # dashed line
POSITIVE_LIFT = TUNDRA        # KPI card border
NEGATIVE_LIFT = SUNBURST      # KPI card border
HEADER_COLOR  = DARK_BLUE     # titles, headers
BG_COLOR      = COOL_WHITE    # card/panel backgrounds

# ── Font ─────────────────────────────────────────────────────────
FONT_FAMILY = "Segoe UI"
FONT_TITLE  = Pt(24)
FONT_SUBTITLE = Pt(14)
FONT_BODY   = Pt(11)
FONT_SMALL  = Pt(9)
FONT_KPI    = Pt(28)
FONT_KPI_LABEL = Pt(9)

# ── Slide Dimensions (16:9 widescreen) ──────────────────────────
SLIDE_WIDTH  = Inches(13.333)
SLIDE_HEIGHT = Inches(7.5)

# ── Plotly Theme (for Track A) ───────────────────────────────────
# Hex versions for Plotly (which uses strings, not RGBColor)
HEX = {
    "dark_blue":   "#003168",
    "bright_blue": "#0051A5",
    "ocean":       "#0091DA",
    "warm_yellow": "#FFC72C",
    "tundra":      "#07AFBF",
    "sunburst":    "#FCA311",
    "gray":        "#9EA2A2",
    "cool_white":  "#E7EEF1",
    "black":       "#333333",
    "white":       "#FFFFFF",
}

PLOTLY_COLORWAY = [
    HEX["bright_blue"], HEX["warm_yellow"], HEX["ocean"],
    HEX["tundra"], HEX["sunburst"], HEX["gray"],
]

def get_plotly_template():
    """Return a Plotly template with VVD brand styling."""
    import plotly.graph_objects as go

    return go.layout.Template(
        layout=go.Layout(
            font=dict(family="Segoe UI", size=14, color=HEX["black"]),
            title=dict(font=dict(size=18, color=HEX["dark_blue"])),
            colorway=PLOTLY_COLORWAY,
            plot_bgcolor=HEX["white"],
            paper_bgcolor=HEX["white"],
            xaxis=dict(
                gridcolor="#f0f0f0",
                linecolor="#ddd",
                showgrid=True,
                zeroline=False,
            ),
            yaxis=dict(
                gridcolor="#f0f0f0",
                linecolor="#ddd",
                showgrid=True,
                zeroline=False,
            ),
            legend=dict(
                bgcolor="rgba(255,255,255,0.8)",
                bordercolor="#ddd",
                borderwidth=1,
            ),
        )
    )


# ── Campaign Metadata ────────────────────────────────────────────
CAMPAIGNS = {
    "VCN": {"name": "Contextual Notification", "goal": "Card Acquisition",    "group": "top_funnel"},
    "VDA": {"name": "Seasonal Acquisition",    "goal": "Card Acquisition",    "group": "top_funnel"},
    "VDT": {"name": "Activation Trigger",      "goal": "Card Activation",     "group": "top_funnel"},
    "VUI": {"name": "Usage Trigger",            "goal": "Card Usage",          "group": "bottom_funnel"},
    "VUT": {"name": "Tokenization Usage",       "goal": "Wallet Provisioning", "group": "bottom_funnel"},
    "VAW": {"name": "Add To Wallet",            "goal": "Wallet Provisioning", "group": "bottom_funnel"},
}

# Slide groupings — 3 per slide for the funnel narrative
SLIDE_GROUPS = {
    "top_funnel":    {"title": "Acquisition & Activation",    "mnes": ["VCN", "VDA", "VDT"]},
    "bottom_funnel": {"title": "Usage & Wallet Provisioning", "mnes": ["VUI", "VUT", "VAW"]},
}
