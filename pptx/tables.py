"""
Native PowerPoint tables — editable, styled with brand colors.
"""

from pptx.util import Inches, Pt, Emu
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.dml.color import RGBColor

from theme import (
    DARK_BLUE, WHITE, COOL_WHITE, TUNDRA, SUNBURST,
    FONT_FAMILY, FONT_SMALL, FONT_BODY, FONT_KPI, FONT_KPI_LABEL,
    ACTION_COLOR, CONTROL_COLOR, HEADER_COLOR, BG_COLOR,
)


def _style_cell(cell, font_size=FONT_SMALL, bold=False,
                font_color=None, fill_color=None, align=PP_ALIGN.CENTER):
    """Apply consistent styling to a table cell."""
    cell.vertical_anchor = MSO_ANCHOR.MIDDLE

    for paragraph in cell.text_frame.paragraphs:
        paragraph.font.size = font_size
        paragraph.font.name = FONT_FAMILY
        paragraph.font.bold = bold
        paragraph.alignment = align
        if font_color:
            paragraph.font.color.rgb = font_color

    if fill_color:
        cell.fill.solid()
        cell.fill.fore_color.rgb = fill_color


def add_summary_table(slide, summary_data, mnes=None,
                      left=Inches(0.3), top=Inches(1.5),
                      width=Inches(12.5)):
    """
    Add a campaign summary table.
    Columns: Campaign | Action Rate | Control Rate | Lift | p-value | Significance
    """
    if mnes is None:
        mnes = ["VCN", "VDA", "VDT", "VUI", "VUT", "VAW"]

    action_rows = {r["MNE"]: r for r in summary_data
                   if r["TST_GRP_CD"].strip() == "TG4" and r["MNE"] in mnes}
    control_rows = {r["MNE"]: r for r in summary_data
                    if r["TST_GRP_CD"].strip() == "TG7" and r["MNE"] in mnes}

    present_mnes = [m for m in mnes if m in action_rows]
    if not present_mnes:
        return None

    headers = ["Campaign", "Clients (Action)", "Action Rate", "Control Rate",
               "Lift (pp)", "p-value", "Sig."]
    n_rows = len(present_mnes) + 1
    n_cols = len(headers)

    row_height = Inches(0.35)
    table_height = row_height * n_rows

    table_shape = slide.shapes.add_table(n_rows, n_cols, left, top, width, table_height)
    table = table_shape.table

    # Header row
    for j, header in enumerate(headers):
        cell = table.cell(0, j)
        cell.text = header
        _style_cell(cell, font_size=FONT_SMALL, bold=True,
                    font_color=WHITE, fill_color=DARK_BLUE)

    # Data rows
    for i, mne in enumerate(present_mnes, start=1):
        a = action_rows[mne]
        c = control_rows.get(mne, {})

        lift = float(a.get("lift", 0))
        p_val = float(a.get("p_value", 1))
        sig = "***" if p_val < 0.001 else "**" if p_val < 0.01 else "*" if p_val < 0.05 else "ns"

        row_data = [
            mne,
            f"{int(a['total_clients']):,}",
            f"{float(a['success_rate']):.2f}%",
            f"{float(c.get('success_rate', 0)):.2f}%" if c else "—",
            f"{lift:+.2f}",
            f"{p_val:.4f}" if p_val >= 0.0001 else "<0.0001",
            sig,
        ]

        row_bg = COOL_WHITE if i % 2 == 0 else WHITE
        lift_color = TUNDRA if lift > 0 else SUNBURST if lift < 0 else None

        for j, val in enumerate(row_data):
            cell = table.cell(i, j)
            cell.text = val

            fc = None
            if j == 4 and lift_color:  # Lift column
                fc = lift_color
            _style_cell(cell, font_size=FONT_SMALL, fill_color=row_bg,
                        font_color=fc if fc else RGBColor(0x33, 0x33, 0x33))

    return table_shape


def add_kpi_card(slide, label, value, sublabel=None, sentiment=None,
                 left=Inches(0.3), top=Inches(0.3),
                 width=Inches(2), height=Inches(1.2)):
    """
    Add a KPI card (colored box with big number + label).
    sentiment: "positive" | "negative" | None
    """
    from pptx.enum.shapes import MSO_SHAPE

    # Background box
    shape = slide.shapes.add_shape(
        MSO_SHAPE.ROUNDED_RECTANGLE, left, top, width, height
    )
    shape.fill.solid()
    shape.fill.fore_color.rgb = COOL_WHITE

    # Border by sentiment
    if sentiment == "positive":
        shape.line.color.rgb = TUNDRA
        shape.line.width = Pt(2)
    elif sentiment == "negative":
        shape.line.color.rgb = SUNBURST
        shape.line.width = Pt(2)
    else:
        shape.line.color.rgb = RGBColor(0xDD, 0xDD, 0xDD)
        shape.line.width = Pt(1)

    # Value text (big number)
    tf = shape.text_frame
    tf.word_wrap = True
    tf.auto_size = None

    p = tf.paragraphs[0]
    p.text = str(value)
    p.font.size = FONT_KPI
    p.font.bold = True
    p.font.name = FONT_FAMILY
    p.alignment = PP_ALIGN.CENTER

    if sentiment == "positive":
        p.font.color.rgb = TUNDRA
    elif sentiment == "negative":
        p.font.color.rgb = SUNBURST
    else:
        p.font.color.rgb = DARK_BLUE

    # Label below value
    p2 = tf.add_paragraph()
    p2.text = label
    p2.font.size = FONT_KPI_LABEL
    p2.font.name = FONT_FAMILY
    p2.font.color.rgb = RGBColor(0x66, 0x66, 0x66)
    p2.alignment = PP_ALIGN.CENTER

    if sublabel:
        p3 = tf.add_paragraph()
        p3.text = sublabel
        p3.font.size = Pt(8)
        p3.font.name = FONT_FAMILY
        p3.font.color.rgb = RGBColor(0x99, 0x99, 0x99)
        p3.alignment = PP_ALIGN.CENTER

    return shape


def add_text_box(slide, text, left, top, width, height,
                 font_size=FONT_BODY, font_color=None, bold=False,
                 align=PP_ALIGN.LEFT):
    """Add a simple text box."""
    textbox = slide.shapes.add_textbox(left, top, width, height)
    tf = textbox.text_frame
    tf.word_wrap = True
    p = tf.paragraphs[0]
    p.text = text
    p.font.size = font_size
    p.font.name = FONT_FAMILY
    p.font.bold = bold
    p.font.color.rgb = font_color or DARK_BLUE
    p.alignment = align
    return textbox


def add_bullet_list(slide, items, left, top, width, height,
                    font_size=FONT_SMALL):
    """Add a bulleted text list."""
    textbox = slide.shapes.add_textbox(left, top, width, height)
    tf = textbox.text_frame
    tf.word_wrap = True

    for i, item in enumerate(items):
        if i == 0:
            p = tf.paragraphs[0]
        else:
            p = tf.add_paragraph()
        p.text = item
        p.font.size = font_size
        p.font.name = FONT_FAMILY
        p.font.color.rgb = RGBColor(0x33, 0x33, 0x33)
        p.level = 0

    return textbox
