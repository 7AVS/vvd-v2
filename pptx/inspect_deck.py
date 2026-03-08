"""
Inspect a generated .pptx — shows every shape's position, size, content,
and flags overlaps. Use this to iterate on layout without opening PowerPoint.

Usage:
    python inspect_deck.py VVD_v2_Report_TrackB.pptx
    python inspect_deck.py VVD_v2_Report_TrackB.pptx --slide 2
"""

import sys
from pptx import Presentation
from pptx.util import Inches, Emu


def emu_to_inches(emu):
    return round(emu / 914400, 2) if emu else 0


def get_shape_info(shape):
    """Extract shape metadata."""
    info = {
        "name": shape.name,
        "type": "shape",
        "left": emu_to_inches(shape.left),
        "top": emu_to_inches(shape.top),
        "width": emu_to_inches(shape.width),
        "height": emu_to_inches(shape.height),
        "right": emu_to_inches(shape.left + shape.width),
        "bottom": emu_to_inches(shape.top + shape.height),
        "text": "",
    }

    if shape.has_chart:
        info["type"] = "CHART"
        chart = shape.chart
        if chart.has_title:
            info["text"] = chart.chart_title.text_frame.text[:60]
        info["series_count"] = len(chart.series)
        info["categories"] = len(chart.plots[0].categories) if chart.plots else 0
    elif shape.has_table:
        info["type"] = "TABLE"
        table = shape.table
        info["text"] = f"{len(table.rows)}x{len(table.columns)}"
        # First row header
        headers = [table.cell(0, c).text for c in range(len(table.columns))]
        info["text"] += f" | {' | '.join(headers)}"
    elif hasattr(shape, "text") and shape.text:
        info["type"] = "TEXT"
        info["text"] = shape.text[:80].replace("\n", " | ")
    elif shape.shape_type == 13:  # PICTURE
        info["type"] = "IMAGE"
        info["text"] = shape.image.content_type if hasattr(shape, "image") else ""

    return info


def check_overlaps(shapes_info):
    """Find shapes that overlap each other."""
    overlaps = []
    for i, a in enumerate(shapes_info):
        for j, b in enumerate(shapes_info):
            if j <= i:
                continue
            # Skip background rectangles (full width)
            if a["width"] > 12 or b["width"] > 12:
                continue
            # Check bounding box overlap
            if (a["left"] < b["right"] and a["right"] > b["left"] and
                    a["top"] < b["bottom"] and a["bottom"] > b["top"]):
                overlaps.append((a, b))
    return overlaps


def check_out_of_bounds(shapes_info, slide_w=13.33, slide_h=7.5):
    """Find shapes extending beyond slide boundaries."""
    oob = []
    for s in shapes_info:
        if s["right"] > slide_w + 0.1:
            oob.append((s, f"extends RIGHT by {s['right'] - slide_w:.2f}in"))
        if s["bottom"] > slide_h + 0.1:
            oob.append((s, f"extends BELOW by {s['bottom'] - slide_h:.2f}in"))
    return oob


def inspect(pptx_path, slide_num=None):
    """Main inspection."""
    prs = Presentation(pptx_path)
    print(f"File: {pptx_path}")
    print(f"Slide size: {emu_to_inches(prs.slide_width)}in x {emu_to_inches(prs.slide_height)}in")
    print(f"Slides: {len(prs.slides)}")
    print("=" * 90)

    for i, slide in enumerate(prs.slides, 1):
        if slide_num and i != slide_num:
            continue

        print(f"\n{'='*90}")
        print(f"SLIDE {i}")
        print(f"{'='*90}")

        shapes_info = []
        for shape in slide.shapes:
            info = get_shape_info(shape)
            shapes_info.append(info)

        # Print shape map
        print(f"\n{'Type':<8} {'Left':>5} {'Top':>5} {'Width':>6} {'Height':>6} {'Right':>6} {'Bottom':>6}  Content")
        print("-" * 90)
        for s in shapes_info:
            print(f"{s['type']:<8} {s['left']:>5} {s['top']:>5} {s['width']:>6} {s['height']:>6} "
                  f"{s['right']:>6} {s['bottom']:>6}  {s['text'][:50]}")

        # Check overlaps
        overlaps = check_overlaps(shapes_info)
        if overlaps:
            print(f"\n  OVERLAPS DETECTED ({len(overlaps)}):")
            for a, b in overlaps[:5]:  # Show first 5
                print(f"    - '{a['text'][:30]}' overlaps '{b['text'][:30]}'")

        # Check out of bounds
        oob = check_out_of_bounds(shapes_info)
        if oob:
            print(f"\n  OUT OF BOUNDS ({len(oob)}):")
            for s, msg in oob:
                print(f"    - '{s['text'][:30]}' {msg}")

        # Visual grid (simplified ASCII)
        print(f"\n  Layout map (1 char = ~0.5in):")
        grid_w, grid_h = 27, 15  # 13.5in x 7.5in at 0.5in per char
        grid = [["." for _ in range(grid_w)] for _ in range(grid_h)]

        markers = {"CHART": "C", "TABLE": "T", "IMAGE": "I", "TEXT": "t", "shape": "#"}
        for s in shapes_info:
            marker = markers.get(s["type"], "?")
            x1 = max(0, int(s["left"] / 0.5))
            y1 = max(0, int(s["top"] / 0.5))
            x2 = min(grid_w - 1, int(s["right"] / 0.5))
            y2 = min(grid_h - 1, int(s["bottom"] / 0.5))
            for y in range(y1, y2 + 1):
                for x in range(x1, x2 + 1):
                    grid[y][x] = marker

        print("  +" + "-" * grid_w + "+")
        for row in grid:
            print("  |" + "".join(row) + "|")
        print("  +" + "-" * grid_w + "+")
        print("  Legend: C=chart T=table I=image t=text #=shape")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "VVD_v2_Report_TrackB.pptx"
    slide = int(sys.argv[2]) if "--slide" in sys.argv and len(sys.argv) > 3 else None
    if "--slide" in sys.argv:
        idx = sys.argv.index("--slide")
        slide = int(sys.argv[idx + 1]) if idx + 1 < len(sys.argv) else None
    inspect(path, slide)
