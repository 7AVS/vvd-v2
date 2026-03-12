#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Build VVD v2 PowerPoint deck from pre-rendered chart PNGs."""

import os
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN

BASE = os.path.dirname(os.path.abspath(__file__))
CHARTS_DIR = os.path.join(BASE, 'data', 'charts')
OUT_PATH = os.path.join(BASE, 'VVD_v2_Report_v4.pptx')

SLIDES = [
    'slide1_overview.png',
    'slide2_lift.png',
    'slide3_vcn.png',
    'slide4_vda.png',
    'slide5_vdt.png',
    'slide6_vui.png',
    'slide7_vut.png',
    'slide8_vaw.png',
    'slide9_recs.png',
]

SLIDE_W = Inches(13.333)
SLIDE_H = Inches(7.5)

IMG_LEFT = Inches(0.3)
IMG_TOP = Inches(0.3)
IMG_W = Inches(12.7)
IMG_H = Inches(6.6)

GRAY = RGBColor(0x9E, 0xA2, 0xA2)
HEADER_TEXT = 'Campaign: Virtual Visa Debit \u2014 VVD'


def build():
    prs = Presentation()
    prs.slide_width = SLIDE_W
    prs.slide_height = SLIDE_H

    blank_layout = prs.slide_layouts[6]  # blank

    for idx, filename in enumerate(SLIDES, start=1):
        img_path = os.path.join(CHARTS_DIR, filename)
        if not os.path.exists(img_path):
            print(f"SKIP: {filename} not found")
            continue

        slide = prs.slides.add_slide(blank_layout)

        # Chart image — centered, filling most of the slide
        slide.shapes.add_picture(img_path, IMG_LEFT, IMG_TOP, IMG_W, IMG_H)

        # Header text top-left
        txbox = slide.shapes.add_textbox(Inches(0.3), Inches(0.05), Inches(5), Inches(0.3))
        tf = txbox.text_frame
        tf.word_wrap = False
        p = tf.paragraphs[0]
        p.text = HEADER_TEXT
        p.font.size = Pt(10)
        p.font.color.rgb = GRAY
        p.font.name = 'Segoe UI'

        # Page number bottom-right
        txbox2 = slide.shapes.add_textbox(
            SLIDE_W - Inches(0.8), SLIDE_H - Inches(0.4),
            Inches(0.6), Inches(0.3),
        )
        tf2 = txbox2.text_frame
        tf2.word_wrap = False
        p2 = tf2.paragraphs[0]
        p2.text = f'| {idx}'
        p2.font.size = Pt(10)
        p2.font.color.rgb = GRAY
        p2.font.name = 'Segoe UI'
        p2.alignment = PP_ALIGN.RIGHT

    prs.save(OUT_PATH)
    print(f"Saved {len(prs.slides)} slides to {OUT_PATH}")


if __name__ == '__main__':
    build()
