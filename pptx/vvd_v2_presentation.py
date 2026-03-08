#!/usr/bin/env python
# -*- coding: utf-8 -*-

# ====================================================================
# VVD v2 Presentation Charts
#
# Publication-quality Plotly charts for the VVD campaign measurement
# spotlight (~10 min) within the Payment LOB Power Pack meeting.
#
# Data sources: local CSV files in pptx/data/
# Environment: Local (pandas + Plotly) or Lumina Jupyter
#
# Structure: Each section is one notebook cell.
# CELL 1: Setup + data loading
# CELL 2: Campaign overview (funnel positioning)
# CELL 3: Portfolio lift overview
# CELL 4-9: Per-campaign vintage curves (VCN, VDA, VDT, VUI, VUT, VAW)
# CELL 10: Recommendations summary
# ====================================================================


# ============================================================
# CELL 1: SETUP + DATA LOADING
# ============================================================

import os
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

try:
    from IPython.display import display, HTML
    IN_NOTEBOOK = True
except ImportError:
    IN_NOTEBOOK = False

# Colors (RBC brand)
C = {
    'dark_blue':   '#003168',
    'bright_blue': '#0051A5',
    'ocean':       '#0091DA',
    'warm_yellow': '#FFC72C',
    'tundra':      '#07AFBF',
    'sunburst':    '#FCA311',
    'gray':        '#9EA2A2',
    'cool_white':  '#E7EEF1',
    'white':       '#FFFFFF',
    'black':       '#333333',
    'light_gray':  '#F0F0F0',
}

ACTION_COLOR = C['bright_blue']
CONTROL_COLOR = C['gray']
SIG_COLOR = C['tundra']
NOSIG_COLOR = '#D0D0D0'
WARN_COLOR = C['sunburst']

FONT = 'Segoe UI'
W, H = 1050, 520

CAMPAIGN_META = {
    'VCN': {'name': 'Contextual Notification', 'goal': 'Card Acquisition',
            'channel': 'MB', 'split': '95/5', 'funnel': 'Acquisition',
            'cadence': 'Monthly batch'},
    'VDA': {'name': 'Seasonal Acquisition', 'goal': 'Card Acquisition',
            'channel': 'IM+EM', 'split': '95/5', 'funnel': 'Acquisition',
            'cadence': 'Seasonal (~2x/yr)'},
    'VDT': {'name': 'Activation Trigger', 'goal': 'Card Activation',
            'channel': 'EM', 'split': '90/10', 'funnel': 'Activation',
            'cadence': 'Triggered (7d+14d)'},
    'VUI': {'name': 'Usage Trigger', 'goal': 'Card Usage',
            'channel': 'EM+IM', 'split': '95/5', 'funnel': 'Usage',
            'cadence': 'Triggered'},
    'VUT': {'name': 'Tokenization Usage', 'goal': 'Wallet Provisioning',
            'channel': 'EM', 'split': '95/5', 'funnel': 'Provisioning',
            'cadence': 'Triggered'},
    'VAW': {'name': 'Add To Wallet', 'goal': 'Wallet Provisioning',
            'channel': 'IM', 'split': '80/20', 'funnel': 'Provisioning',
            'cadence': 'Seasonal'},
}

FUNNEL_ORDER = ['VCN', 'VDA', 'VDT', 'VUI', 'VUT', 'VAW']

# Resolve data path relative to this file
BASE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(BASE, 'data')

summary_df = pd.read_csv(os.path.join(DATA, 'campaign_summary.csv'))
vintage_df = pd.read_csv(os.path.join(DATA, 'vintage_curves.csv'))
srm_df = pd.read_csv(os.path.join(DATA, 'srm_check.csv'))

action_df = summary_df[summary_df['TST_GRP_CD'] == 'TG4'].set_index('MNE')
control_df = summary_df[summary_df['TST_GRP_CD'] == 'TG7'].set_index('MNE')


def show(fig):
    if IN_NOTEBOOK:
        display(HTML(fig.to_html(include_plotlyjs='cdn')))
    else:
        fig.show()


def base_layout(**overrides):
    layout = dict(
        template='plotly_white',
        font=dict(family=FONT, size=13, color=C['black']),
        width=W, height=H,
        margin=dict(l=60, r=120, t=80, b=90),
        legend=dict(x=0.98, y=0.98, xanchor='right', yanchor='top',
                    bgcolor='rgba(255,255,255,0.85)',
                    bordercolor='#ddd', borderwidth=1, font=dict(size=11)),
    )
    layout.update(overrides)
    return layout


print(f"Loaded {len(summary_df)} summary rows, {len(vintage_df)} vintage rows, {len(srm_df)} SRM rows")


# ============================================================
# CELL 2: CAMPAIGN OVERVIEW (Slide 1)
# ============================================================

funnel_stages = ['Acquisition', 'Activation', 'Usage', 'Provisioning']
funnel_colors = [C['dark_blue'], C['bright_blue'], C['ocean'], C['tundra']]
funnel_map = {s: c for s, c in zip(funnel_stages, funnel_colors)}

# Build data for the overview chart
rows = []
for mne in FUNNEL_ORDER:
    m = CAMPAIGN_META[mne]
    a = action_df.loc[mne]
    c = control_df.loc[mne]
    volume = int(a['total_clients']) + int(c['total_clients'])
    rows.append({
        'mne': mne, 'name': m['name'], 'funnel': m['funnel'],
        'channel': m['channel'], 'split': m['split'],
        'volume': volume, 'cadence': m['cadence'],
    })
overview = pd.DataFrame(rows)

fig2 = go.Figure()

# Horizontal bars for volume, colored by funnel stage
for stage in funnel_stages:
    mask = overview['funnel'] == stage
    subset = overview[mask]
    if subset.empty:
        continue
    labels = [f"{r['mne']} — {r['name']}" for _, r in subset.iterrows()]
    fig2.add_trace(go.Bar(
        y=labels,
        x=subset['volume'],
        orientation='h',
        name=stage,
        marker_color=funnel_map[stage],
        text=[f"{v:,.0f}" for v in subset['volume']],
        textposition='outside',
        textfont=dict(size=11),
    ))

# Annotations: channel, split, cadence below each bar label
for i, r in overview.iterrows():
    label = f"{r['mne']} — {r['name']}"
    fig2.add_annotation(
        x=0, y=label,
        text=f"{r['channel']} | {r['split']} | {r['cadence']}",
        showarrow=False, xanchor='left',
        font=dict(size=9, color=C['gray']),
        yshift=-16,
    )

fig2.update_layout(
    **base_layout(
        title=dict(
            text='VVD Campaign Portfolio — 6 campaigns across the cardholder lifecycle',
            font=dict(size=18, color=C['dark_blue']),
        ),
        height=460,
        margin=dict(l=60, r=40, t=100, b=80),
        xaxis=dict(title='Total Clients (Action + Control)', showgrid=True,
                   gridcolor=C['light_gray'],
                   range=[0, 2_500_000]),
        yaxis=dict(autorange='reversed', tickfont=dict(size=12)),
        barmode='stack',
        showlegend=True,
        legend=dict(x=0.98, y=0.02, xanchor='right', yanchor='bottom',
                    bgcolor='rgba(255,255,255,0.85)',
                    bordercolor='#ddd', borderwidth=1, font=dict(size=10)),
    )
)
fig2.add_annotation(
    text='Funnel: Acquisition → Activation → Usage → Provisioning',
    xref='paper', yref='paper', x=0.5, y=-0.22,
    showarrow=False, font=dict(size=11, color=C['gray']),
)

show(fig2)


# ============================================================
# CELL 3: PORTFOLIO LIFT OVERVIEW
# ============================================================

LIFT_ORDER = ['VDT', 'VAW', 'VUI', 'VDA', 'VCN', 'VUT']

lifts = []
for mne in LIFT_ORDER:
    a = action_df.loc[mne]
    sig = a['p_value'] < 0.05
    lifts.append({
        'mne': mne,
        'name': CAMPAIGN_META[mne]['name'],
        'lift': a['lift'],
        'sig': sig,
        'p': a['p_value'],
    })
lift_df = pd.DataFrame(lifts)

revenue_map = {
    'VCN': ('281K', 280852),
    'VDA': ('218K', 218359),
    'VDT': ('480K', 480033),
    'VAW': ('846K*', 845903),
}

fig3 = go.Figure()

colors = [SIG_COLOR if r['sig'] else NOSIG_COLOR for _, r in lift_df.iterrows()]
labels = [f"{r['mne']} — {r['name']}" for _, r in lift_df.iterrows()]

fig3.add_trace(go.Bar(
    y=labels,
    x=lift_df['lift'],
    orientation='h',
    marker_color=colors,
    text=[f"+{v:.2f}pp" if v > 0 else f"{v:.2f}pp" for v in lift_df['lift']],
    textposition='outside',
    textfont=dict(size=12, color=C['black']),
))

# Significance stars and revenue labels
for i, r in lift_df.iterrows():
    label = f"{r['mne']} — {r['name']}"
    if r['sig']:
        stars = '***' if r['p'] < 0.001 else ('**' if r['p'] < 0.01 else '*')
        fig3.add_annotation(
            x=r['lift'], y=label,
            text=f"  {stars}",
            showarrow=False, xanchor='left',
            font=dict(size=14, color=SIG_COLOR, family=FONT),
            xshift=50,
        )
    if r['mne'] in revenue_map:
        rev_label, _ = revenue_map[r['mne']]
        fig3.add_annotation(
            x=r['lift'], y=label,
            text=f"  {rev_label}",
            showarrow=False, xanchor='left',
            font=dict(size=11, color=C['dark_blue'], family=FONT),
            xshift=85,
        )

fig3.update_layout(
    **base_layout(
        title=dict(
            text='4 of 6 campaigns show statistically significant positive lift',
            font=dict(size=18, color=C['dark_blue']),
        ),
        height=460,
        margin=dict(l=60, r=40, t=100, b=100),
        xaxis=dict(title='Absolute Lift (percentage points)', showgrid=True,
                   gridcolor=C['light_gray'], zeroline=True,
                   zerolinecolor=C['gray'], zerolinewidth=1,
                   range=[-0.2, 6.5]),
        yaxis=dict(tickfont=dict(size=12)),
        showlegend=False,
    )
)
fig3.add_annotation(
    text='Conservative NIBT: 499K (VCN+VDA)  |  With activation: 979K  |  Extended: 1.83M*',
    xref='paper', yref='paper', x=0.5, y=-0.18,
    showarrow=False,
    font=dict(size=12, color=C['dark_blue'], family=FONT),
)
fig3.add_annotation(
    text='* VAW revenue uses acquisition proxy (78.21/client) — not validated for provisioning',
    xref='paper', yref='paper', x=0.5, y=-0.26,
    showarrow=False,
    font=dict(size=10, color=C['gray'], family=FONT),
)

show(fig3)


# ============================================================
# CELL 4: VCN VINTAGE CURVES
# ============================================================

def plot_vintage(mne, title, subtitle, metric='PRIMARY',
                 bold_action=False, muted=False):
    """Plot vintage curves for a campaign. One line per cohort per group."""
    df = vintage_df[(vintage_df['MNE'] == mne) & (vintage_df['METRIC'] == metric)]
    if df.empty:
        print(f"No vintage data for {mne}/{metric}")
        return None

    cohorts = sorted(df['COHORT'].unique())
    max_day = int(df['DAY'].max())

    # Cohort color palette (blue shades for action, gray shades for control)
    n = max(len(cohorts), 1)
    action_shades = [
        f'rgba(0,81,165,{0.3 + 0.7 * i / max(n - 1, 1)})' for i in range(n)
    ]
    control_shades = [
        f'rgba(158,162,162,{0.3 + 0.7 * i / max(n - 1, 1)})' for i in range(n)
    ]

    if muted:
        # Muted but still distinguishable: action = desaturated blue, control = light gray
        action_shades = [
            f'rgba(120,140,160,{0.35 + 0.5 * i / max(n - 1, 1)})' for i in range(n)
        ]
        control_shades = [
            f'rgba(190,195,200,{0.3 + 0.5 * i / max(n - 1, 1)})' for i in range(n)
        ]

    fig = go.Figure()

    # Track final rates for gap annotation
    action_finals = []
    control_finals = []

    for ci, cohort in enumerate(cohorts):
        for grp, grp_name, shades, dash_style, lw in [
            ('TG4', 'Action', action_shades, 'solid', 2.5 if bold_action else 2),
            ('TG7', 'Control', control_shades, 'dash', 1.5),
        ]:
            cdf = df[(df['COHORT'] == cohort) & (df['TST_GRP_CD'] == grp)].sort_values('DAY')
            if cdf.empty:
                continue

            # Cumulative rate (SUCCESS_CNT is already cumulative per day)
            fig.add_trace(go.Scatter(
                x=cdf['DAY'], y=cdf['RATE'],
                mode='lines',
                name=f'{grp_name} {cohort}',
                line=dict(color=shades[ci], width=lw, dash=dash_style),
                legendgroup=grp_name,
                showlegend=(ci == 0),
                hovertemplate=f'{grp_name} {cohort}<br>Day %{{x}}<br>Rate: %{{y:.3f}}%<extra></extra>',
            ))

            final_rate = cdf.iloc[-1]['RATE']
            if grp == 'TG4':
                action_finals.append(final_rate)
            else:
                control_finals.append(final_rate)

    # Gap annotation — place inside the chart area, left of the final day
    if action_finals and control_finals:
        avg_action = sum(action_finals) / len(action_finals)
        avg_control = sum(control_finals) / len(control_finals)
        gap = avg_action - avg_control
        gap_sign = '+' if gap >= 0 else ''

        # Position annotation at ~80% of max_day so it doesn't clip
        ann_x = max_day * 0.75
        ann_y = (avg_action + avg_control) / 2
        fig.add_annotation(
            x=ann_x, y=ann_y,
            text=f'<b>{gap_sign}{gap:.2f}pp gap</b>',
            showarrow=True, arrowhead=2, arrowcolor=C['dark_blue'],
            ax=-40, ay=-30,
            font=dict(size=12, color=C['dark_blue'], family=FONT),
            bgcolor='rgba(255,255,255,0.8)',
            bordercolor=C['dark_blue'], borderwidth=1, borderpad=4,
        )

    fig.update_layout(
        **base_layout(
            title=dict(
                text=title,
                font=dict(size=16, color=C['dark_blue']),
            ),
            xaxis=dict(title=f'Days since treatment start (0-{max_day})',
                       showgrid=True, gridcolor=C['light_gray'],
                       range=[0, max_day + 5]),
            yaxis=dict(title='Cumulative success rate (%)',
                       showgrid=True, gridcolor=C['light_gray']),
        )
    )
    fig.add_annotation(
        text=subtitle,
        xref='paper', yref='paper', x=0.5, y=-0.16,
        showarrow=False, font=dict(size=10, color=C['gray'], family=FONT),
    )

    return fig


fig4 = plot_vintage(
    'VCN',
    title='VCN: Contextual Notification drives +0.18pp lift (p<0.001) — 281K NIBT',
    subtitle='N=1,999,540 | Channel: MB | Split: 95/5 | SRM detected (94.7/5.3 vs 95/5)',
)
if fig4:
    show(fig4)


# ============================================================
# CELL 5: VDA VINTAGE CURVES
# ============================================================

fig5 = plot_vintage(
    'VDA',
    title='VDA: Seasonal Acquisition delivers +0.31pp lift (p<0.001) — 218K NIBT',
    subtitle='N=894,779 | Channel: IM+EM | Split: 95/5 | SRM detected (92/8 vs 95/5)',
)
if fig5:
    show(fig5)


# ============================================================
# CELL 6: VDT VINTAGE CURVES
# ============================================================

fig6 = plot_vintage(
    'VDT',
    title='VDT: Activation Trigger shows strongest lift at +4.65pp (p<0.001) — 480K NIBT',
    subtitle='N=132,015 | Channel: EM | Split: 90/10 | 95% of cards auto-activate — shrinking TAM',
)
if fig6:
    show(fig6)


# ============================================================
# CELL 7: VUI VINTAGE CURVES
# ============================================================

fig7 = plot_vintage(
    'VUI',
    title='VUI: Usage Trigger shows +0.78pp lift — NOT statistically significant',
    subtitle='N=158,895 | Channel: EM+IM | Split: 95/5 | Config change recommended Oct 2025 — status unknown',
    muted=True,
)
if fig7:
    show(fig7)


# ============================================================
# CELL 8: VUT VINTAGE CURVES
# ============================================================

fig8 = plot_vintage(
    'VUT',
    title='VUT: Tokenization shows zero lift (+0.02pp, p=0.91) — recommend discontinuation',
    subtitle='N=784,849 | Channel: EM | Split: 95/5 | Same goal as VAW but opposite results',
    muted=True,
)
if fig8:
    show(fig8)


# ============================================================
# CELL 9: VAW VINTAGE CURVES
# ============================================================

fig9 = plot_vintage(
    'VAW',
    title='VAW: Add To Wallet delivers strongest campaign lift at +2.62pp (p<0.001) — 846K* NIBT',
    subtitle='N=412,568 | Channel: IM | Split: 80/20 | *NIBT uses acquisition proxy, not validated for provisioning',
    bold_action=True,
)
if fig9:
    show(fig9)


# ============================================================
# CELL 10: RECOMMENDATIONS SUMMARY
# ============================================================

rec_rows = [
    ('VCN', 'Continue', 'Contextual Notification',
     '+0.18pp (p<0.001)', '$281K',
     'Strong lift, clean revenue. SRM needs investigation.'),
    ('VDA', 'Continue', 'Seasonal Acquisition',
     '+0.31pp (p<0.001)', '$218K',
     'Strong lift, seasonal efficiency. SRM caveat.'),
    ('VDT', 'Continue', 'Activation Trigger',
     '+4.65pp (p<0.001)', '$480K',
     'Strongest lift. Monitor shrinking TAM (95% auto-activate).'),
    ('VUI', 'Optimize', 'Usage Trigger',
     '+0.78pp (n.s.)', 'N/A',
     'Not significant. Were Oct 2025 config changes implemented?'),
    ('VUT', 'Discontinue', 'Tokenization Usage',
     '+0.02pp (n.s.)', 'N/A',
     'Zero lift. Redirect budget to VAW.'),
    ('VAW', 'Expand', 'Add To Wallet',
     '+2.62pp (p<0.001)', '$846K*',
     'Best ROI. Scale with VUT budget.'),
]

action_colors = ['#2E7D32', '#2E7D32', '#2E7D32',
                 C['sunburst'], '#C62828', C['tundra']]

header_color = C['dark_blue']
row_colors = ['#F8F9FA', C['cool_white']] * 3

fig10 = go.Figure(data=[go.Table(
    columnwidth=[80, 100, 160, 130, 70, 300],
    header=dict(
        values=['<b>Campaign</b>', '<b>Action</b>', '<b>Name</b>',
                '<b>Lift</b>', '<b>NIBT</b>', '<b>Key Finding</b>'],
        fill_color=header_color,
        font=dict(color='white', size=13, family=FONT),
        align='left',
        height=38,
    ),
    cells=dict(
        values=[
            [r[0] for r in rec_rows],
            [r[1] for r in rec_rows],
            [r[2] for r in rec_rows],
            [r[3] for r in rec_rows],
            [r[4] for r in rec_rows],
            [r[5] for r in rec_rows],
        ],
        fill_color=[row_colors[:len(rec_rows)]] * 6,
        font=dict(
            size=12, family=FONT,
            color=[
                [C['dark_blue']] * len(rec_rows),
                action_colors,
                [C['black']] * len(rec_rows),
                [C['black']] * len(rec_rows),
                [C['dark_blue']] * len(rec_rows),
                [C['black']] * len(rec_rows),
            ],
        ),
        align='left',
        height=34,
    ),
)])

fig10.update_layout(
    **base_layout(
        title=dict(
            text='Recommendations & Next Investigations',
            font=dict(size=18, color=C['dark_blue']),
        ),
        height=420,
        margin=dict(l=20, r=20, t=80, b=130),
    )
)

# Revenue summary (no dollar signs to avoid MathJax rendering)
fig10.add_annotation(
    text=('Revenue Summary:  '
          'Conservative 499K (VCN+VDA)  |  '
          'With VDT: 979K  |  '
          'With VAW: 1.83M*'),
    xref='paper', yref='paper', x=0.0, y=-0.22,
    showarrow=False, xanchor='left',
    font=dict(size=12, color=C['dark_blue'], family=FONT),
)

# Next investigations
fig10.add_annotation(
    text=('<b>Next Investigations:</b>  '
          'SRM root cause  ·  '
          'VUI config status  ·  '
          'VUT/VAW attribution overlap  ·  '
          'Revenue methodology validation  ·  '
          'Card type segmentation'),
    xref='paper', yref='paper', x=0.0, y=-0.34,
    showarrow=False, xanchor='left',
    font=dict(size=11, color=C['gray'], family=FONT),
)

show(fig10)


# ============================================================
# CELL 11: EXPORT HELPER (optional)
# ============================================================

# Export all figures to standalone HTML for sharing or embedding in PPTX.
# Requires: kaleido for PNG, or just use HTML.

FIGURES = {
    'slide1_overview': fig2,
    'slide2_lift': fig3,
    'slide3_vcn': fig4,
    'slide4_vda': fig5,
    'slide5_vdt': fig6,
    'slide6_vui': fig7,
    'slide7_vut': fig8,
    'slide8_vaw': fig9,
    'slide9_recs': fig10,
}

out_dir = os.path.join(BASE, 'data', 'charts')
os.makedirs(out_dir, exist_ok=True)

for name, fig in FIGURES.items():
    if fig is None:
        continue
    html_path = os.path.join(out_dir, f'{name}.html')
    fig.write_html(html_path, include_plotlyjs='cdn')
    try:
        png_path = os.path.join(out_dir, f'{name}.png')
        fig.write_image(png_path, width=W, height=H, scale=2)
    except Exception:
        pass

print(f"Exported {len([f for f in FIGURES.values() if f])} charts to {out_dir}")
