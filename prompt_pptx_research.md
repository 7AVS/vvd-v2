# Research Prompt: Automating PowerPoint from Data Analysis Output

I'm a data analyst working on campaign measurement for 6 marketing campaigns (card acquisition, activation, usage, wallet provisioning). My analysis runs in PySpark on a remote cluster and produces:

- Vintage curves (cumulative success rate over time — line charts, Action vs Control)
- Lift & significance tables (per campaign, per cohort)
- Bar charts (spend comparison, frequency distribution)
- Summary statistics tables
- Spend velocity curves (cumulative dollars over time)

The volume is large: 6 campaigns × multiple metrics × Action vs Control × quarterly cohorts = potentially hundreds of charts and tables.

**My problem**: I need to produce a polished, professional PowerPoint deck. Manual chart-by-chart formatting is not feasible at this scale. I've tried python-pptx before and the output looked too "generated" — not presentation-ready.

**What I need you to research**:

1. **python-pptx capabilities in 2025-2026** — Has it improved? Can it produce native editable PowerPoint charts (not just embedded images)? How good are styled line charts with multiple series? Best GitHub examples?

2. **Plotly → PowerPoint workflow** — Exporting Plotly charts as high-res PNG/SVG, then placing them on slides with python-pptx. What resolution/DPI works? Does it look professional? Can I define a Plotly theme once and apply it to all charts?

3. **Template-based approach** — Create a .potx template with master slides, color scheme, fonts. Then python-pptx fills in the data. How well does this work? Limitations?

4. **GitHub projects and frameworks** — Any purpose-built tools for data-to-deck pipelines? DataFrame → PowerPoint? Campaign analytics → slides? Look for actively maintained projects with real usage.

5. **Commercial/paid tools** — Think-Cell automation, Beautiful.ai API, Gamma.app, Canva API, Microsoft Graph API for PowerPoint. Any tool that takes structured data + template → polished deck?

6. **AI-powered solutions** — Any LLM-based tools that generate slide decks from data? MCP servers for PowerPoint? Tools that understand data context and create appropriate visualizations?

7. **Alternative pipelines** — Google Slides API → export to .pptx? Quarto presentations? Marp (Markdown → slides)? Any approach that produces .pptx as output?

8. **What do analysts actually use?** — Reddit, community forums, real-world experiences. Not just documentation but practical advice from people who've solved this.

**Evaluation criteria for each solution**:
- Does the output look genuinely professional (not "script-generated")?
- Can I edit/tweak individual charts after generation?
- How much setup effort vs ongoing time saved?
- Is it reliable and maintained?
- Can it handle 100+ charts with consistent formatting?

**My constraints**:
- Analysis runs on a remote Linux cluster (PySpark/Jupyter). PowerPoint generation can happen locally (Windows).
- I can export data as CSV/Parquet and charts as PNG from the cluster.
- Final output must be .pptx (PowerPoint is the stakeholder standard).
- I have Python skills but don't want to spend weeks building a framework.

Give me a structured comparison with a clear recommendation for the most practical approach.
