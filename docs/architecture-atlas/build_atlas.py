#!/usr/bin/env python3
"""Render atlas.json as an offline, accessible HTML/SVG learning atlas.

Content and edge routes come exclusively from the JSON model. No dependencies.
Usage: python3 build_atlas.py [--input atlas.json] [--output index.html]
"""

from __future__ import annotations

import argparse
import html
import json
import math
import re
import sys
from pathlib import Path


HERE = Path(__file__).resolve().parent
PALETTE = {
    "sql": ("#1754AD", "#EEF5FF", "#8DB6ED"),
    "branch": ("#6845C0", "#F4EFFF", "#BC9FE7"),
    "wal": ("#925100", "#FFF4DE", "#DDB660"),
    "check": ("#16665A", "#EAF7F2", "#81BAAA"),
    "lifecycle": ("#994957", "#FFF0F2", "#D8A0AA"),
    "neutral": ("#455366", "#F1F4F7", "#B4BFCC"),
}


def esc(value):
    return html.escape(str(value), quote=True)


def token(value):
    return re.sub(r"[^A-Za-z0-9_-]", "-", str(value))


def tone(value):
    if value not in PALETTE:
        raise ValueError(f"Unknown tone {value!r}; expected one of {', '.join(PALETTE)}")
    return value


def measure(text, size, bold=False):
    """Conservative width estimate; browser audit supplies actual glyph bounds."""
    widths = []
    for char in str(text):
        if char in " ilI.,:;'`!|":
            widths.append(0.30)
        elif char in "mwMW@%&":
            widths.append(0.86)
        elif char.isupper():
            widths.append(0.67)
        elif ord(char) > 0x2500:
            widths.append(0.96)
        else:
            widths.append(0.55)
    return sum(widths) * size * (1.035 if bold else 1.01)


def wrap(text, width, size, bold=False):
    result = []
    for paragraph in str(text).split("\n"):
        line = ""
        for word in paragraph.split():
            proposal = f"{line} {word}".strip()
            if line and measure(proposal, size, bold) > width:
                result.append(line)
                line = word
            else:
                line = proposal
        result.append(line)
    return result


def source_links(sources):
    items = []
    for source in sources:
        path = str(source["file"])
        line = int(source.get("line", 1))
        href = "../../" + path + f"#L{line}"
        label = str(source.get("label", ""))
        items.append(
            f'<li><a href="{esc(href)}" target="_blank" rel="noopener">'
            f'<span class="source-label">{esc(label)}</span>'
            f'<code>{esc(path)}:{line}</code><span aria-hidden="true">↗</span></a></li>'
        )
    return '<ul class="source-list">' + "".join(items) + "</ul>" if items else ""


def text_lines(lines, x, y, size, line_height, css_class, **attrs):
    extras = " ".join(f'{key.replace("_", "-")}="{esc(value)}"' for key, value in attrs.items())
    spans = "".join(
        f'<tspan x="{x:g}" y="{y + i * line_height:g}">{esc(line)}</tspan>'
        for i, line in enumerate(lines)
    )
    return f'<text class="{css_class}" font-size="{size:g}" {extras}>{spans}</text>'


class Renderer:
    def __init__(self, model):
        self.model = model
        self.issues = []
        self.notes = []
        self.nodes = {}

    def issue(self, where, message):
        self.issues.append(f"{where}: {message}")

    def svg(self, diagram, prefix, title):
        width, height = float(diagram["width"]), float(diagram["height"])
        if width <= 0 or height <= 0:
            raise ValueError(f"{prefix}: diagram width and height must be positive")
        nodes = diagram.get("nodes", [])
        ids = {str(node["id"]) for node in nodes}
        if len(ids) != len(nodes):
            raise ValueError(f"{prefix}: duplicate node id")
        out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width:g} {height:g}" '
               f'width="{width:g}" height="{height:g}" role="group" aria-labelledby="{prefix}-title" '
               f'class="architecture-svg" data-diagram="{prefix}">'
               f'<title id="{prefix}-title">{esc(title)}</title>']
        out.append('<defs><style>'
                   '.architecture-svg{font-family:Inter,ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}'
                   '.node-title{font-weight:650;fill:#17263B}.node-line{fill:#37465B}'
                   '.lane-label{font-weight:700;letter-spacing:1.1px}.edge-label{font-weight:550;fill:#43536A}'
                   '.diagram-node{cursor:pointer}.diagram-node:focus{outline:none}'
                   '.diagram-node:focus .node-box,.diagram-node[data-selected="true"] .node-box{stroke:#142B49;stroke-width:2.5}'
                   '</style>')
        for name, (ink, _, _) in PALETTE.items():
            out.append(f'<marker id="{prefix}-arrow-{name}" markerWidth="9" markerHeight="9" '
                       f'refX="7.5" refY="4.5" orient="auto" markerUnits="userSpaceOnUse">'
                       f'<path d="M1 1 L7.5 4.5 L1 8" fill="none" stroke="{ink}" '
                       'stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>')
        out.append('</defs><rect width="100%" height="100%" fill="#fff"/>')

        for lane in diagram.get("lanes", []):
            t = tone(lane.get("tone", "neutral"))
            ink, fill, border = PALETTE[t]
            x, y, w, h = (float(lane[k]) for k in ("x", "y", "w", "h"))
            out.append(f'<g class="diagram-lane"><rect x="{x:g}" y="{y:g}" width="{w:g}" height="{h:g}" '
                       f'rx="13" fill="{fill}" fill-opacity=".43" stroke="{border}" stroke-opacity=".48"/>'
                       f'<text class="lane-label" x="{x+17:g}" y="{y+25:g}" font-size="11" fill="{ink}">'
                       f'{esc(lane["label"])}</text></g>')
            if measure(lane["label"], 11, True) + 34 > w:
                self.issue(prefix, f"lane label too wide: {lane['label']}")

        # Explicit routes only: rendering never infers or changes architecture edges.
        for index, edge in enumerate(diagram.get("edges", [])):
            for endpoint in ("from", "to"):
                if str(edge[endpoint]) not in ids:
                    raise ValueError(f"{prefix}: edge {index} {endpoint} references missing node {edge[endpoint]!r}")
            points = edge["points"]
            if len(points) < 2:
                raise ValueError(f"{prefix}: edge {index} requires two or more points")
            for point in points:
                if len(point) != 2 or any(not math.isfinite(float(v)) for v in point):
                    raise ValueError(f"{prefix}: edge {index} has an invalid point")
                if not (0 <= point[0] <= width and 0 <= point[1] <= height):
                    self.issue(prefix, f"edge {index} point {point} exceeds diagram bounds")
            t = tone(edge.get("tone", "neutral"))
            ink = PALETTE[t][0]
            route = " ".join(f'{float(x):g},{float(y):g}' for x, y in points)
            dashed = ' stroke-dasharray="5 5"' if edge.get("dashed") else ""
            out.append(f'<polyline class="diagram-edge" data-edge-index="{index}" '
                       f'data-from="{esc(str(edge["from"]))}" data-to="{esc(str(edge["to"]))}" '
                       f'points="{route}" fill="none" stroke="{ink}" '
                       f'stroke-width="1.55" stroke-linecap="round" stroke-linejoin="round"{dashed} '
                       f'marker-end="url(#{prefix}-arrow-{t})"/>')
            if edge.get("label"):
                lx, ly = float(edge["label_x"]), float(edge["label_y"])
                label_lines = str(edge["label"]).split("\n")
                lw = max(measure(line, 11.5) for line in label_lines) + 12
                lh = 15 * len(label_lines) + 4
                out.append(f'<g class="edge-label-group" data-edge-index="{index}"><rect x="{lx-lw/2:g}" y="{ly-12:g}" '
                           f'width="{lw:g}" height="{lh:g}" rx="4" fill="#fff" fill-opacity=".96"/>')
                out.append(text_lines(label_lines, lx, ly, 11.5, 15, "edge-label", text_anchor="middle"))
                out.append('</g>')

        for node in nodes:
            node_id = f'{prefix}-{token(node["id"])}'
            self.nodes[node_id] = {"title": node["title"], "detail": node.get("detail", []),
                                   "sources_html": source_links(node.get("sources", [])),
                                   "tone": node.get("tone", "neutral")}
            x, y, w, h = (float(node[k]) for k in ("x", "y", "w", "h"))
            if x < 0 or y < 0 or x + w > width or y + h > height:
                self.issue(prefix, f"node {node['id']} exceeds diagram bounds")
            t = tone(node.get("tone", "neutral"))
            ink, fill, border = PALETTE[t]
            fits = False
            for scale in (1.0, .96, .92, .88):
                title_size, body_size = 16 * scale, 13 * scale
                title_lh, body_lh = 20 * scale, 18 * scale
                title_rows = wrap(node["title"], w-32, title_size, True)
                body_rows = []
                for line in node.get("lines", []):
                    body_rows.extend(wrap(line, w-32, body_size))
                content_height = len(title_rows)*title_lh + (7 if body_rows else 0) + len(body_rows)*body_lh
                if content_height <= h-28 and all(measure(s, title_size, True) <= w-32 for s in title_rows) and all(measure(s, body_size) <= w-32 for s in body_rows):
                    fits = True
                    break
            if not fits:
                self.issue(prefix, f"node {node['id']} text requires {content_height+28:.0f}px height, has {h:g}px; or a word exceeds its width")
            elif scale < 1:
                self.notes.append(f"{prefix}/{node['id']}: font scale {scale:.2f} to fit {w:g}×{h:g}")
            top = y + max(16, (h-content_height)/2) + title_size
            out.append(f'<g id="{node_id}" class="diagram-node tone-{t}" role="button" tabindex="0" '
                       f'aria-label="{esc(node["title"])}: open explanation" data-node="{node_id}" data-model-id="{esc(str(node["id"]))}" '
                       f'data-bounds="{x:g},{y:g},{w:g},{h:g}">'
                       f'<rect class="node-box" x="{x:g}" y="{y:g}" width="{w:g}" height="{h:g}" '
                       f'rx="10" fill="{fill}" stroke="{border}" stroke-width="1.2"/>'
                       f'<rect x="{x:g}" y="{y+14:g}" width="3" height="{h-28:g}" rx="1.5" fill="{ink}"/>')
            out.append(text_lines(title_rows, x+16, top, title_size, title_lh, "node-title"))
            if body_rows:
                body_y = top + (len(title_rows)-1)*title_lh + 7 + body_lh
                out.append(text_lines(body_rows, x+16, body_y, body_size, body_lh, "node-line"))
            out.append('</g>')

        for annotation in diagram.get("texts", []):
            t = tone(annotation.get("tone", "neutral"))
            size = float(annotation.get("size", 12))
            rows = str(annotation["text"]).split("\n")
            out.append(text_lines(rows, float(annotation["x"]), float(annotation["y"]), size, size*1.35,
                                  "diagram-annotation", fill=PALETTE[t][0],
                                  font_weight=annotation.get("weight", 400), text_anchor=annotation.get("anchor", "start")))
        out.append('</svg>')
        return "".join(out)

    def section(self, section):
        section_id = token(section["id"])
        number = str(section["number"]).zfill(2)
        figures = [(None, section["diagram"], "", section["title"])]
        for variant in section.get("variants", []):
            figures.append((variant["label"], variant["diagram"], variant.get("description", ""), variant["label"]))
        parts = [f'<section class="atlas-section{" has-table" if section.get("table") else ""}" id="{section_id}" aria-labelledby="heading-{section_id}">'
                 f'<header class="section-heading"><span class="section-number">{esc(number)}</span>'
                 f'<div><h2 id="heading-{section_id}">{esc(section["title"])}</h2>'
                 f'<p class="section-subtitle">{esc(section["subtitle"])}</p></div></header>']
        parts.append('<div class="diagram-shell"><div class="diagram-toolbar">')
        if len(figures) > 1:
            parts.append(f'<div class="variant-tabs" role="tablist" aria-label="{esc(section["title"])} states">')
            for i, (label, _, _, _) in enumerate(figures):
                label = label or "Overview"
                parts.append(f'<button role="tab" id="{section_id}-tab-{i}" aria-controls="{section_id}-figure-{i}" '
                             f'aria-selected="{str(i == 0).lower()}" tabindex="{0 if i == 0 else -1}" '
                             f'data-variant="{i}">{esc(label)}</button>')
            parts.append('</div>')
        else:
            parts.append('<span class="diagram-hint"><span class="hint-dot"></span>Select a node to explore</span>')
        parts.append('<button class="export-svg quiet-button" type="button" aria-label="Download displayed diagram as SVG">'
                     '<span aria-hidden="true">↓</span> SVG</button></div>')
        for i, (label, diagram, description, diagram_title) in enumerate(figures):
            parts.append(f'<figure class="diagram-figure{" is-active" if i == 0 else ""}" '
                         f'id="{section_id}-figure-{i}" data-variant-index="{i}" '
                         f'aria-label="{esc(diagram_title)}">')
            if label:
                parts.append(f'<figcaption><strong>{esc(label)}</strong>'
                             f'{"<span>" + esc(description) + "</span>" if description else ""}</figcaption>')
            parts.append('<div class="svg-viewport" tabindex="0" aria-label="Scrollable architecture diagram">')
            parts.append(self.svg(diagram, f'{section_id}-v{i}', diagram_title))
            parts.append('</div></figure>')
        parts.append('</div>')
        parts.append('<aside class="node-detail" aria-live="polite" aria-atomic="true">'
                     '<div class="detail-placeholder"><span aria-hidden="true">↳</span>'
                     ' Select any diagram node for its explanation and source evidence. Keyboard: Tab, then Enter.</div></aside>')
        if section.get("cards"):
            parts.append('<div class="cards">')
            for card in section["cards"]:
                t = tone(card.get("tone", "neutral"))
                parts.append(f'<article class="concept-card tone-{t}"><h3>{esc(card["title"])}</h3>'
                             f'<p>{esc(card["text"])}</p></article>')
            parts.append('</div>')
        if section.get("table"):
            table = section["table"]
            parts.append('<div class="table-scroll"><table><thead><tr>')
            parts.extend(f'<th scope="col">{esc(h)}</th>' for h in table["headers"])
            parts.append('</tr></thead><tbody>')
            for row in table["rows"]:
                if len(row) != len(table["headers"]):
                    raise ValueError(f"{section_id}: table row has a different cell count from its headers")
                parts.append('<tr>' + ''.join(f'<td>{esc(cell)}</td>' for cell in row) + '</tr>')
            parts.append('</tbody></table></div>')
        if section.get("sources"):
            parts.append(f'<details class="evidence"><summary>Implementation evidence '
                         f'<span>{len(section["sources"])} references</span></summary>'
                         + source_links(section["sources"]) + '</details>')
        print_sources = list(section.get("sources", []))
        known_sources = {(s["file"], s.get("line", 1)) for s in print_sources}
        for _, diagram, _, _ in figures:
            for node in diagram.get("nodes", []):
                for source in node.get("sources", []):
                    key = (source["file"], source.get("line", 1))
                    if key not in known_sources:
                        print_sources.append(source)
                        known_sources.add(key)
        if print_sources:
            parts.append('<div class="print-sources"><strong>Source evidence</strong>' + source_links(print_sources) + '</div>')
        # Node explanations are included in print, not just hidden in browser interactions.
        parts.append('<div class="print-node-notes"><h3>Node explanations</h3><div class="print-note-grid">')
        seen = set()
        for _, diagram, _, _ in figures:
            for node in diagram.get("nodes", []):
                signature = json.dumps([node["title"], node.get("detail", []), node.get("sources", [])], sort_keys=True)
                if signature in seen or not node.get("detail"):
                    continue
                seen.add(signature)
                parts.append(f'<article><h4>{esc(node["title"])}</h4>')
                parts.extend(f'<p>{esc(paragraph)}</p>' for paragraph in node.get("detail", []))
                parts.append(source_links(node.get("sources", [])) + '</article>')
        parts.append('</div></div></section>')
        return ''.join(parts)

    def render(self):
        model = self.model
        ids = [token(section["id"]) for section in model["sections"]]
        if len(set(ids)) != len(ids):
            raise ValueError("Section IDs must be unique")
        sections = ''.join(self.section(section) for section in model["sections"])
        outline = ''.join(f'<a href="#{token(s["id"])}"><span>{esc(str(s["number"]).zfill(2))}</span>'
                          f'<span>{esc(s["title"])}</span></a>' for s in model["sections"])
        legend = ''.join(f'<span class="legend-item tone-{tone(item["key"])}"><i></i>{esc(item["label"])}</span>'
                         for item in model.get("legend", []))
        glossary = ''.join(f'<div><dt>{esc(item["term"])}</dt><dd>{esc(item["meaning"])}</dd></div>'
                           for item in model.get("glossary", []))
        scope = ''.join(f'<li>{esc(item)}</li>' for item in model.get("scope", []))
        data = json.dumps(self.nodes, ensure_ascii=False).replace('</', '<\\/')
        intro = model.get("intro", "")
        intro_html = ''.join(f'<p>{esc(p)}</p>' for p in intro) if isinstance(intro, list) else f'<p>{esc(intro)}</p>'
        warnings = ''
        if self.issues:
            warnings = '<div class="build-warning"><strong>Layout validation needs attention</strong><ul>' + ''.join(f'<li>{esc(x)}</li>' for x in self.issues) + '</ul></div>'
        return f'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light"><title>{esc(model["title"])}</title><style>{CSS}</style></head>
<body data-print-mode="visual"><a class="skip-link" href="#main">Skip to diagrams</a>
<div class="app-layout"><aside class="sidebar"><a class="brand" href="#top"><span class="brand-symbol" aria-hidden="true">F</span><span>FERRODB<small>Architecture atlas</small></span></a>
<div class="nav-caption">READING PATH</div><nav class="outline" aria-label="Atlas outline">{outline}<a href="#glossary"><span>⌕</span><span>Glossary & scope</span></a></nav>
<div class="sidebar-footer"><span class="offline-dot"></span>Self-contained · offline<br><code>{esc(str(model["revision"])[:12])}</code></div></aside>
<main id="main"><header class="hero" id="top"><div class="hero-topline"><span class="eyebrow">A GUIDE TO THE IMPLEMENTATION</span><div class="print-actions"><select id="print-mode" aria-label="PDF content"><option value="visual">Visual guide</option><option value="full">With explanations</option></select><button type="button" class="print-button" id="print-button">Print / PDF <span aria-hidden="true">↗</span></button></div></div>
<h1>{esc(model["title"])}</h1><div class="intro">{intro_html}</div><div class="metadata"><span>Revision <code>{esc(str(model["revision"])[:12])}</code></span><span>{esc(model["date"])}</span></div><div class="legend" aria-label="Subsystem colors">{legend}</div></header>
{warnings}{sections}<section class="glossary-section" id="glossary"><div class="section-heading"><span class="section-number">A–Z</span><div><h2>Glossary & scope</h2><p class="section-subtitle">Keep the mechanisms distinct.</p></div></div><dl class="glossary">{glossary}</dl><div class="scope"><h3>Scope of this atlas</h3><ul>{scope}</ul></div></section>
<footer class="page-footer"><span>{esc(model["title"])}</span><span>{esc(model["revision"])} · {esc(model["date"])}</span></footer></main></div>
<script id="atlas-node-data" type="application/json">{data}</script><script>{JS}</script></body></html>'''


CSS = r'''
:root{--ink:#17263b;--muted:#617086;--line:#dfe5ed;--page:#f6f8fb;--sql:#1754ad;--branch:#6845c0;--wal:#925100;--check:#16665a;--lifecycle:#994957;--neutral:#455366}
*{box-sizing:border-box}html{scroll-behavior:smooth;scroll-padding-top:24px}body{margin:0;color:var(--ink);background:var(--page);font:14px/1.55 Inter,ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;-webkit-font-smoothing:antialiased}button,a{-webkit-tap-highlight-color:transparent}button{font:inherit;cursor:pointer}a{color:inherit}button:focus-visible,a:focus-visible,summary:focus-visible,.svg-viewport:focus-visible{outline:3px solid #82abe9;outline-offset:4px}h1,h2,h3,h4,p,figure{margin:0}button{border:0}.skip-link{position:fixed;left:20px;top:-90px;padding:12px 18px;background:#fff;z-index:30}.skip-link:focus{top:15px}.app-layout{max-width:1800px;margin:auto;display:grid;grid-template-columns:250px minmax(0,1fr)}.sidebar{position:sticky;top:0;height:100vh;padding:35px 22px 24px 25px;border-right:1px solid var(--line);display:flex;flex-direction:column;background:#f9fafc}.brand{text-decoration:none;display:flex;align-items:center;gap:12px;font-size:13px;letter-spacing:1.6px;font-weight:780}.brand small{display:block;letter-spacing:0;font-size:11px;font-weight:450;color:var(--muted);margin-top:2px}.brand-symbol{display:grid;place-items:center;width:35px;height:40px;border:1px solid #bac8d9;border-radius:8px;color:#213e60;background:#fff;font-size:23px;letter-spacing:-2px}.nav-caption{font-size:9px;letter-spacing:1.9px;font-weight:750;color:#8792a3;margin:48px 10px 13px}.outline{display:flex;flex-direction:column;gap:5px;overflow:auto}.outline a{display:grid;grid-template-columns:23px 1fr;gap:9px;padding:10px 10px;border-radius:7px;text-decoration:none;color:#5b687a;font-size:12px;line-height:1.4;transition:background .15s}.outline a>span:first-child{font:10px/1.8 ui-monospace,SFMono-Regular,Menlo,monospace;color:#8994a3}.outline a:hover{background:#edf1f6}.outline a[aria-current="location"]{background:#e7edf6;color:#213f68;font-weight:650}.outline a[aria-current="location"]>span:first-child{color:#31588b}.sidebar-footer{margin-top:auto;padding:25px 10px 0;color:#8993a2;font-size:10px;line-height:1.9}.sidebar-footer code{font-size:10px}.offline-dot{display:inline-block;width:5px;height:5px;border-radius:50%;background:#579880;margin-right:6px;vertical-align:2px}main{min-width:0;padding:44px 44px 24px;max-width:1500px}.hero{padding:0 2px 32px;border-bottom:1px solid var(--line);margin-bottom:42px}.hero-topline{display:flex;justify-content:space-between;align-items:center;gap:15px;margin-bottom:20px}.eyebrow{font-size:10px;letter-spacing:2px;color:#6d7e93;font-weight:700}.print-button{border:1px solid #ccd6e2;border-radius:7px;background:#fff;color:#40516a;padding:8px 12px;font-size:11px;white-space:nowrap}.print-button span{margin-left:14px}h1{font-size:clamp(29px,3.1vw,43px);font-weight:720;letter-spacing:-1.6px;line-height:1.12;max-width:1000px}.intro{max-width:890px;font-size:14px;line-height:1.75;color:#53637a;margin-top:19px}.intro p+p{margin-top:7px}.metadata{display:flex;gap:20px;margin-top:19px;font-size:10px;color:#8390a1}.metadata code{font-size:10px;color:#62758f;margin-left:3px}.legend{display:flex;flex-wrap:wrap;gap:10px 23px;margin-top:27px}.legend-item{display:inline-flex;align-items:center;gap:7px;font-size:10px;color:#5d6b7e}.legend-item i{width:8px;height:8px;border-radius:2px;background:var(--tone)}.tone-sql{--tone:var(--sql)}.tone-branch{--tone:var(--branch)}.tone-wal{--tone:var(--wal)}.tone-check{--tone:var(--check)}.tone-lifecycle{--tone:var(--lifecycle)}.tone-neutral{--tone:var(--neutral)}.atlas-section{margin-bottom:56px;scroll-margin-top:24px}.section-heading{display:flex;gap:15px;align-items:flex-start;margin-bottom:20px}.section-number{font:11px/1.1 ui-monospace,SFMono-Regular,Menlo,monospace;color:#7b8ba0;background:#e9edf3;border:1px solid #dde4ee;border-radius:5px;padding:8px 7px;min-width:31px;text-align:center;margin-top:4px}h2{font-size:25px;letter-spacing:-.7px;line-height:1.3;font-weight:680}.section-subtitle{font-size:12px;line-height:1.6;color:#6a7890;max-width:920px;margin-top:6px}.diagram-shell{border:1px solid #d8e0eb;border-radius:11px;background:white;box-shadow:0 4px 15px #1f3d5e04;overflow:hidden}.diagram-toolbar{min-height:42px;border-bottom:1px solid #ecf0f5;padding:7px 14px;display:flex;align-items:center;justify-content:space-between;gap:15px;background:#fdfefe}.diagram-hint{font-size:10px;letter-spacing:.25px;color:#8793a4;display:flex;align-items:center;gap:7px}.hint-dot{width:5px;height:5px;border-radius:50%;background:#9baabe}.quiet-button{background:transparent;font-size:10px;color:#718199;padding:3px 5px;white-space:nowrap}.quiet-button:hover{color:#183e70}.quiet-button span{font-size:14px;margin-right:3px}.svg-viewport{overflow-x:auto;overflow-y:hidden;width:100%;padding:9px 3px}.architecture-svg{display:block;width:100%;height:auto;min-width:760px;max-width:none}.diagram-figure{display:none}.diagram-figure.is-active{display:block}.diagram-figure figcaption{padding:14px 20px 0;font-size:11px;color:#60728a;display:flex;gap:10px;align-items:baseline}.diagram-figure figcaption strong{color:#374b66;font-weight:650}.variant-tabs{display:flex;flex-wrap:wrap;gap:4px}.variant-tabs button{background:transparent;border-radius:5px;font-size:10px;color:#6f7d90;padding:6px 10px}.variant-tabs button[aria-selected="true"]{background:#eaf0f7;color:#294e7b;font-weight:650}.node-detail{margin:10px 0 17px;border:1px solid #e2e8f0;border-radius:8px;background:#fdfefe;min-height:42px}.detail-placeholder{padding:11px 15px;color:#8c98aa;font-size:10px}.detail-placeholder>span{color:#5a7596;font-size:15px;margin-right:8px}.detail-content{display:grid;grid-template-columns:minmax(0,1.35fr) minmax(230px,1fr);gap:25px;padding:19px 20px;border-left:3px solid var(--tone);border-radius:7px}.detail-content h3{font-size:14px;margin-bottom:7px}.detail-content p{font-size:12px;color:#5d6d83;line-height:1.7}.detail-content p+p{margin-top:7px}.detail-content .source-list{margin:0}.detail-content .source-list a{padding:5px 0;display:grid;grid-template-columns:1fr auto;gap:2px 8px}.detail-content .source-label{grid-column:1/-1}.detail-content .source-list code{font-size:9px}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(235px,1fr));gap:12px;margin-top:16px}.concept-card{background:#fff;border:1px solid #dfe6ee;border-radius:8px;padding:17px 18px;position:relative}.concept-card:before{content:"";display:block;width:22px;height:2px;background:var(--tone);margin-bottom:11px}.concept-card h3{font-size:12px;font-weight:680;margin-bottom:6px}.concept-card p{font-size:11px;line-height:1.75;color:#62718a}.table-scroll{overflow-x:auto;margin-top:16px;background:#fff;border:1px solid var(--line);border-radius:8px}table{width:100%;border-collapse:collapse;font-size:11px;line-height:1.65}th{text-align:left;padding:10px 15px;background:#f0f4f8;font-size:10px;color:#4b607c;font-weight:650}td{padding:10px 15px;border-top:1px solid #e6ebf2;color:#53647b;vertical-align:top}td:first-child{color:#273e5b;font-weight:550}.evidence{margin-top:15px;border-top:1px solid #dfe6ef;padding-top:11px}.evidence summary{font-size:10px;color:#677b95;cursor:pointer;width:fit-content}.evidence summary span{color:#99a4b2;margin-left:10px}.source-list{list-style:none;padding:0;margin:9px 0 0}.source-list a{display:flex;align-items:baseline;gap:15px;font-size:10px;padding:4px 0;color:#546c8d;text-decoration:none}.source-list a:hover{text-decoration:underline}.source-label{font-weight:560}.source-list code{color:#8a97aa;font-size:9px;overflow-wrap:anywhere}.source-list a>span:last-child{font-size:11px;color:#9baabc}.print-node-notes{display:none}.glossary-section{margin-top:24px;padding-top:32px;border-top:1px solid var(--line)}.glossary{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:0 34px;margin:20px 0 0}.glossary>div{padding:14px 0;border-bottom:1px solid #e0e6ef;break-inside:avoid}.glossary dt{font-size:12px;font-weight:680;color:#2b4262}.glossary dd{margin:5px 0 0;color:#67788f;font-size:11px;line-height:1.75}.scope{margin-top:30px;padding:21px 24px;background:#edf1f6;border-radius:9px}.scope h3{font-size:12px}.scope ul{padding-left:17px;margin:10px 0 0;font-size:11px;color:#667a93;line-height:1.85}.page-footer{display:flex;justify-content:space-between;gap:15px;color:#97a2b1;font-size:9px;border-top:1px solid var(--line);margin-top:44px;padding-top:19px}.build-warning{background:#fff2d6;border:2px solid #b97700;padding:20px;border-radius:8px;margin-bottom:25px}.build-warning ul{font-size:12px}
.print-actions{display:flex;align-items:center;gap:8px}.print-actions select{font-family:inherit;font-size:10px;line-height:1.5;color:#657791;background:transparent;border:0;padding:5px 7px;max-width:140px}.print-sources{display:none}.architecture-svg{min-width:1000px}
@media(min-width:1650px){main{padding-left:60px;padding-right:60px}.app-layout{grid-template-columns:265px minmax(0,1fr)}}
@media(max-width:1100px){.app-layout{grid-template-columns:205px minmax(0,1fr)}.sidebar{padding:25px 15px}.outline a{font-size:11px;gap:5px}main{padding:32px 25px}.detail-content{grid-template-columns:1fr}.cards{grid-template-columns:repeat(auto-fit,minmax(210px,1fr))}}
@media(max-width:760px){.app-layout{display:block}.sidebar{position:relative;height:auto;padding:18px 20px;border-right:none;border-bottom:1px solid var(--line)}.brand-symbol{width:28px;height:32px;font-size:20px}.nav-caption,.sidebar-footer{display:none}.outline{margin-top:15px;display:flex;flex-direction:row;overflow:auto;gap:7px;padding-bottom:3px}.outline a{min-width:150px;font-size:10px;padding:6px 8px;grid-template-columns:18px 1fr}.brand small{display:none}main{padding:27px 16px}.hero{margin-bottom:30px}.hero-topline{align-items:flex-start}.eyebrow{font-size:8px;letter-spacing:1.4px}.print-button{font-size:9px;padding:6px 8px}h1{font-size:31px;letter-spacing:-1.1px}.intro{font-size:12px}.legend{gap:8px 15px}.legend-item{font-size:9px}.section-heading{gap:10px}h2{font-size:22px}.section-subtitle{font-size:11px}.cards{grid-template-columns:1fr}.glossary{grid-template-columns:1fr}.page-footer{flex-direction:column}.detail-content{padding:15px}.source-list a{flex-wrap:wrap;gap:3px 10px}.atlas-section{margin-bottom:38px}}
@media(prefers-reduced-motion:reduce){html{scroll-behavior:auto}*{transition:none!important}}
@page{size:A3 landscape;margin:12mm 14mm}
@media print{html{scroll-behavior:auto}body{background:#fff;color:#17263b;font-size:10px;-webkit-print-color-adjust:exact;print-color-adjust:exact}.app-layout{display:block;max-width:none}.sidebar,.skip-link,.print-button,.diagram-toolbar,.node-detail,.build-warning{display:none!important}main{max-width:none;padding:0}.hero{padding:0 0 9mm;margin:0 0 9mm;break-after:page;border-bottom:0}.hero-topline{margin:8mm 0 9mm}.eyebrow{font-size:11px}h1{font-size:42px;max-width:290mm}.intro{font-size:17px;max-width:310mm;margin-top:8mm;line-height:1.75}.metadata{font-size:12px;margin-top:12mm}.metadata code{font-size:12px}.legend{margin-top:15mm;gap:8mm}.legend-item{font-size:12px}.legend-item i{width:10px;height:10px}.atlas-section{break-before:page;margin:0;scroll-margin:0}.section-heading{margin-bottom:5mm;gap:4mm;break-after:avoid}.section-number{font-size:10px;padding:7px;min-width:27px;margin-top:2px}h2{font-size:24px}.section-subtitle{font-size:11px;max-width:350mm;margin-top:4px}.diagram-shell{border-radius:6px;box-shadow:none;overflow:visible;border:none}.diagram-figure,.diagram-figure.is-active{display:block!important;break-inside:avoid;margin:0 0 5mm}.diagram-figure+ .diagram-figure{break-before:page;padding-top:4mm}.diagram-figure figcaption{font-size:12px;padding:0 0 3mm}.svg-viewport{overflow:visible;padding:0;width:100%}.architecture-svg{min-width:0;width:100%;height:auto;max-height:170mm}.cards{grid-template-columns:repeat(3,minmax(0,1fr));gap:3mm;margin-top:4mm;break-inside:avoid}.concept-card{padding:3mm 4mm;border-radius:5px}.concept-card:before{margin-bottom:2mm;width:16px}.concept-card h3{font-size:11px;margin-bottom:3px}.concept-card p{font-size:10px;line-height:1.6}.table-scroll{overflow:visible;break-inside:avoid;margin-top:4mm}table{font-size:10px;line-height:1.5}th{font-size:10px;padding:2mm 3mm}td{padding:2mm 3mm}.evidence{display:none}.print-node-notes{display:block;break-before:page;padding-top:3mm}.print-node-notes>h3{font-size:14px;margin:0 0 4mm;padding-bottom:3mm;border-bottom:1px solid #dde4ee}.print-note-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:5mm 7mm}.print-note-grid article{break-inside:avoid}.print-note-grid h4{font-size:11px;margin:0 0 2mm}.print-note-grid p{font-size:10px;line-height:1.6;color:#52637b;margin:0 0 1.5mm}.print-note-grid .source-list{margin-top:2mm}.print-note-grid .source-list a{display:block;padding:0;font-size:8px}.print-note-grid .source-list code{display:block;font-size:7.5px}.print-note-grid .source-list a>span:last-child{display:none}.glossary-section{break-before:page;margin:0;padding:0;border:0}.glossary{grid-template-columns:repeat(3,minmax(0,1fr));gap:0 8mm;margin-top:5mm}.glossary>div{padding:3mm 0}.glossary dt{font-size:11px}.glossary dd{font-size:10px;line-height:1.6}.scope{padding:4mm 5mm;margin-top:6mm;break-inside:avoid}.scope h3{font-size:11px}.scope ul{font-size:10px;line-height:1.7}.page-footer{margin-top:7mm;padding-top:3mm;font-size:8px}.page-footer span:last-child{font-size:7px}a{text-decoration:none}.diagram-node{cursor:default}}
@media(max-width:760px){.print-actions{flex-direction:column-reverse;gap:3px;align-items:flex-end}.print-actions select{font-size:8px;padding:2px;max-width:105px}}
@media print{.print-actions{display:none!important}.architecture-svg{max-height:163mm}.has-table .architecture-svg{max-height:135mm}.cards{grid-template-columns:repeat(auto-fit,minmax(28%,1fr))}.print-sources{display:flex;align-items:baseline;gap:3mm;border-top:1px solid #e4e9ef;margin-top:3mm;padding-top:2mm;break-inside:avoid;font-size:8px;color:#70829a}.print-sources>strong{white-space:nowrap;font-size:8px;font-weight:550}.print-sources .source-list{margin:0;display:flex;flex-wrap:wrap;gap:1mm 4mm}.print-sources .source-list li{display:inline}.print-sources .source-list a{display:inline;font-size:7.5px;padding:0}.print-sources .source-label,.print-sources .source-list a>span:last-child{display:none}.print-sources .source-list code{font-size:7.5px;color:#70829a}body[data-print-mode="visual"] .print-node-notes{display:none}}
'''


JS = r'''
(()=>{'use strict';
const nodes=JSON.parse(document.getElementById('atlas-node-data').textContent);
const safe=s=>String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function selectNode(el){const data=nodes[el.dataset.node];if(!data)return;const section=el.closest('.atlas-section');section.querySelectorAll('[data-selected]').forEach(n=>n.removeAttribute('data-selected'));el.dataset.selected='true';const panel=section.querySelector('.node-detail');panel.innerHTML='<div class="detail-content tone-'+safe(data.tone)+'"><div><h3>'+safe(data.title)+'</h3>'+data.detail.map(p=>'<p>'+safe(p)+'</p>').join('')+'</div><div>'+data.sources_html+'</div></div>';}
document.querySelectorAll('.diagram-node').forEach(el=>{el.addEventListener('click',()=>selectNode(el));el.addEventListener('keydown',event=>{if(event.key==='Enter'||event.key===' '){event.preventDefault();selectNode(el);}});});
document.querySelectorAll('.variant-tabs').forEach(tablist=>{const tabs=[...tablist.querySelectorAll('[role="tab"]')];function activate(tab,focus=false){const section=tab.closest('.atlas-section');tabs.forEach(t=>{const selected=t===tab;t.setAttribute('aria-selected',String(selected));t.tabIndex=selected?0:-1;});section.querySelectorAll('.diagram-figure').forEach(f=>f.classList.toggle('is-active',f.dataset.variantIndex===tab.dataset.variant));if(focus)tab.focus();}tabs.forEach((tab,i)=>{tab.addEventListener('click',()=>activate(tab));tab.addEventListener('keydown',event=>{let next;if(event.key==='ArrowRight')next=(i+1)%tabs.length;if(event.key==='ArrowLeft')next=(i+tabs.length-1)%tabs.length;if(event.key==='Home')next=0;if(event.key==='End')next=tabs.length-1;if(next!==undefined){event.preventDefault();activate(tabs[next],true);}});});});
document.getElementById('print-mode').addEventListener('change',event=>{document.body.dataset.printMode=event.target.value;});
document.getElementById('print-button').addEventListener('click',()=>window.print());
document.querySelectorAll('.export-svg').forEach(button=>button.addEventListener('click',()=>{const section=button.closest('.atlas-section');const svg=section.querySelector('.diagram-figure.is-active svg').cloneNode(true);svg.querySelectorAll('[data-selected]').forEach(n=>n.removeAttribute('data-selected'));const source='<?xml version="1.0" encoding="UTF-8"?>\n'+new XMLSerializer().serializeToString(svg);const url=URL.createObjectURL(new Blob([source],{type:'image/svg+xml;charset=utf-8'}));const link=document.createElement('a');link.href=url;link.download='ferrodb-'+svg.dataset.diagram+'.svg';document.body.appendChild(link);link.click();link.remove();setTimeout(()=>URL.revokeObjectURL(url),1000);}));
const outlineLinks=[...document.querySelectorAll('.outline a')];let scheduled=false;function markCurrent(){scheduled=false;let current=outlineLinks[0];for(const link of outlineLinks){const target=document.querySelector(link.getAttribute('href'));if(target&&target.getBoundingClientRect().top<=160)current=link;}outlineLinks.forEach(link=>link===current?link.setAttribute('aria-current','location'):link.removeAttribute('aria-current'));}window.addEventListener('scroll',()=>{if(!scheduled){scheduled=true;requestAnimationFrame(markCurrent);}}, {passive:true});markCurrent();
// Browser-level audit: actual glyph bounds, all variants, and viewport containment.
window.atlasCheckLayout=()=>{
const issues=[];const figures=[...document.querySelectorAll('.diagram-figure')];const saved=figures.map(f=>f.style.display);figures.forEach(f=>f.style.display='block');
const intersects=(a,b,pad=0)=>a.x+a.w>b.x+pad&&a.x<b.x+b.w-pad&&a.y+a.h>b.y+pad&&a.y<b.y+b.h-pad;
const segmentInRect=(a,b,r)=>{let lo=0,hi=1;const dx=b.x-a.x,dy=b.y-a.y;const ps=[-dx,dx,-dy,dy],qs=[a.x-r.x,r.x+r.w-a.x,a.y-r.y,r.y+r.h-a.y];for(let i=0;i<4;i++){if(Math.abs(ps[i])<1e-8){if(qs[i]<0)return false;}else{const t=qs[i]/ps[i];if(ps[i]<0)lo=Math.max(lo,t);else hi=Math.min(hi,t);if(lo>hi)return false;}}return hi>lo&&hi>0&&lo<1;};
document.querySelectorAll('.architecture-svg').forEach(svg=>{const vb=svg.viewBox.baseVal;
svg.querySelectorAll('text').forEach(text=>{const b=text.getBBox();if(b.x < -1||b.y < -1||b.x+b.width>vb.width+1||b.y+b.height>vb.height+1)issues.push({kind:'diagram-text-overflow',diagram:svg.dataset.diagram,text:text.textContent,bounds:{x:b.x,y:b.y,w:b.width,h:b.height}});});
const rects=[...svg.querySelectorAll('.diagram-node')].map(node=>{const[x,y,w,h]=node.dataset.bounds.split(',').map(Number);return{node,x,y,w,h};});
rects.forEach(r=>{const{node,x,y,w,h}=r;node.querySelectorAll('text').forEach(text=>{const b=text.getBBox();if(b.x<x+9||b.x+b.width>x+w-9||b.y<y+6||b.y+b.height>y+h-6)issues.push({kind:'node-text-overflow',diagram:svg.dataset.diagram,node:node.dataset.node,text:text.textContent,bounds:{x:b.x,y:b.y,w:b.width,h:b.height},nodeBounds:{x,y,w,h}});});});
for(let i=0;i<rects.length;i++)for(let j=i+1;j<rects.length;j++)if(intersects(rects[i],rects[j],1))issues.push({kind:'node-overlap',diagram:svg.dataset.diagram,nodes:[rects[i].node.dataset.modelId,rects[j].node.dataset.modelId]});
svg.querySelectorAll('.edge-label-group text').forEach(text=>{const b=text.getBBox(),label={x:b.x,y:b.y,w:b.width,h:b.height};rects.forEach(r=>{if(intersects(label,r,1))issues.push({kind:'edge-label-node-overlap',diagram:svg.dataset.diagram,text:text.textContent,node:r.node.dataset.modelId,edgeIndex:text.parentElement.dataset.edgeIndex});});});
svg.querySelectorAll('.diagram-edge').forEach(edge=>{const points=[...edge.points];rects.filter(r=>![edge.dataset.from,edge.dataset.to].includes(r.node.dataset.modelId)).forEach(r=>{const inset={x:r.x+2,y:r.y+2,w:r.w-4,h:r.h-4};for(let i=1;i<points.length;i++)if(segmentInRect(points[i-1],points[i],inset)){issues.push({kind:'edge-crosses-unrelated-node',diagram:svg.dataset.diagram,edgeIndex:edge.dataset.edgeIndex,from:edge.dataset.from,to:edge.dataset.to,node:r.node.dataset.modelId});break;}});});
});figures.forEach((f,i)=>f.style.display=saved[i]);return{ok:issues.length===0,issues,diagrams:document.querySelectorAll('.architecture-svg').length,nodes:document.querySelectorAll('.diagram-node').length,bodyOverflow:document.documentElement.scrollWidth>window.innerWidth+1};};
window.atlasReady=true;
})();
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, default=HERE/'atlas.json')
    parser.add_argument('--output', type=Path, default=HERE/'index.html')
    parser.add_argument('--allow-overflow', action='store_true', help='Render with a visible warning rather than fail on detected bounds issues')
    parser.add_argument('--check', action='store_true', help='Validate and render in memory without writing output')
    args = parser.parse_args()
    model = json.loads(args.input.read_text())
    renderer = Renderer(model)
    result = renderer.render()
    for note in renderer.notes:
        print('NOTE: '+note, file=sys.stderr)
    for issue in renderer.issues:
        print('LAYOUT ERROR: '+issue, file=sys.stderr)
    if renderer.issues and not args.allow_overflow:
        print('No output written. Fix geometry/text or use --allow-overflow for a diagnostic render.', file=sys.stderr)
        return 2
    if not args.check:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(result)
        print(f'Wrote {args.output} ({len(result.encode()):,} bytes; {len(model["sections"])} sections; {len(renderer.nodes)} diagram nodes).')
    print(f'Layout preflight: {len(renderer.issues)} errors, {len(renderer.notes)} bounded font adjustments. Run window.atlasCheckLayout() in a browser for measured glyph bounds.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
