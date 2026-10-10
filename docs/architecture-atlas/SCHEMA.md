# Renderer contract

`atlas.json` is the source of content. The renderer must not invent implementation claims.

Top level: `title`, `revision`, `date`, `intro`, `legend` (`key`, `label`), `sections`, `glossary` (`term`, `meaning`), `scope` (strings).

Each section: `id`, `number`, `title`, `subtitle`, `diagram`, optional `variants`, `cards`, optional `table`, `sources`.

`diagram`: `width`, `height`, `lanes`, `nodes`, `edges`, optional `texts`.

- Lane: `x`, `y`, `w`, `h`, `label`, `tone`.
- Node: `id`, `x`, `y`, `w`, `h`, `tone`, `title`, `lines` (short strings), `detail` (short paragraphs), `sources` (optional). Draw each as a rounded rectangle. Clicking or keyboard-activating the node shows detail and source citations in a section detail area. Long text wraps; no clipping.
- Edge: `from`, `to`, `points` ([[x,y],...], explicit route), optional `label`, `label_x`, `label_y`, `tone`, `dashed`. All arrows point from first to last coordinate. Use a light label background if necessary. Edges are behind nodes. from/to identify nodes, not implicit geometry.
- Text: `x`, `y`, `text`, optional `tone`, `size`, `weight`, `anchor`.
- Tone keys: `sql`, `branch`, `wal`, `check`, `lifecycle`, `neutral`. Keep colors consistent and provide accessible contrasts.
- `variants`: array of `{label, description, diagram}`. Optional step buttons replace the displayed SVG using pre-rendered versions; default `diagram` is first state. Print each variant as a separate figure or use a compact strip that preserves legibility; don't silently omit educational states. Avoid making all sections excessively tall.
- Card: `title`, `text`, optional `tone`.
- Table: `{headers:[...], rows:[[...],...]}`. Cells are plain text.
- Source: `{file:'src/...', line:123, label:'...'}`; display file:line and link to the source using a relative filesystem URL from docs/architecture-atlas/.

Output `index.html` with all CSS, SVG, JS inline and no network requests. Support responsive diagram scrolling, keyboard navigation, sticky outline, print mode, a glossary, and source details. Include global “Print / PDF” and section SVG export where feasible. Source evidence can be collapsed on screen. Keep the prose secondary to diagrams. Structure must support the final browser screenshot/text-overflow check and A3 landscape PDF export.
