# Theme and layout

The host owns theme mode and palette, including custom palettes within the same mode. Use `--pioneer-background`, `--pioneer-foreground`, `--pioneer-muted`, `--pioneer-border`, `--pioneer-accent`, and `--pioneer-accent-foreground`. Check the current tool contract for supported tokens before using additional ones. `--pioneer-muted` is muted text (not a background surface). Accent is the primary action surface, not a link or general text color. Use `--pioneer-accent-foreground` for text on that surface.

Do not declare these reserved variables yourself. Author local semantic tokens derived from them if useful. Use `html, body { margin: 0; padding: 0; }`, content-driven sections, `min-width: 0` for grid/flex children, wrapping labels, and responsive columns. Avoid `height: 100vh`, `min-height: 100vh`, and full-page overflow traps. Preserve visible keyboard focus and readable text at narrow widths.

CSS variables update without reloading the document. For Canvas or JavaScript charts, read `getComputedStyle(document.documentElement)` when drawing, then listen with `window.addEventListener('pioneer-theme', redraw)`. Also redraw after size changes. The event signals that host tokens are current; do not infer palette from a theme name or cache only a light/dark boolean. Use explicit token colors in graphics rather than fixed colors that become unreadable in the other mode.

Preview uses the two standard Pioneer palettes. A successful preview does not validate arbitrary future custom colors; semantic host tokens allow those colors to be applied naturally.
