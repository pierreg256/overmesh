# site/

Visual identity mockups. **Not** the production site.

These are three static files with no build step. Open
`mockup-landing.html` in a browser — everything is relative.

| File | Purpose |
| --- | --- |
| `mockup.css` | Shared token layer, then the two surfaces |
| `mockup-landing.html` | Pure-black surface: hero, adoption argument, trade-off |
| `mockup-reference.html` | Softened reading surface: long-form document, tables, code, sticky TOC |

## What they are for

To settle the visual identity before any generator is installed. The two files
deliberately share one stylesheet, because the question they answer is whether
a single token system can carry both a landing page at full contrast and a
1,000-line specification that stays readable.

Once agreed, these become the reference for the Zola templates:

- `mockup.css` tokens → `site/sass/_tokens.scss`
- `mockup-landing.html` → `templates/index.html`
- `mockup-reference.html` → `templates/page.html`

## Decisions embedded in the mockups

**Two surfaces, one palette.** Pure black (`#000`) is reserved for the landing
page. Reference pages use `#0b0b0f` with body text at `#d6d8e0`. White on pure
black is unreadable past three paragraphs.

**Glow is rationed.** The gradient appears once, on the landing `h1`. Glow is
applied to the hero, interactive elements, and the status badge — never to body
headings, where it becomes noise within two screens.

**Syntax highlighting is deliberately dull.** One accent for keywords, greys
for the rest. Neon code is the trap of every saturated dark theme.

**The ring is SVG.** Rebuilt as geometry rather than reusing the 950 KB raster:
a few kilobytes, crisp at any size, recolourable through the gradient
definition, and slowly animated. `prefers-reduced-motion` stops it.

**No third-party fonts.** System stacks here; self-hosted in production. A
project whose argument is reducing external dependency surface should not fetch
its typography from a CDN.

## Open points

- Display typeface is a placeholder stack. The open graph image suggests a
  heavy geometric sans — Space Grotesk and Chakra Petch are the two candidates.
- The dot-matrix ring motif is currently only a mark. It could also serve as a
  section divider and a very low opacity page background.
- Table density on narrow viewports: the Azure comparison table scrolls
  horizontally rather than collapsing. Acceptable, or worth a card layout?
