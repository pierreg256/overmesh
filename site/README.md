# site/

Zola source for the Overmesh documentation site, plus the visual identity
mockups that preceded it.

The mockups remain the visual reference. Open `mockup-landing.html` directly in
a browser to compare the production templates with the approved static design.

| File | Purpose |
| --- | --- |
| `mockup.css` | Shared token layer, then the two surfaces |
| `mockup-landing.html` | Pure-black surface: hero, adoption argument, trade-off |
| `mockup-reference.html` | Softened reading surface: long-form document, tables, code, sticky TOC |

## Build

`make site-content` reads the explicit publication registry in
`docs/traceability.toml`, adds Zola front matter, removes the authored H1, and
rewrites relative links. Authored Markdown remains in place; assembled pages
under `site/content/` are ignored.

`make site-build` uses Zola 0.23.3 and Pagefind 1.5.2. The production workflow
installs those exact versions before publishing `site/public/`.

Space Grotesk 5.3.0 is self-hosted for display text, Inter 5.3.0 for prose,
and JetBrains Mono 5.3.0 for code. Their WOFF2 files and licenses are versioned
under `static/fonts/`; the site makes no font or script request to a
third-party origin.

## Visual reference

To settle the visual identity before any generator is installed. The two files
deliberately share one stylesheet, because the question they answer is whether
a single token system can carry both a landing page at full contrast and a
1,000-line specification that stays readable.

The production mapping is:

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

The dot-matrix motif remains a mark rather than a page background. Tables scroll
horizontally on narrow screens so normative rows are not reinterpreted as
cards.
