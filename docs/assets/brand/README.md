# lance-csr brand assets

Option A: a sparse matrix extends into a compact lance. Light and dark variants share exactly the same geometry. These are clean vector interpretations of the approved imagegen concept, with original outlined lettering rather than a font dependency.

## Files

| Use | Light background | Dark background | PNG sizes |
| --- | --- | --- | --- |
| Desktop wordmark | `desktop-light.svg` | `desktop-dark.svg` | 508 × 96, 1016 × 192 |
| Mobile emblem | `mobile-light.svg` | `mobile-dark.svg` | 128 × 96, 256 × 192 |
| Favicon | `favicon-light.svg` | `favicon-dark.svg` | 16, 32, 48 px square |

All SVG and PNG assets have transparent backgrounds. PNG filenames include their width. ICO files contain 16, 32 and 48 px versions; `favicon.ico` is the light-background fallback. `favicon.svg` follows the operating system's preferred colour scheme automatically. For a manually selected site theme, choose the matching explicit variant instead.

Use the desktop logo around 254–300 px wide, and the mobile emblem around 64 px wide. Preserve the aspect ratio and built-in clear space. The favicon reduces the matrix to three columns and four rows to preserve separation at tiny sizes; it is not a diagram of a specific dataset.

## Colours

| Role | Light | Dark |
| --- | --- | --- |
| Filled cells and lance | `#2455A4` | `#82AEFF` |
| Empty cell outlines | `#6086BF` | `#6F92C8` |
| Lettering | `#172C48` | `#EDF3FF` |
| Preview background | `#FFFFFF` | `#101B2D` |

## Usage

Use SVG for headers; PNG files are fallbacks for tools that do not accept SVG. Give linked header images `alt="lance-csr home"` and decorative duplicates `alt=""`.

```html
<link rel="icon" href="/assets/brand/favicon.ico" sizes="any">
<link rel="icon" href="/assets/brand/favicon.svg" type="image/svg+xml">
```

Adjust URL paths to the site's actual asset location. No site configuration has been changed.

## Source and review

`preview.html` contains the vector masters and a side-by-side size preview; `preview.png` is its rendered export. The SVGs have accessible titles and descriptions, contain no external resources, and use path outlines for lettering. Raster sizes and alpha channels were checked, and both colour modes were visually reviewed.

The approved source concept and generation prompt are in `../../../output/imagegen/lance-matrix-options.png` and `../../../output/imagegen/lance-matrix-options-prompt.txt`. The production artwork was redrawn as vectors; it is not an automatic raster trace. Browser-tab favicon selection has not been tested in a deployed docs site.
