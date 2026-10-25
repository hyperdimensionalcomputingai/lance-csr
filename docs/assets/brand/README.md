# lance-csr brand assets

The approved cube logo: an isometric sparse matrix of ivory and charcoal cubes, an orange chevron, and the original squared wordmark. The small organization name beneath the wordmark has been removed.

## Files

| Use | Light background | Dark background | PNG widths |
| --- | --- | --- | --- |
| Desktop wordmark | `desktop-light.svg` | `desktop-dark.svg` | 508, 1016 px |
| Mobile emblem | `mobile-light.svg` | `mobile-dark.svg` | 128, 256 px |
| Favicon | `favicon-light.svg` | `favicon-dark.svg` | 16, 32, 48 px |

SVG files embed PNG artwork to preserve the approved shading and lettering; they are **not vector outlines**. All asset PNG files have transparent backgrounds. Keep the aspect ratio when resizing. The desktop master is 1996 × 424 px, the mobile master is 540 × 424 px, and favicon canvases are square. `source-light.png` and `source-dark.png` retain the full-resolution imagegen edits.

ICO files contain 16, 32 and 48 px variants. `favicon.ico` is the light-background fallback; `favicon.svg` follows the operating system's colour scheme. The site uses explicit light/dark header and home-page images to follow its manual theme toggle. The cube emblem is retained in the favicon; individual cubes are naturally less distinct at 16 px.

## Colours and usage

| Role | Light | Dark |
| --- | --- | --- |
| Wordmark | Charcoal, approximately `#292929` | Warm white, approximately `#F2EFE8` |
| Cubes | Ivory, charcoal and orange | Ivory, charcoal and orange |
| Docs link colour | `#B44700` | `#FFA252` |
| Preview background | `#FFFFFF` | `#171717` |

Use the full wordmark around 300–360 px wide and the mobile emblem around 64 px wide. At smaller header sizes, use the emblem without lettering. Give linked header images `alt="lance-csr home"` and decorative duplicates `alt=""`.

`preview.html` shows both modes at desktop, mobile and favicon sizes; `preview.png` is its rendered export. Raster dimensions and transparency are checked during packaging.

`social-preview.png` is an opaque 1731 × 909 px banner for shared links, with the light wordmark centred on a
warm-white background. The shared template declares its absolute URL through Open Graph and Twitter card
metadata. Chat and social apps may cache older previews after deployment.

## Provenance

Source supplied by the user: `lance-csr-logo.png`. Built-in imagegen removed the bottom line and adapted wordmark contrast for light and dark backgrounds. The cube arrangement and letter shapes were preserved rather than replaced with a new design. The generation prompt is saved at `../../../output/imagegen/cube-logo-prompts.txt`.

The docs header, home-page wordmarks and favicon use these assets. The ZIP package is at `../lance-csr-brand-assets.zip`.

The social banner was generated with built-in imagegen from `desktop-light-1016.png`. Prompt: place the complete
approved logo centrally on a solid warm-white (#FAF9F6) landscape canvas, approximately 1.905:1, with ample
margin; preserve the cube arrangement, orange chevron, charcoal wordmark, proportions and colours; add no text
or decoration. The generated size is retained, with the actual dimensions declared in the metadata.
