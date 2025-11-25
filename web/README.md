# pgmt Web

Documentation and marketing site built with [Astro](https://astro.build), [Starlight](https://starlight.astro.build), and [Panda CSS](https://panda-css.com).

## Project Structure

```text
web/
├── public/                  # Static assets (images, favicons, etc.)
├── src/
│   ├── content/
│   │   ├── config.ts       # Content collections configuration
│   │   └── docs/           # Starlight documentation files (.md/.mdx)
│   ├── components/         # Reusable Astro components
│   ├── layouts/            # Page layouts
│   ├── pages/              # Custom pages (marketing, landing, etc.)
│   └── *.svg              # Logo files
├── theme/                  # Panda CSS theme configuration
│   ├── tokens.ts          # Design tokens (colors, spacing, etc.)
│   ├── semanticTokens.ts  # Semantic token mappings
│   ├── recipes/           # Component style recipes
│   └── patterns.ts        # Layout patterns
├── styled-system/          # Generated Panda CSS output (don't edit)
├── astro.config.mjs       # Astro + Starlight configuration
├── panda.config.ts        # Panda CSS configuration
└── package.json
```

## Commands

Run from the `web/` directory:

| Command             | Action                                      |
| :------------------ | :------------------------------------------ |
| `pnpm install`      | Installs dependencies                       |
| `pnpm prepare`      | Generates Panda CSS styled-system           |
| `pnpm dev`          | Starts local dev server at `localhost:4321` |
| `pnpm build`        | Build production site to `./dist/`          |
| `pnpm preview`      | Preview build locally before deploying      |
| `pnpm astro ...`    | Run Astro CLI commands                      |
| **Code Quality**    |                                             |
| `pnpm lint`         | Run ESLint to check code quality            |
| `pnpm lint:fix`     | Run ESLint and auto-fix issues              |
| `pnpm format`       | Format code with Prettier                   |
| `pnpm format:check` | Check if code is properly formatted         |
| `pnpm typecheck`    | Run TypeScript type checking                |
| `pnpm check`        | Run all checks (lint + format + typecheck)  |

## Documentation

Documentation lives in `src/content/docs/` as Markdown/MDX files. Starlight generates navigation from the file structure automatically.

To add docs: create `.md` or `.mdx` files with frontmatter for title/description.

## Styling

Panda CSS theme configuration lives in `theme/`:

- `tokens.ts` - Base design values (colors, spacing, typography)
- `semanticTokens.ts` - Context-aware token mappings
- `recipes/` - Component styles with variants
- `patterns.ts` - Layout utilities

Example usage:

```astro
---
import { css } from '../styled-system/css';
import { button } from '../styled-system/recipes';
---

<h2 class={css({ fontSize: '2xl', color: 'text.primary' })}>Title</h2>
<button class={button({ variant: 'primary' })}>Click me</button>
```
