# pgmt Web - Documentation & Marketing Site

This directory contains the pgmt project's documentation and marketing website, built with **Astro**, **Starlight**, and **Panda CSS**.

## 🏗️ Tech Stack

- **[Astro](https://astro.build)** - Static site generator with excellent performance
- **[Starlight](https://starlight.astro.build)** - Documentation framework built on Astro
- **[Panda CSS](https://panda-css.com)** - Build-time atomic CSS-in-JS with type safety
- **TypeScript** - Full type safety throughout the project

## 🚀 Project Structure

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

## 🧞 Commands

All commands are run from the `web/` directory:

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

## 📚 Documentation (Starlight)

Documentation lives in `src/content/docs/` as Markdown/MDX files. Starlight automatically:

- Generates navigation from file structure
- Provides search functionality
- Handles responsive layout and dark/light themes
- Supports syntax highlighting, callouts, and more

**Adding new documentation:**

1. Create `.md` or `.mdx` files in `src/content/docs/`
2. Add frontmatter for title, description, and sidebar configuration
3. The sidebar auto-generates based on file structure

## 🎨 Styling (Panda CSS)

This project uses **Panda CSS** for styling with a comprehensive design system:

### Design System Structure

- **Tokens** (`theme/tokens.ts`) - Base design values (colors, spacing, typography)
- **Semantic Tokens** (`theme/semanticTokens.ts`) - Context-aware token mappings
- **Recipes** (`theme/recipes/`) - Reusable component styles with variants
- **Patterns** (`theme/patterns.ts`) - Layout and positioning utilities

### Using Styles in Components

```astro
---
import { css } from '../styled-system/css';
import { card, button } from '../styled-system/recipes';
---

<div class={card({ variant: 'elevated' })}>
  <h2 class={css({ fontSize: '2xl', color: 'text.primary' })}>Title</h2>
  <button class={button({ variant: 'primary', size: 'lg' })}>Click me</button>
</div>
```

### Development Workflow

1. **Install dependencies**: `pnpm install`
2. **Generate styles**: `pnpm prepare` (runs automatically on install)
3. **Start dev server**: `pnpm dev`
4. Edit components, pages, or theme files - styles regenerate automatically

## 🌟 Key Features

- **Documentation**: Full Starlight integration with auto-generated navigation
- **Marketing Pages**: Custom Astro pages for landing, features, etc.
- **Design System**: Comprehensive Panda CSS setup with tokens, recipes, and patterns
- **Type Safety**: Full TypeScript support including CSS-in-JS
- **Performance**: Static site generation with Astro's zero-JS by default approach
- **Responsive**: Mobile-first design with dark/light theme support

## 📖 Learn More

- **[Astro Documentation](https://docs.astro.build)** - Learn about Astro features
- **[Starlight Guide](https://starlight.astro.build)** - Starlight documentation framework
- **[Panda CSS Docs](https://panda-css.com)** - Build-time CSS-in-JS styling system
