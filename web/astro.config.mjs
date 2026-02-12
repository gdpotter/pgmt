// @ts-check
import { defineConfig } from 'astro/config';
import sitemap from '@astrojs/sitemap';
import starlight from '@astrojs/starlight';
import rehypeMermaid from 'rehype-mermaid';
import { links } from './src/config/links.ts';

// https://astro.build/config
export default defineConfig({
  site: 'https://pgmt.dev',
  output: 'static', // Ensures static site generation for GitHub Pages
  markdown: {
    rehypePlugins: [[rehypeMermaid, { strategy: 'img-svg' }]],
  },
  integrations: [
    sitemap(),
    starlight({
      expressiveCode: {
        defaultProps: {
          frame: 'code',
        },
      },
      title: 'pgmt Documentation',
      description:
        'Modern, database-first migration tool for PostgreSQL designed around modular schemas, declarative drift detection, and explicit migrations.',
      logo: {
        light: './src/logo-light.svg',
        dark: './src/logo-dark.svg',
        replacesTitle: true,
      },
      social: [
        {
          icon: 'github',
          label: 'GitHub',
          href: links.github.repo,
        },
      ],
      disable404Route: true,
      components: {
        Footer: './src/components/DocsFooter.astro',
        Head: './src/components/DocsHead.astro',
      },
      sidebar: [
        {
          label: 'Getting Started',
          items: [
            { label: 'Quick Start', link: '/docs/getting-started/quick-start' },
          ],
        },
        {
          label: 'Using pgmt',
          items: [
            {
              label: 'Schema Organization',
              link: '/docs/guides/schema-organization',
            },
            {
              label: 'Migration Workflow',
              link: '/docs/guides/migration-workflow',
            },
            {
              label: 'Roles & Permissions',
              link: '/docs/guides/roles-and-permissions',
            },
          ],
        },
        {
          label: 'Going to Production',
          items: [
            {
              label: 'Multi-Section Migrations',
              link: '/docs/guides/multi-section-migrations',
            },
            { label: 'CI/CD Integration', link: '/docs/guides/ci-cd' },
            {
              label: 'Baseline Management',
              link: '/docs/guides/baseline-management',
            },
          ],
        },
        {
          label: 'Integrations',
          items: [
            {
              label: 'Adopt Existing Database',
              link: '/docs/guides/existing-database',
            },
            {
              label: 'Supabase',
              link: '/docs/guides/supabase',
            },
          ],
        },
        {
          label: 'Under the Hood',
          items: [
            {
              label: 'Why Schema-as-Code?',
              link: '/docs/concepts/philosophy',
            },
            { label: 'How pgmt Works', link: '/docs/concepts/how-it-works' },
            {
              label: 'Shadow Databases',
              link: '/docs/concepts/shadow-database',
            },
            {
              label: 'Dependency Tracking',
              link: '/docs/concepts/dependency-tracking',
            },
          ],
        },
        {
          label: 'Reference',
          items: [
            { label: 'CLI', link: '/docs/cli/' },
            {
              label: 'Configuration',
              link: '/docs/reference/configuration',
            },
            {
              label: 'Supported Features',
              link: '/docs/reference/supported-features',
            },
            {
              label: 'Troubleshooting',
              link: '/docs/guides/troubleshooting',
            },
          ],
        },
        {
          label: 'Project',
          items: [
            { label: 'Roadmap', link: '/docs/project/roadmap' },
            { label: 'Contributing', link: '/docs/development/contributing' },
            { label: 'Architecture', link: '/docs/development/architecture' },
          ],
        },
      ],
    }),
  ],
});
