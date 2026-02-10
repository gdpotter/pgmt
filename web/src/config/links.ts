/**
 * Centralized URL configuration
 *
 * All URLs used across the site should be defined here to:
 * - Prevent URLs from getting stale
 * - Make URL changes easy
 * - Catch typos at compile time
 */

const GITHUB_REPO = 'https://github.com/gdpotter/pgmt';

export const links = {
  // External - GitHub
  github: {
    repo: GITHUB_REPO,
    releases: `${GITHUB_REPO}/releases`,
    discussions: `${GITHUB_REPO}/discussions`,
    issues: `${GITHUB_REPO}/issues`,
  },

  // Internal - Main pages
  home: '/',
  whyPgmt: '/why-pgmt',
  blog: '/blog',

  // Internal - Documentation
  docs: {
    root: '/docs',
    quickStart: '/docs/getting-started/quick-start',
    cli: '/docs/cli',
    roadmap: '/docs/project/roadmap',
    contributing: '/docs/development/contributing',
  },
} as const;

// Type helper for external link attributes
export const externalLinkAttrs = {
  target: '_blank',
  rel: 'noopener noreferrer',
} as const;
