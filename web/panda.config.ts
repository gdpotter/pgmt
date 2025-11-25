import { defineConfig } from '@pandacss/dev';
import { tokens, semanticTokens, globalCss } from './theme';
import {
  sectionRecipe,
  featureIconRecipe,
  heroTitleRecipe,
  heroSubtitleRecipe,
  cardRecipe,
  iconRecipe,
  featureRecipe,
  featureHeaderRecipe,
  featureTitleRecipe,
  featureDescriptionRecipe,
  comparisonRecipe,
  comparisonItemRecipe,
  comparisonHeaderRecipe,
  comparisonContentRecipe,
  tabsContainerRecipe,
  tabsListRecipe,
  tabRecipe,
  tabPanelRecipe,
  badgeRecipe,
} from './theme/recipes';
import {
  heroPattern,
  featureGridPattern,
  sectionWrapperPattern,
  ctaGroupPattern,
  statsPattern,
} from './theme/patterns';

export default defineConfig({
  // Whether to use css reset
  preflight: true,

  // Where to look for your css declarations
  include: [
    './src/**/*.{ts,tsx,js,jsx,astro}',
    './pages/**/*.{ts,tsx,js,jsx,astro}',
  ],

  // Files to exclude
  exclude: [],

  staticCss: {
    recipes: {
      // Generate ALL variants for frequently used recipes
      icon: ['*'],
      card: ['*'],
      badge: ['*'],
      feature: ['*'],
      featureTitle: ['*'],
      featureDescription: ['*'],
      comparison: ['*'],
      comparisonItem: ['*'],
      comparisonHeader: ['*'],
      comparisonContent: ['*'],
      tabs: ['*'],
      tabsList: ['*'],
      tab: ['*'],
      tabPanel: ['*'],
      heroTitle: ['*'],
      heroSubtitle: ['*'],
      section: ['*'],
    },
    css: [
      {
        properties: {
          // Common dynamic properties used in components
          backgroundColor: ['surface', 'surface.raised', 'transparent'],
          borderColor: ['border.subtle', 'border.default', 'border.strong'],
          padding: ['sm', 'md', 'lg', 'xl', '2xl'],
          gap: ['sm', 'md', 'lg', 'xl', '2xl'],
          borderRadius: ['md', 'lg', 'xl'],
          fontSize: ['sm', 'md', 'lg', 'xl', '2xl', '3xl'],
          fontWeight: ['400', '500', '600', '700'],
          marginBottom: ['sm', 'md', 'lg', 'xl', '2xl', '3xl'],
          marginTop: ['sm', 'md', 'lg', 'xl', '2xl', '3xl'],
        },
      },
    ],
  },

  // Custom patterns
  patterns: {
    extend: {
      hero: heroPattern,
      featureGrid: featureGridPattern,
      sectionWrapper: sectionWrapperPattern,
      ctaGroup: ctaGroupPattern,
      stats: statsPattern,
    },
  },

  // Theme configuration
  theme: {
    extend: {
      tokens,
      semanticTokens,
      recipes: {
        section: sectionRecipe,
        featureIcon: featureIconRecipe,
        heroTitle: heroTitleRecipe,
        heroSubtitle: heroSubtitleRecipe,
        card: cardRecipe,
        icon: iconRecipe,
        feature: featureRecipe,
        featureHeader: featureHeaderRecipe,
        featureTitle: featureTitleRecipe,
        featureDescription: featureDescriptionRecipe,
        comparison: comparisonRecipe,
        comparisonItem: comparisonItemRecipe,
        comparisonHeader: comparisonHeaderRecipe,
        comparisonContent: comparisonContentRecipe,
        tabsContainer: tabsContainerRecipe,
        tabsList: tabsListRecipe,
        tab: tabRecipe,
        tabPanel: tabPanelRecipe,
        badge: badgeRecipe,
      },
    },
  },

  // Global CSS
  globalCss,

  // The output directory for your css system
  outdir: 'styled-system',
});
