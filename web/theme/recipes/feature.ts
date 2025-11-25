import { defineRecipe } from '@pandacss/dev';

export const featureRecipe = defineRecipe({
  className: 'feature',
  base: {
    display: 'flex',
    flexDirection: 'column',
    gap: 'md',
  },
  variants: {
    layout: {
      vertical: {
        alignItems: 'stretch',
      },
      horizontal: {
        flexDirection: 'row',
        alignItems: 'flex-start',
        gap: 'lg',
      },
      centered: {
        alignItems: 'center',
        textAlign: 'center',
      },
    },
    spacing: {
      tight: { gap: 'sm' },
      normal: { gap: 'md' },
      loose: { gap: 'lg' },
      spacious: { gap: 'xl' },
    },
  },
  defaultVariants: {
    layout: 'vertical',
    spacing: 'normal',
  },
});

export const featureHeaderRecipe = defineRecipe({
  className: 'feature-header',
  base: {
    display: 'flex',
    alignItems: 'flex-start',
    gap: 'lg',
  },
  variants: {
    alignment: {
      start: { alignItems: 'flex-start' },
      center: { alignItems: 'center' },
      end: { alignItems: 'flex-end' },
    },
    gap: {
      sm: { gap: 'sm' },
      md: { gap: 'md' },
      lg: { gap: 'lg' },
      xl: { gap: 'xl' },
    },
  },
  defaultVariants: {
    alignment: 'start',
    gap: 'lg',
  },
});

export const featureTitleRecipe = defineRecipe({
  className: 'feature-title',
  base: {
    fontWeight: '600',
    color: 'text.primary',
    fontFamily: 'heading',
    lineHeight: '1.2',
  },
  variants: {
    size: {
      sm: { fontSize: 'md' },
      md: { fontSize: 'lg' },
      lg: { fontSize: 'xl' },
      xl: { fontSize: '2xl' },
    },
    align: {
      left: { textAlign: 'left' },
      center: { textAlign: 'center' },
      right: { textAlign: 'right' },
    },
  },
  defaultVariants: {
    size: 'lg',
    align: 'left',
  },
  compoundVariants: [
    {
      size: 'xl',
      align: 'center',
      css: {
        marginBottom: 'lg',
        // Large centered titles get extra bottom margin
      },
    },
  ],
});

export const featureDescriptionRecipe = defineRecipe({
  className: 'feature-description',
  base: {
    color: 'text.secondary',
    lineHeight: '1.6',
  },
  variants: {
    size: {
      sm: { fontSize: 'sm' },
      md: { fontSize: 'md' },
      lg: { fontSize: 'lg' },
    },
    align: {
      left: { textAlign: 'left' },
      center: { textAlign: 'center' },
      right: { textAlign: 'right' },
    },
  },
  defaultVariants: {
    size: 'md',
    align: 'left',
  },
  compoundVariants: [
    {
      size: 'lg',
      align: 'center',
      css: {
        maxWidth: '48rem',
        marginX: 'auto',
        // Large centered descriptions are constrained and centered
      },
    },
  ],
});
