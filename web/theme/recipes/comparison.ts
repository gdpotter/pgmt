import { defineRecipe } from '@pandacss/dev';

export const comparisonRecipe = defineRecipe({
  className: 'comparison',
  base: {
    display: 'flex',
    gap: 'xl',
    alignItems: 'stretch',
  },
  variants: {
    direction: {
      horizontal: {
        flexDirection: 'row',
        '@media (max-width: 768px)': {
          flexDirection: 'column',
          gap: 'lg',
        },
      },
      vertical: { flexDirection: 'column' },
    },
    gap: {
      sm: { gap: 'md' },
      md: { gap: 'lg' },
      lg: { gap: 'xl' },
      xl: { gap: '2xl' },
    },
  },
  defaultVariants: {
    direction: 'horizontal',
    gap: 'lg',
  },
});

export const comparisonItemRecipe = defineRecipe({
  className: 'comparison-item',
  base: {
    flex: '1',
  },
  variants: {
    type: {
      before: {},
      after: {},
      neutral: {},
    },
  },
  defaultVariants: {
    type: 'neutral',
  },
});

export const comparisonHeaderRecipe = defineRecipe({
  className: 'comparison-header',
  base: {
    fontSize: 'xl',
    fontWeight: '600',
    marginBottom: 'lg',
    textAlign: 'center',
    fontFamily: 'heading',
  },
  variants: {
    type: {
      before: {
        color: 'error',
        _before: { content: '"❌ "' },
      },
      after: {
        color: 'success',
        _before: { content: '"✅ "' },
      },
      neutral: { color: 'text.primary' },
    },
  },
  defaultVariants: {
    type: 'neutral',
  },
});

export const comparisonContentRecipe = defineRecipe({
  className: 'comparison-content',
  base: {
    fontFamily: 'mono',
    fontSize: 'sm',
    color: 'text.secondary',
    '& .highlight-error': {
      color: 'error',
    },
    '& .highlight-success': {
      color: 'success',
    },
    '& .highlight-muted': {
      color: 'text.muted',
    },
  },
  variants: {
    variant: {
      code: {
        backgroundColor: 'surface.raised',
        padding: 'md',
        borderRadius: 'md',
        overflow: 'auto',
      },
      text: {
        padding: 'sm',
      },
    },
  },
  defaultVariants: {
    variant: 'text',
  },
});
