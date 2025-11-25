import { defineRecipe } from '@pandacss/dev';

export const heroTitleRecipe = defineRecipe({
  className: 'heroTitle',
  base: {
    fontWeight: '800',
    fontFamily: 'heading',
    marginBottom: 'xl',
    lineHeight: '1.05',
    animation: 'fade-in',
    position: 'relative',
  },
  variants: {
    size: {
      large: {
        fontSize: { base: '4xl', md: '6xl', lg: '7xl' },
      },
      medium: {
        fontSize: { base: '3xl', md: '4xl', lg: '5xl' },
      },
    },
    gradient: {
      true: {
        background:
          'linear-gradient(135deg, #00D4FF 0%, #FF6B9D 50%, #00D4FF 100%)',
        backgroundClip: 'text',
        WebkitBackgroundClip: 'text',
        WebkitTextFillColor: 'transparent',
        backgroundSize: '200% 200%',
        animation: 'gradient-shift',
        _after: {
          content: '""',
          position: 'absolute',
          top: '0',
          left: '0',
          right: '0',
          bottom: '0',
          background: 'inherit',
          backgroundClip: 'text',
          WebkitBackgroundClip: 'text',
          filter: 'blur(8px)',
          opacity: '0.7',
          zIndex: '-1',
        },
      },
      false: {
        color: 'text-primary',
      },
    },
  },
  defaultVariants: {
    size: 'large',
    gradient: false,
  },
});

export const heroSubtitleRecipe = defineRecipe({
  className: 'heroSubtitle',
  base: {
    color: 'text-secondary',
    marginBottom: '3xl',
    maxWidth: '48rem',
    marginX: 'auto',
    lineHeight: '1.5',
    animation: 'slide-up',
    animationDelay: '0.2s',
    animationFillMode: 'both',
  },
  variants: {
    size: {
      large: {
        fontSize: { base: 'xl', md: '2xl' },
      },
      medium: {
        fontSize: { base: 'lg', md: 'xl' },
      },
    },
  },
  defaultVariants: {
    size: 'large',
  },
});
