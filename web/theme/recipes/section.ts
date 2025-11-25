import { defineRecipe } from '@pandacss/dev';

export const sectionRecipe = defineRecipe({
  className: 'section',
  base: {
    paddingY: { base: '3xl', md: '4xl' },
    position: 'relative',
  },
  variants: {
    background: {
      default: {},
      surface: {
        bgColor: 'surface',
      },
      texture: {
        _before: {
          content: '""',
          position: 'absolute',
          top: '0',
          left: '0',
          right: '0',
          bottom: '0',
          background:
            'linear-gradient(135deg, rgba(26, 26, 26, 0.5) 0%, rgba(36, 36, 36, 0.3) 100%)',
          zIndex: '-1',
        },
        _after: {
          content: '""',
          position: 'absolute',
          top: '0',
          left: '0',
          right: '0',
          bottom: '0',
          backgroundImage:
            'radial-gradient(circle at 30% 70%, rgba(79, 195, 247, 0.02) 0%, transparent 60%)',
          zIndex: '-1',
        },
      },
    },
    spacing: {
      default: {
        paddingY: { base: '3xl', md: '4xl' },
      },
      large: {
        paddingY: { base: '4xl', md: '5xl' },
      },
      hero: {
        paddingTop: { base: '4xl', md: '6xl' },
        paddingBottom: { base: '4xl', md: '5xl' },
      },
    },
  },
  defaultVariants: {
    background: 'default',
    spacing: 'default',
  },
});
