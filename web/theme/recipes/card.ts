import { defineRecipe } from '@pandacss/dev';

export const cardRecipe = defineRecipe({
  className: 'card',
  base: {
    borderRadius: 'lg',
    transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
    position: 'relative',
    overflow: 'hidden',
  },
  variants: {
    variant: {
      default: {
        backgroundColor: 'surface',
        border: '1px solid',
        borderColor: 'border.subtle',
      },
      elevated: {
        backgroundColor: 'surface.raised',
        border: '1px solid transparent',
        background: `linear-gradient(token(colors.surface.raised), token(colors.surface.raised)) padding-box, 
                     token(gradients.card-border) border-box`,
        boxShadow: 'md',
        _before: {
          content: '""',
          position: 'absolute',
          top: '0',
          left: '0',
          right: '0',
          height: '1px',
          background: 'token(gradients.card-border)',
          opacity: '0',
          transition: 'all 0.3s ease',
          transform: 'translateX(-100%)',
        },
        _after: {
          content: '""',
          position: 'absolute',
          inset: '0',
          background:
            'radial-gradient(circle at 50% 50%, rgba(0, 212, 255, 0.03) 0%, transparent 70%)',
          opacity: '0',
          transition: 'opacity 0.3s ease',
        },
        _hover: {
          transform: 'translateY(-6px)',
          boxShadow:
            '0 0 35px token(colors.shadow.accent), 0 0 70px rgba(255, 107, 157, 0.15)',
          background: `linear-gradient(token(colors.surface.raised), token(colors.surface.raised)) padding-box, 
                       token(gradients.surface-elevated) border-box`,
          _before: {
            opacity: '1',
            transform: 'translateX(0)',
          },
          _after: {
            opacity: '1',
          },
        },
      },
      bordered: {
        backgroundColor: 'surface',
        border: '1px solid',
        borderColor: 'border.default',
        _hover: {
          borderColor: 'border.strong',
          transform: 'translateY(-2px)',
        },
      },
      ghost: {
        backgroundColor: 'transparent',
        border: 'none',
        _hover: {
          backgroundColor: 'surface',
        },
      },
    },
    padding: {
      none: { padding: '0' },
      sm: { padding: { base: 'sm', md: 'md' } },
      md: { padding: { base: 'md', md: 'lg' } },
      lg: { padding: { base: 'lg', md: 'xl' } },
      xl: { padding: { base: 'xl', md: '2xl' } },
    },
    size: {
      sm: { minHeight: '4rem' },
      md: { minHeight: '6rem' },
      lg: { minHeight: '8rem' },
      xl: { minHeight: '10rem' },
      auto: {},
    },
  },
  defaultVariants: {
    variant: 'default',
    padding: 'lg',
    size: 'auto',
  },
  compoundVariants: [
    {
      variant: 'elevated',
      padding: 'lg',
      css: {
        // Enhanced elevated card with large padding gets extra glow
        _hover: {
          boxShadow:
            '0 0 40px token(colors.shadow.accent), 0 0 80px rgba(255, 107, 157, 0.2)',
        },
      },
    },
    {
      variant: 'elevated',
      size: 'lg',
      css: {
        // Large elevated cards have stronger shadow
        boxShadow: 'lg',
        _hover: {
          boxShadow:
            '0 0 45px token(colors.shadow.accent), 0 0 90px rgba(255, 107, 157, 0.25)',
        },
      },
    },
  ],
});
