import { defineRecipe } from '@pandacss/dev';

export const badgeRecipe = defineRecipe({
  className: 'badge',
  base: {
    display: 'inline-flex',
    alignItems: 'center',
    justifyContent: 'center',
    fontWeight: '500',
    fontSize: 'xs',
    borderRadius: 'sm',
    padding: 'sm',
    textTransform: 'uppercase',
    letterSpacing: '0.05em',
    whiteSpace: 'nowrap',
  },
  variants: {
    variant: {
      default: {
        backgroundColor: 'surface.raised',
        color: 'text.primary',
        border: '1px solid',
        borderColor: 'border.default',
      },
      primary: {
        backgroundColor: 'primary',
        color: 'white',
      },
      secondary: {
        backgroundColor: 'secondary',
        color: 'white',
      },
      accent: {
        backgroundColor: 'accent',
        color: 'white',
      },
      success: {
        backgroundColor: 'rgba(34, 197, 94, 0.1)',
        color: '#10B981',
        border: '1px solid rgba(34, 197, 94, 0.3)',
      },
      warning: {
        backgroundColor: 'rgba(245, 158, 11, 0.1)',
        color: '#F59E0B',
        border: '1px solid rgba(245, 158, 11, 0.3)',
      },
      error: {
        backgroundColor: 'rgba(239, 68, 68, 0.1)',
        color: '#EF4444',
        border: '1px solid rgba(239, 68, 68, 0.3)',
      },
      ghost: {
        backgroundColor: 'transparent',
        color: 'text.secondary',
        border: '1px solid',
        borderColor: 'border.subtle',
      },
      outline: {
        backgroundColor: 'transparent',
        color: 'text.primary',
        border: '1px solid',
        borderColor: 'border.default',
      },
      // Technology-specific variants
      pgblue: {
        backgroundColor: 'rgba(51, 103, 145, 0.1)',
        color: 'pgblue.300',
        border: '1px solid rgba(51, 103, 145, 0.3)',
      },
      green: {
        backgroundColor: 'rgba(5, 150, 105, 0.1)',
        color: '#10B981',
        border: '1px solid rgba(5, 150, 105, 0.3)',
      },
      purple: {
        backgroundColor: 'rgba(124, 58, 237, 0.1)',
        color: '#A855F7',
        border: '1px solid rgba(124, 58, 237, 0.3)',
      },
      red: {
        backgroundColor: 'rgba(220, 38, 38, 0.1)',
        color: '#F87171',
        border: '1px solid rgba(220, 38, 38, 0.3)',
      },
      cyan: {
        backgroundColor: 'rgba(8, 145, 178, 0.1)',
        color: '#06B6D4',
        border: '1px solid rgba(8, 145, 178, 0.3)',
      },
    },
    size: {
      xs: {
        fontSize: '2xs',
        padding: 'xs sm',
      },
      sm: {
        fontSize: 'xs',
        padding: 'sm',
      },
      md: {
        fontSize: 'sm',
        padding: 'sm md',
      },
      lg: {
        fontSize: 'md',
        padding: 'md lg',
      },
    },
  },
  defaultVariants: {
    variant: 'default',
    size: 'sm',
  },
});
