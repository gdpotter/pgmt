import { defineRecipe } from '@pandacss/dev';

export const iconRecipe = defineRecipe({
  className: 'icon',
  base: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    borderRadius: 'lg',
    flexShrink: '0',
    color: 'white',
  },
  variants: {
    size: {
      sm: {
        width: '2rem',
        height: '2rem',
        '& svg': { width: '1rem', height: '1rem' },
      },
      md: {
        width: '2.5rem',
        height: '2.5rem',
        '& svg': { width: '1.25rem', height: '1.25rem' },
      },
      lg: {
        width: '3rem',
        height: '3rem',
        '& svg': { width: '1.5rem', height: '1.5rem' },
      },
      xl: {
        width: '3.5rem',
        height: '3.5rem',
        '& svg': { width: '1.75rem', height: '1.75rem' },
      },
      '2xl': {
        width: '4rem',
        height: '4rem',
        '& svg': { width: '2rem', height: '2rem' },
      },
    },
    gradient: {
      'pg-blue': {
        background: 'token(gradients.pg-blue)',
        boxShadow: '0 0 20px rgba(51, 103, 145, 0.3)',
      },
      'green-teal': {
        background: 'token(gradients.green-teal)',
        boxShadow: '0 0 20px rgba(5, 150, 105, 0.3)',
      },
      'purple-violet': {
        background: 'token(gradients.purple-violet)',
        boxShadow: '0 0 20px rgba(139, 92, 246, 0.3)',
      },
      'red-orange': {
        background: 'token(gradients.red-orange)',
        boxShadow: '0 0 20px rgba(220, 38, 38, 0.3)',
      },
      'cyan-blue': {
        background: 'token(gradients.cyan-blue)',
        boxShadow: '0 0 20px rgba(8, 145, 178, 0.3)',
      },
      'emerald-green': {
        background: 'token(gradients.emerald-green)',
        boxShadow: '0 0 20px rgba(5, 150, 105, 0.3)',
      },
      'sky-cyan': {
        background: 'token(gradients.sky-cyan)',
        boxShadow: '0 0 20px rgba(14, 165, 233, 0.3)',
      },
      'violet-purple': {
        background: 'token(gradients.violet-purple)',
        boxShadow: '0 0 20px rgba(124, 58, 237, 0.3)',
      },
      'blue-cyan': {
        background: 'token(gradients.blue-cyan)',
        boxShadow: '0 0 20px rgba(59, 130, 246, 0.3)',
      },
      'purple-pink': {
        background: 'token(gradients.purple-pink)',
        boxShadow: '0 0 20px rgba(168, 85, 247, 0.3)',
      },
      primary: {
        background: 'token(gradients.button-glow)',
        boxShadow: '0 0 20px token(colors.shadow.accent)',
      },
      accent: {
        backgroundColor: 'accent',
        boxShadow: '0 0 20px token(colors.shadow.glow)',
      },
    },
    variant: {
      solid: {},
      ghost: {
        backgroundColor: 'transparent',
        color: 'text.primary',
        boxShadow: 'none',
        border: '1px solid',
        borderColor: 'border.default',
      },
      outline: {
        backgroundColor: 'transparent',
        color: 'text.primary',
        boxShadow: 'none',
        border: '2px solid',
        borderColor: 'current',
      },
    },
  },
  defaultVariants: {
    size: 'md',
    gradient: 'pg-blue',
    variant: 'solid',
  },
});
