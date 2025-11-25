import { defineRecipe } from '@pandacss/dev';

export const featureIconRecipe = defineRecipe({
  className: 'featureIcon',
  base: {
    width: '3rem',
    height: '3rem',
    borderRadius: 'lg',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    color: 'white',
    flexShrink: '0',
    position: 'relative',
    overflow: 'hidden',
    backgroundSize: '200% 200%',
    animation: 'gradient-shift',
    transition: 'all 0.3s ease',
    _before: {
      content: '""',
      position: 'absolute',
      top: '0',
      left: '0',
      right: '0',
      bottom: '0',
      background: 'inherit',
      filter: 'blur(6px)',
      opacity: '0',
      zIndex: '-1',
      transition: 'opacity 0.3s ease',
    },
    _hover: {
      transform: 'translateY(-3px) scale(1.05)',
      _before: {
        opacity: '0.8',
      },
    },
  },
  variants: {
    gradient: {
      'blue-cyan': {
        background:
          'linear-gradient(135deg, #00D4FF 0%, #FF6B9D 50%, #00D4FF 100%)',
        boxShadow: '0 0 20px rgba(0, 212, 255, 0.2)',
        _hover: {
          boxShadow:
            '0 0 30px rgba(0, 212, 255, 0.4), 0 0 60px rgba(255, 107, 157, 0.3)',
        },
      },
      pgblue: {
        background: 'linear-gradient(135deg, #336791 0%, #4FCDF7 100%)',
        boxShadow: '0 0 20px rgba(51, 103, 145, 0.3)',
      },
      'green-teal': {
        background: 'linear-gradient(135deg, #059669 0%, #10B981 100%)',
        boxShadow: '0 0 20px rgba(5, 150, 105, 0.3)',
      },
      'purple-pink': {
        background: 'linear-gradient(135deg, #7C3AED 0%, #A855F7 100%)',
        boxShadow: '0 0 20px rgba(124, 58, 237, 0.3)',
      },
      'red-orange': {
        background: 'linear-gradient(135deg, #DC2626 0%, #F87171 100%)',
        boxShadow: '0 0 20px rgba(220, 38, 38, 0.3)',
      },
      'cyan-blue': {
        background: 'linear-gradient(135deg, #0891B2 0%, #06B6D4 100%)',
        boxShadow: '0 0 20px rgba(8, 145, 178, 0.3)',
      },
      'emerald-green': {
        background: 'linear-gradient(135deg, #059669 0%, #34D399 100%)',
        boxShadow: '0 0 20px rgba(5, 150, 105, 0.3)',
      },
      'sky-cyan': {
        background: 'linear-gradient(135deg, #0ea5e9 0%, #06b6d4 100%)',
        boxShadow: '0 0 30px rgba(14, 165, 233, 0.3)',
      },
      'violet-purple': {
        background: 'linear-gradient(135deg, #8b5cf6 0%, #a855f7 100%)',
        boxShadow: '0 0 30px rgba(139, 92, 246, 0.3)',
      },
      'emerald-teal': {
        background: 'linear-gradient(135deg, #10b981 0%, #059669 100%)',
        boxShadow: '0 0 30px rgba(16, 185, 129, 0.3)',
      },
    },
    size: {
      small: {
        width: '2.5rem',
        height: '2.5rem',
      },
      medium: {
        width: '3rem',
        height: '3rem',
      },
      large: {
        width: '60px',
        height: '60px',
      },
    },
  },
  defaultVariants: {
    gradient: 'blue-cyan',
    size: 'medium',
  },
});
