import { defineTokens } from '@pandacss/dev';

export const tokens = defineTokens({
  colors: {
    // Brand colors
    pgblue: {
      50: { value: '#e6f1fa' },
      100: { value: '#bdd9f1' },
      200: { value: '#94c1e8' },
      300: { value: '#6ba9df' },
      400: { value: '#4291d6' },
      500: { value: '#336791' }, // PostgreSQL blue
      600: { value: '#2a5577' },
      700: { value: '#21435d' },
      800: { value: '#183143' },
      900: { value: '#0f1f29' },
    },
    accent: {
      value: '#00D4FF', // Brighter cyan
    },
    secondary: {
      value: '#FF6B9D', // Pink secondary accent
    },
    // Base colors for dark theme (removed to avoid conflicts with semantic tokens)
    success: {
      value: '#4CAF50',
    },
    warning: {
      value: '#FF9800',
    },
    error: {
      value: '#F44336',
    },
  },
  fonts: {
    heading: { value: 'Space Grotesk, system-ui, -apple-system, sans-serif' },
    body: { value: 'Inter, system-ui, -apple-system, sans-serif' },
    mono: { value: 'JetBrains Mono, Menlo, monospace' },
  },
  fontSizes: {
    xs: { value: '0.75rem' },
    sm: { value: '0.875rem' },
    md: { value: '1rem' },
    lg: { value: '1.125rem' },
    xl: { value: '1.25rem' },
    '2xl': { value: '1.5rem' },
    '3xl': { value: '1.875rem' },
    '4xl': { value: '2.25rem' },
    '5xl': { value: '3rem' },
    '6xl': { value: '3.75rem' },
  },
  spacing: {
    xs: { value: '0.25rem' },
    sm: { value: '0.5rem' },
    md: { value: '1rem' },
    lg: { value: '1.5rem' },
    xl: { value: '2rem' },
    '2xl': { value: '3rem' },
    '3xl': { value: '4rem' },
    '4xl': { value: '6rem' },
    '5xl': { value: '8rem' },
    '6xl': { value: '10rem' },
    '7xl': { value: '12rem' },
    '8xl': { value: '16rem' },
  },
  sizes: {
    icon: {
      sm: { value: '1rem' },
      md: { value: '1.5rem' },
      lg: { value: '2rem' },
      xl: { value: '2.5rem' },
      '2xl': { value: '3rem' },
    },
    container: {
      sm: { value: '640px' },
      md: { value: '768px' },
      lg: { value: '1024px' },
      xl: { value: '1280px' },
      '2xl': { value: '1536px' },
      '3xl': { value: '1728px' },
      '4xl': { value: '1920px' },
      '5xl': { value: '2048px' },
      '6xl': { value: '2304px' },
      '7xl': { value: '2560px' },
    },
  },
  radii: {
    sm: { value: '0.25rem' },
    md: { value: '0.5rem' },
    lg: { value: '0.75rem' },
    xl: { value: '1rem' },
    full: { value: '9999px' },
  },
  shadows: {
    sm: { value: '0 1px 2px 0 rgba(0, 0, 0, 0.5)' },
    md: { value: '0 4px 6px -1px rgba(0, 0, 0, 0.5)' },
    lg: { value: '0 10px 15px -3px rgba(0, 0, 0, 0.5)' },
    xl: { value: '0 20px 25px -5px rgba(0, 0, 0, 0.5)' },
    glow: { value: '0 0 20px rgba(79, 195, 247, 0.3)' },
    'glow-lg': { value: '0 0 40px rgba(79, 195, 247, 0.4)' },
    'inner-glow': { value: 'inset 0 0 20px rgba(79, 195, 247, 0.1)' },
  },
  gradients: {
    'hero-bg': {
      value:
        'radial-gradient(ellipse at top, rgba(51, 103, 145, 0.15) 0%, rgba(10, 10, 10, 0.8) 50%, rgba(10, 10, 10, 1) 100%)',
    },
    'text-gradient': {
      value: 'linear-gradient(135deg, #00D4FF 0%, #FF6B9D 50%, #00D4FF 100%)',
    },
    'card-border': {
      value:
        'linear-gradient(135deg, rgba(0, 212, 255, 0.4) 0%, rgba(255, 107, 157, 0.2) 100%)',
    },
    'button-glow': {
      value: 'linear-gradient(135deg, #00D4FF 0%, #0099CC 100%)',
    },
    shimmer: {
      value:
        'linear-gradient(90deg, transparent 0%, rgba(0, 212, 255, 0.4) 50%, transparent 100%)',
    },
    'accent-glow': {
      value:
        'linear-gradient(135deg, rgba(0, 212, 255, 0.1) 0%, rgba(255, 107, 157, 0.05) 100%)',
    },
    // Icon gradients
    'pg-blue': {
      value: 'linear-gradient(135deg, #336791 0%, #4FCDF7 100%)',
    },
    'green-teal': {
      value: 'linear-gradient(135deg, #059669 0%, #10B981 100%)',
    },
    'purple-violet': {
      value: 'linear-gradient(135deg, #8B5CF6 0%, #A855F7 100%)',
    },
    'red-orange': {
      value: 'linear-gradient(135deg, #DC2626 0%, #F87171 100%)',
    },
    'cyan-blue': {
      value: 'linear-gradient(135deg, #0891B2 0%, #06B6D4 100%)',
    },
    'emerald-green': {
      value: 'linear-gradient(135deg, #059669 0%, #34D399 100%)',
    },
    'sky-cyan': {
      value: 'linear-gradient(135deg, #0ea5e9 0%, #06b6d4 100%)',
    },
    'violet-purple': {
      value: 'linear-gradient(135deg, #7c3aed 0%, #a855f7 100%)',
    },
    'blue-cyan': {
      value: 'linear-gradient(135deg, #3b82f6 0%, #06b6d4 100%)',
    },
    'purple-pink': {
      value: 'linear-gradient(135deg, #a855f7 0%, #ec4899 100%)',
    },
    // Surface gradients
    'surface-elevated': {
      value:
        'linear-gradient(135deg, rgba(0, 212, 255, 0.15) 0%, rgba(255, 107, 157, 0.1) 100%)',
    },
    'surface-texture': {
      value:
        'radial-gradient(ellipse at top left, rgba(51, 103, 145, 0.05) 0%, transparent 50%), radial-gradient(ellipse at bottom right, rgba(79, 195, 247, 0.03) 0%, transparent 50%)',
    },
  },
  animations: {
    'fade-in': { value: 'fadeIn 0.6s ease-out' },
    'slide-up': { value: 'slideUp 0.8s ease-out' },
    'glow-pulse': { value: 'glowPulse 2s ease-in-out infinite' },
    float: { value: 'float 3s ease-in-out infinite' },
    'gradient-shift': { value: 'gradientShift 8s ease-in-out infinite' },
    'shimmer-sweep': { value: 'shimmerSweep 3s ease-in-out infinite' },
    'rainbow-glow': { value: 'rainbowGlow 4s ease-in-out infinite' },
  },
});
