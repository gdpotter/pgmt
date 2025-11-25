import { defineSemanticTokens } from '@pandacss/dev';

export const semanticTokens = defineSemanticTokens({
  colors: {
    primary: {
      value: {
        base: '{colors.pgblue.500}',
        _light: '{colors.pgblue.600}',
      },
    },
    'primary.hover': {
      value: {
        base: '{colors.pgblue.400}',
        _light: '{colors.pgblue.500}',
      },
    },
    // Background colors - always use actual color values, not self-references
    background: {
      value: {
        base: '#0a0a0a',
        _light: '#ffffff',
      },
    },
    surface: {
      value: {
        base: '#1a1a1a',
        _light: '#f8f9fa',
      },
    },
    'surface.hover': {
      value: {
        base: '#242424',
        _light: '#f0f0f0',
      },
    },
    'surface.raised': {
      value: {
        base: '#242424',
        _light: '#ffffff',
      },
    },
    // Text colors - explicit values
    'text.primary': {
      value: {
        base: '#ffffff',
        _light: '#1a1a1a',
      },
    },
    'text.secondary': {
      value: {
        base: '#b0b0b0',
        _light: '#4b5563',
      },
    },
    'text.muted': {
      value: {
        base: '#808080',
        _light: '#9ca3af',
      },
    },
    // Border colors
    border: {
      value: {
        base: '#2a2a2a',
        _light: '#e5e7eb',
      },
    },
    'border.subtle': {
      value: {
        base: 'rgba(255, 255, 255, 0.05)',
        _light: '#f3f4f6',
      },
    },
    'border.default': {
      value: {
        base: 'rgba(255, 255, 255, 0.1)',
        _light: '#d1d5db',
      },
    },
    'border.strong': {
      value: {
        base: 'rgba(255, 255, 255, 0.2)',
        _light: '#9ca3af',
      },
    },
    'border.accent': {
      value: {
        base: 'rgba(0, 212, 255, 0.3)',
        _light: 'rgba(0, 153, 204, 0.4)',
      },
    },
    // Accent colors (keep bright in both modes)
    accent: {
      value: {
        base: '#00D4FF',
        _light: '#0099cc',
      },
    },
    'accent.hover': {
      value: {
        base: '#00a8d4',
        _light: '#007aa3',
      },
    },
    secondary: {
      value: {
        base: '#FF6B9D',
        _light: '#e6547d',
      },
    },
    // Shadow colors
    'shadow.default': {
      value: {
        base: 'rgba(0, 0, 0, 0.5)',
        _light: 'rgba(0, 0, 0, 0.1)',
      },
    },
    'shadow.accent': {
      value: {
        base: 'rgba(79, 195, 247, 0.3)',
        _light: 'rgba(79, 195, 247, 0.2)',
      },
    },
    'shadow.glow': {
      value: {
        base: 'rgba(0, 212, 255, 0.4)',
        _light: 'rgba(0, 212, 255, 0.3)',
      },
    },
  },
});
