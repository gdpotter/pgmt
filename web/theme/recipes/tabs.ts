import { defineRecipe } from '@pandacss/dev';

export const tabsContainerRecipe = defineRecipe({
  className: 'tabs-container',
  base: {},
  variants: {
    orientation: {
      horizontal: { width: '100%' },
      vertical: {
        display: 'flex',
        flexDirection: 'row',
        gap: 'lg',
      },
    },
  },
  defaultVariants: {
    orientation: 'horizontal',
  },
});

export const tabsListRecipe = defineRecipe({
  className: 'tabs-list',
  base: {
    display: 'grid',
    gap: 'md',
    marginBottom: 'xl',
  },
  variants: {
    columns: {
      1: { gridTemplateColumns: '1fr' },
      2: {
        gridTemplateColumns: 'repeat(2, 1fr)',
        '@media (max-width: 640px)': { gridTemplateColumns: '1fr' },
      },
      3: {
        gridTemplateColumns: 'repeat(3, 1fr)',
        '@media (max-width: 768px)': { gridTemplateColumns: '1fr' },
      },
      4: {
        gridTemplateColumns: 'repeat(4, 1fr)',
        '@media (max-width: 768px)': { gridTemplateColumns: 'repeat(2, 1fr)' },
        '@media (max-width: 640px)': { gridTemplateColumns: '1fr' },
      },
    },
    variant: {
      default: {},
      pills: { gap: 'sm' },
      underline: {
        gap: '0',
        borderBottom: '1px solid',
        borderColor: 'border.default',
      },
    },
  },
  defaultVariants: {
    columns: 4,
    variant: 'default',
  },
});

export const tabRecipe = defineRecipe({
  className: 'tab',
  base: {
    background: 'transparent',
    border: 'none',
    padding: 'lg',
    cursor: 'pointer',
    borderBottom: '2px solid transparent',
    transition: 'all 0.3s ease',
    position: 'relative',
    overflow: 'hidden',
    width: '100%',
    textAlign: 'center',
    fontFamily: 'inherit',
    fontSize: 'sm',
    fontWeight: '600',
    color: 'text.secondary',
    _focusVisible: {
      outline: '2px solid',
      outlineColor: 'accent',
      outlineOffset: '2px',
    },
    _hover: {
      background: 'token(gradients.accent-glow)',
      borderRadius: 'md',
    },
    _before: {
      content: '""',
      position: 'absolute',
      bottom: '0',
      left: '0',
      width: '0',
      height: '2px',
      background: 'token(gradients.text-gradient)',
      transition: 'width 0.3s ease',
    },
    '&:hover::before': {
      width: '100%',
    },
    // Active state using data attribute
    '&[data-state="active"]': {
      background: 'token(gradients.accent-glow)',
      borderRadius: 'md',
      color: 'white',
      textShadow: '0 0 10px rgba(0, 212, 255, 0.3)',
      _before: {
        width: '100%',
      },
    },
  },
  variants: {
    variant: {
      default: {},
      pills: {
        borderRadius: 'full',
        padding: { base: 'sm md', md: 'md lg' },
        borderBottom: 'none',
        _before: { display: 'none' },
        _hover: {
          backgroundColor: 'surface',
        },
        '&[data-state="active"]': {
          background: 'token(gradients.accent-glow)',
          color: 'white',
          textShadow: '0 0 10px rgba(0, 212, 255, 0.3)',
        },
      },
      underline: {
        borderRadius: 'none',
        padding: { base: 'sm md', md: 'md lg' },
        borderBottom: '2px solid transparent',
        _hover: {
          background: 'transparent',
          borderBottomColor: 'border.accent',
        },
        '&[data-state="active"]': {
          background: 'transparent',
          borderBottomColor: 'accent',
          color: 'white',
          textShadow: '0 0 10px rgba(0, 212, 255, 0.3)',
        },
      },
    },
  },
  defaultVariants: {
    variant: 'default',
  },
});

export const tabPanelRecipe = defineRecipe({
  className: 'tab-panel',
  base: {
    textAlign: 'left',
    animation: 'fadeIn 0.3s ease-out',
    display: 'none',
    // Active state using data attribute
    '&[data-state="active"]': {
      display: 'block',
    },
  },
});
