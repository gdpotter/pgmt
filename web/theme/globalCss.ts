export const globalCss = {
  html: {
    scrollBehavior: 'smooth',
  },
  body: {
    minHeight: '100vh',
    display: 'flex',
    flexDirection: 'column',
    background: `
      radial-gradient(ellipse at top left, rgba(51, 103, 145, 0.02) 0%, transparent 50%),
      radial-gradient(ellipse at bottom right, rgba(79, 195, 247, 0.015) 0%, transparent 50%),
      #0a0a0a
    `,
    color: 'text-primary',
    fontFamily: 'body',
    fontSize: { base: 'sm', md: 'md' },
    lineHeight: '1.6',
    margin: '0',
    padding: '0',
    // Better text rendering on mobile
    WebkitFontSmoothing: 'antialiased',
    MozOsxFontSmoothing: 'grayscale',
    WebkitTextSizeAdjust: '100%',
  },
  // Light mode overrides
  '.light body': {
    background: `
      radial-gradient(ellipse at top left, rgba(51, 103, 145, 0.05) 0%, transparent 50%),
      radial-gradient(ellipse at bottom right, rgba(79, 195, 247, 0.03) 0%, transparent 50%),
      #ffffff
    `,
  },
  main: {
    flex: '1',
  },
  '::selection': {
    backgroundColor: 'rgba(79, 195, 247, 0.3)',
    color: 'white',
  },
  '::-webkit-scrollbar': {
    width: '10px',
    height: '10px',
  },
  '::-webkit-scrollbar-track': {
    background: 'surface',
  },
  '::-webkit-scrollbar-thumb': {
    background: 'border',
    borderRadius: '5px',
  },
  '::-webkit-scrollbar-thumb:hover': {
    background: 'text-muted',
  },

  // Keyframe animations
  '@keyframes fadeIn': {
    '0%': { opacity: '0', transform: 'translateY(20px)' },
    '100%': { opacity: '1', transform: 'translateY(0)' },
  },
  '@keyframes slideUp': {
    '0%': { opacity: '0', transform: 'translateY(40px)' },
    '100%': { opacity: '1', transform: 'translateY(0)' },
  },
  '@keyframes glowPulse': {
    '0%, 100%': { boxShadow: '0 0 20px rgba(79, 195, 247, 0.3)' },
    '50%': { boxShadow: '0 0 40px rgba(79, 195, 247, 0.6)' },
  },
  '@keyframes float': {
    '0%, 100%': { transform: 'translateY(0px)' },
    '50%': { transform: 'translateY(-10px)' },
  },
  '@keyframes gradientShift': {
    '0%, 100%': { backgroundPosition: '0% 50%' },
    '50%': { backgroundPosition: '100% 50%' },
  },
  '@keyframes shimmerSweep': {
    '0%': { transform: 'translateX(-100%)' },
    '100%': { transform: 'translateX(100%)' },
  },
  '@keyframes rainbowGlow': {
    '0%, 100%': {
      boxShadow:
        '0 0 20px rgba(0, 212, 255, 0.4), 0 0 40px rgba(0, 212, 255, 0.2)',
      borderColor: 'rgba(0, 212, 255, 0.3)',
    },
    '33%': {
      boxShadow:
        '0 0 20px rgba(255, 107, 157, 0.4), 0 0 40px rgba(255, 107, 157, 0.2)',
      borderColor: 'rgba(255, 107, 157, 0.3)',
    },
    '66%': {
      boxShadow:
        '0 0 20px rgba(79, 195, 247, 0.4), 0 0 40px rgba(79, 195, 247, 0.2)',
      borderColor: 'rgba(79, 195, 247, 0.3)',
    },
  },
};
