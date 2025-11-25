import { definePattern } from '@pandacss/dev';
import type { SystemStyleObject } from '../styled-system/types';

export const heroPattern = definePattern({
  description: 'A hero section pattern with consistent spacing and alignment',
  properties: {
    background: { type: 'enum', value: ['default', 'gradient'] },
    textAlign: { type: 'enum', value: ['left', 'center', 'right'] },
    padding: { type: 'enum', value: ['sm', 'md', 'lg', 'xl'] },
  },
  defaultValues: {
    background: 'default',
    textAlign: 'center',
    padding: 'lg',
  },
  transform(props) {
    const { background, textAlign, padding } = props;

    const paddingMap: Record<string, SystemStyleObject> = {
      sm: {
        paddingTop: { base: 'xl', md: '2xl' },
        paddingBottom: { base: 'lg', md: 'xl' },
      },
      md: {
        paddingTop: { base: '2xl', md: '3xl' },
        paddingBottom: { base: 'xl', md: '2xl' },
      },
      lg: {
        paddingTop: { base: '3xl', md: '4xl' },
        paddingBottom: { base: '2xl', md: '3xl' },
      },
      xl: {
        paddingTop: { base: '4xl', md: '6xl' },
        paddingBottom: { base: '4xl', md: '5xl' },
      },
    };

    return {
      textAlign,
      position: 'relative',
      ...paddingMap[padding as keyof typeof paddingMap],
      ...(background === 'gradient' && {
        background: 'token(gradients.hero-bg)',
        overflow: 'hidden',
        _before: {
          content: '""',
          position: 'absolute',
          top: '-50%',
          left: '-50%',
          width: '200%',
          height: '200%',
          background:
            'radial-gradient(circle, rgba(79, 195, 247, 0.05) 0%, transparent 70%)',
          animation: 'float',
          pointerEvents: 'none',
        },
      }),
    };
  },
});

export const featureGridPattern = definePattern({
  description: 'A responsive grid pattern for feature cards',
  properties: {
    columns: { type: 'number' },
    gap: { type: 'enum', value: ['sm', 'md', 'lg', 'xl'] },
    minItemWidth: { type: 'string' },
  },
  defaultValues: {
    columns: 3,
    gap: 'xl',
    minItemWidth: '280px',
  },
  transform(props) {
    const { columns, gap, minItemWidth } = props;

    const gapMap: Record<string, string> = {
      sm: 'md',
      md: 'lg',
      lg: 'xl',
      xl: '2xl',
    };

    return {
      display: 'grid',
      gridTemplateColumns: {
        base: '1fr',
        md:
          columns === 2
            ? 'repeat(2, 1fr)'
            : columns >= 3
              ? `repeat(auto-fit, minmax(${minItemWidth}, 1fr))`
              : '1fr',
        lg: `repeat(${columns}, 1fr)`,
      },
      gap: gapMap[gap as keyof typeof gapMap],
      alignItems: 'stretch',
    };
  },
});

export const sectionWrapperPattern = definePattern({
  description:
    'A section wrapper with consistent spacing and background options',
  properties: {
    background: { type: 'enum', value: ['default', 'surface', 'texture'] },
    padding: { type: 'enum', value: ['sm', 'md', 'lg', 'xl'] },
    maxWidth: { type: 'enum', value: ['4xl', '5xl', '6xl', '7xl', 'full'] },
  },
  defaultValues: {
    background: 'default',
    padding: 'lg',
    maxWidth: '7xl',
  },
  transform(props) {
    const { background, padding, maxWidth } = props;

    const paddingMap: Record<string, SystemStyleObject> = {
      sm: { paddingY: { base: 'xl', md: '2xl' } },
      md: { paddingY: { base: '2xl', md: '3xl' } },
      lg: { paddingY: { base: '3xl', md: '4xl' } },
      xl: { paddingY: { base: '4xl', md: '5xl' } },
    };

    const backgroundStyles: Record<string, SystemStyleObject> = {
      default: {},
      surface: { backgroundColor: 'surface' },
      texture: {
        background: 'token(gradients.surface-texture)',
        backgroundColor: 'background',
      },
    };

    return {
      ...paddingMap[padding as keyof typeof paddingMap],
      ...backgroundStyles[background as keyof typeof backgroundStyles],
      '& .section-container': {
        maxWidth:
          maxWidth === 'full' ? '100%' : `token(sizes.container.${maxWidth})`,
        marginX: 'auto',
        paddingX: { base: 'lg', md: 'xl' },
      },
    };
  },
});

export const ctaGroupPattern = definePattern({
  description: 'A call-to-action button group with consistent spacing',
  properties: {
    direction: { type: 'enum', value: ['row', 'column'] },
    align: { type: 'enum', value: ['start', 'center', 'end'] },
    gap: { type: 'enum', value: ['sm', 'md', 'lg'] },
    wrap: { type: 'boolean' },
  },
  defaultValues: {
    direction: 'row',
    align: 'center',
    gap: 'lg',
    wrap: true,
  },
  transform(props) {
    const { direction, align, gap, wrap } = props;

    const alignMap: Record<string, string> = {
      start: 'flex-start',
      center: 'center',
      end: 'flex-end',
    };

    return {
      display: 'flex',
      flexDirection: direction,
      justifyContent: alignMap[align as keyof typeof alignMap],
      alignItems:
        direction === 'row'
          ? 'center'
          : alignMap[align as keyof typeof alignMap],
      gap,
      ...(wrap && { flexWrap: 'wrap' }),
    };
  },
});

export const statsPattern = definePattern({
  description: 'A statistics display pattern',
  properties: {
    columns: { type: 'number' },
    gap: { type: 'enum', value: ['sm', 'md', 'lg', 'xl'] },
    align: { type: 'enum', value: ['start', 'center', 'end'] },
  },
  defaultValues: {
    columns: 3,
    gap: 'xl',
    align: 'center',
  },
  transform(props) {
    const { gap, align } = props;

    const alignMap: Record<string, string> = {
      start: 'flex-start',
      center: 'center',
      end: 'flex-end',
    };

    return {
      display: 'flex',
      justifyContent: alignMap[align as keyof typeof alignMap],
      gap: { base: 'md', sm: 'lg', md: gap },
      flexWrap: 'wrap',
      '& .stat-item': {
        textAlign: align,
        animation: 'fadeIn',
        animationDelay: '0.6s',
        animationFillMode: 'both',
      },
      '& .stat-number': {
        fontSize: { base: '2xl', md: '3xl' },
        fontWeight: '700',
        color: 'pgblue.400',
        display: 'block',
      },
      '& .stat-label': {
        fontSize: 'sm',
        color: 'text.muted',
        textTransform: 'uppercase',
        letterSpacing: '0.05em',
      },
    };
  },
});
