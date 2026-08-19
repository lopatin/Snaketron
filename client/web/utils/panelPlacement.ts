/**
 * Where the floating online-players panel goes, and whether it fits at all.
 *
 * The panel is a side column, so it lives in whichever margin the page's
 * centered content leaves free. That margin is not a constant: it changes with
 * the viewport, and it disappears entirely on a phone or behind a modal. Rather
 * than guess, the component measures and asks this.
 *
 * Pure so it can be tested on plain numbers — the repo's split for view logic.
 */

export type PanelSide = 'left' | 'right';

export interface PanelPlacementInput {
  viewportWidth: number;
  /** Left edge of the page's occupied content band, in viewport pixels. */
  contentLeft: number;
  /** Right edge of the same band. */
  contentRight: number;
  panelWidth: number;
  /** Breathing room required between the panel and both of its neighbours. */
  edgeGap: number;
  /** A modal owns the screen; nothing floating should compete with it. */
  modalOpen: boolean;
}

export interface PanelPlacement {
  side: PanelSide;
  /** False means the panel must start minimized rather than overlap content. */
  fits: boolean;
}

export function resolvePanelPlacement({
  viewportWidth,
  contentLeft,
  contentRight,
  panelWidth,
  edgeGap,
  modalOpen,
}: PanelPlacementInput): PanelPlacement {
  const leftGap = Math.max(0, contentLeft);
  const rightGap = Math.max(0, viewportWidth - contentRight);
  // Ties go right: that is where the chat dock and the rest of the app's
  // floating chrome already live, so the two read as one column of furniture.
  const side: PanelSide = rightGap >= leftGap ? 'right' : 'left';
  const available = Math.max(leftGap, rightGap);

  return {
    side,
    fits: !modalOpen && available >= panelWidth + edgeGap * 2,
  };
}

/**
 * The horizontal band the page's content actually occupies.
 *
 * Measures the children rather than `main` itself, because `main` is usually
 * full-width with its content centered inside — its own rect would say the
 * page fills the viewport and the panel never fits anywhere.
 */
export function measureContentBand(main: Element | null, viewportWidth: number): {
  contentLeft: number;
  contentRight: number;
} {
  const children = main ? Array.from(main.children) : [];
  let contentLeft = Number.POSITIVE_INFINITY;
  let contentRight = Number.NEGATIVE_INFINITY;

  for (const child of children) {
    const rect = child.getBoundingClientRect();
    if (rect.width === 0 && rect.height === 0) {
      continue;
    }
    contentLeft = Math.min(contentLeft, rect.left);
    contentRight = Math.max(contentRight, rect.right);
  }

  // Nothing measurable: assume the content fills the viewport, which makes the
  // panel start minimized instead of landing on top of something.
  if (!Number.isFinite(contentLeft) || !Number.isFinite(contentRight)) {
    return { contentLeft: 0, contentRight: viewportWidth };
  }
  return { contentLeft, contentRight };
}
