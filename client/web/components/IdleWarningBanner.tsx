import React, { useId, useRef } from 'react';
import type { IdleWarningPresentation } from '../utils/idlePresentation';

export interface IdleWarningBannerProps {
  warning: IdleWarningPresentation;
  onConfirmActivity: () => void;
}

const IdleWarningBanner: React.FC<IdleWarningBannerProps> = ({
  warning,
  onConfirmActivity,
}) => {
  const titleId = useId();
  const descriptionId = useId();
  // This sentence is mounted once per authoritative deadline (GameArena keys
  // the component by deadlineTick). The changing visual timer stays outside
  // the live region so assistive technology is not interrupted every tick.
  const announcedSecondsRef = useRef(warning.remainingSeconds);

  return (
    <section
      className={`game-idle-warning${warning.isUrgent ? ' is-urgent' : ''}`}
      style={{
        '--game-idle-progress': warning.progress,
      } as React.CSSProperties}
      aria-labelledby={titleId}
      aria-describedby={descriptionId}
      data-testid="idle-warning"
      data-deadline-tick={warning.deadlineTick}
    >
      <span className="game-idle-warning__flag" aria-hidden="true">!</span>

      <span className="game-idle-warning__copy">
        <strong id={titleId}>Inactivity warning</strong>
        <span id={descriptionId}>Make a move to stay in the match.</span>
      </span>

      <time
        className="game-idle-warning__countdown"
        aria-label={`${warning.remainingSeconds} seconds remaining`}
        data-testid="idle-warning-countdown"
      >
        {warning.remainingSeconds}<span aria-hidden="true">s</span>
      </time>

      <button
        type="button"
        className="game-idle-warning__action"
        onClick={onConfirmActivity}
        data-testid="idle-confirm-activity"
      >
        I&rsquo;m here
      </button>

      <span className="game-idle-warning__rail" aria-hidden="true">
        <span />
      </span>

      <span className="sr-only" role="alert">
        Inactivity warning. Make a move or use the I&rsquo;m here button within{' '}
        {announcedSecondsRef.current} seconds to stay in the match.
      </span>
    </section>
  );
};

export default IdleWarningBanner;
