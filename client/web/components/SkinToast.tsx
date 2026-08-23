import React, { useEffect } from 'react';
import SnakeBuxIcon from './SnakeBuxIcon';

/**
 * "That worked", said briefly and out of the way.
 *
 * Getting a free skin has no confirmation step, which is the point — but an
 * action with no dialog and no visible result reads as a dead button. This is
 * the receipt.
 */
const SkinToast: React.FC<{
  message: string;
  coin?: boolean;
  onDone: () => void;
}> = ({ message, coin = false, onDone }) => {
  useEffect(() => {
    const timer = window.setTimeout(onDone, 3200);
    return () => window.clearTimeout(timer);
  }, [message, onDone]);

  return (
    <div className="skin-toast" role="status" data-testid="skin-toast">
      {coin ? <SnakeBuxIcon size={18} /> : <span className="skin-toast-tick" aria-hidden="true">✓</span>}
      <span>{message}</span>
    </div>
  );
};

export default SkinToast;
