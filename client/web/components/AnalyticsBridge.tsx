import React, { useEffect } from 'react';
import { useAuth } from '../contexts/AuthContext';
import { useInputSurface } from '../hooks/useInputSurface';
import { analytics } from '../services/analytics';

/**
 * Carries session-level context into GameAnalytics, and enforces the one
 * exclusion that can only be known after sign-in.
 *
 * Renders nothing. It is a bridge in the same sense as `CrazyGamesBridge`:
 * React owns the state, a plain service owns the SDK, and this is the only
 * place the two meet.
 */
export const AnalyticsBridge: React.FC = () => {
  const { user } = useAuth();
  const inputSurface = useInputSurface();

  useEffect(() => {
    if (!user) {
      return;
    }

    // An operator of this deployment is never counted. This can only be known
    // once `/auth/me` resolves — potentially after the session has already
    // opened — so the service both silences the live SDK and remembers the
    // choice for every later load in this browser.
    if (user.isAdmin) {
      analytics.excludeOperator();
      return;
    }

    analytics.setAccountType(user.isGuest ? 'guest' : 'registered');
    analytics.setPlayerId(String(user.id));
  }, [user]);

  useEffect(() => {
    analytics.setInputSurface(inputSurface === 'touch' ? 'touch' : 'keyboard');
  }, [inputSurface]);

  return null;
};
