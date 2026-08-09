import React, { useEffect, useState } from 'react';
import { Routes, useLocation } from 'react-router-dom';
import { AnimatedRoutesProps } from '../types';
import {
  isGameRoutePath,
  shouldSwapRouteImmediately,
} from '../utils/routeTransitions';

function AnimatedRoutes({ children }: AnimatedRoutesProps) {
  const location = useLocation();
  const [displayLocation, setDisplayLocation] = useState(location);
  const [isAnimating, setIsAnimating] = useState(false);
  const swapImmediately = shouldSwapRouteImmediately(
    displayLocation.pathname,
    location.pathname,
  );
  const renderedLocation = swapImmediately ? location : displayLocation;

  useEffect(() => {
    if (location.pathname !== displayLocation.pathname) {
      if (shouldSwapRouteImmediately(displayLocation.pathname, location.pathname)) {
        setDisplayLocation(location);
        setIsAnimating(false);
        return;
      }

      // Start fade out
      setIsAnimating(true);
      
      // Check if we're navigating to a game screen
      const isGameRoute = isGameRoutePath(location.pathname);
      const fadeOutDuration = isGameRoute ? 300 : 100;
      
      // After fade out, update location and fade in
      const timer = setTimeout(() => {
        setDisplayLocation(location);
        // Start fade in after a tiny delay
        setTimeout(() => {
          setIsAnimating(false);
        }, 10);
      }, fadeOutDuration);

      return () => clearTimeout(timer);
    }
  }, [location, displayLocation]);

  // Use longer transition for game routes
  const isGameRoute = isGameRoutePath(renderedLocation.pathname);
  const transitionDuration = isGameRoute ? 'duration-300' : 'duration-100';

  return (
    <div className={`relative flex-1 transition-opacity ${transitionDuration} ease-in-out ${
      isAnimating ? 'opacity-0' : 'opacity-100'
    }`}>
      <Routes location={renderedLocation}>
        {children}
      </Routes>
    </div>
  );
}

export default AnimatedRoutes;
