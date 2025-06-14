import React, { useEffect, useState } from 'react';
import { Routes, useLocation } from 'react-router-dom';

function AnimatedRoutes({ children }) {
  const location = useLocation();
  const [displayLocation, setDisplayLocation] = useState(location);
  const [isAnimating, setIsAnimating] = useState(false);

  useEffect(() => {
    if (location.pathname !== displayLocation.pathname) {
      // Start fade out
      setIsAnimating(true);
      
      // After fade out, update location and fade in
      const timer = setTimeout(() => {
        setDisplayLocation(location);
        // Start fade in after a tiny delay
        setTimeout(() => {
          setIsAnimating(false);
        }, 10);
      }, 100);

      return () => clearTimeout(timer);
    }
  }, [location, displayLocation]);

  return (
    <div className={`flex-1 transition-opacity duration-100 ease-in-out ${
      isAnimating ? 'opacity-0' : 'opacity-100'
    }`}>
      <Routes location={displayLocation}>
        {children}
      </Routes>
    </div>
  );
}

export default AnimatedRoutes;