import { useCallback, useRef } from 'react';
import { useNavigate } from 'react-router-dom';

export function useNavigateWithDebounce(delay = 300) {
  const navigate = useNavigate();
  const timeoutRef = useRef(null);
  const lastNavigationRef = useRef(null);

  const navigateDebounced = useCallback((to, options) => {
    // If trying to navigate to the same path, ignore
    if (lastNavigationRef.current === to) {
      return;
    }

    // Clear any pending navigation
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
    }

    // Set the navigation with a small delay to prevent flooding
    timeoutRef.current = setTimeout(() => {
      lastNavigationRef.current = to;
      navigate(to, options);
      
      // Reset after navigation
      setTimeout(() => {
        lastNavigationRef.current = null;
      }, delay);
    }, 50); // Small delay to prevent immediate re-navigation
  }, [navigate, delay]);

  return navigateDebounced;
}