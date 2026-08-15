import React from 'react';
import { Navigate } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import Spinner from './Spinner';

const IS_EMBEDDED_BUILD = process.env.ITCH_BUILD === 'true'
  || process.env.CRAZYGAMES_BUILD === 'true';

export const AdminRoute: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const { user, loading } = useAuth();

  if (IS_EMBEDDED_BUILD) {
    return <Navigate to="/" replace />;
  }

  if (loading) {
    return (
      <main className="admin-route-loading" aria-busy="true">
        <Spinner className="w-8 h-8" />
        <span>Verifying operator access…</span>
      </main>
    );
  }

  if (!user?.isAdmin) {
    return <Navigate to="/" replace />;
  }

  return <>{children}</>;
};
