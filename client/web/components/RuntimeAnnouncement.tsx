import React from 'react';
import { useRuntimeConfig } from '../contexts/RuntimeConfigContext';

export const RuntimeAnnouncement: React.FC = () => {
  const { config } = useRuntimeConfig();
  const message = config.announcement.message.trim();

  if (!config.announcement.enabled || !message) {
    return null;
  }

  return (
    <aside className="runtime-announcement" aria-label="Service announcement">
      <span className="runtime-announcement-pip" aria-hidden="true" />
      <strong>Arena notice</strong>
      <span>{message}</span>
    </aside>
  );
};
