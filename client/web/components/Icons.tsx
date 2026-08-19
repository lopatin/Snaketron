import React from 'react';

export const CheckIcon = ({ className = '' }) => (
  <svg 
    className={className} 
    fill="none" 
    viewBox="0 0 24 24" 
    stroke="currentColor"
    strokeWidth={2}
  >
    <path 
      strokeLinecap="round" 
      strokeLinejoin="round" 
      d="M5 13l4 4L19 7" 
    />
  </svg>
);

export const XIcon = ({ className = '' }) => (
  <svg
    className={className}
    fill="none"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth={2}
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M6 18L18 6M6 6l12 12"
    />
  </svg>
);

export const UserIcon = ({ className = '' }) => (
  <svg
    className={className}
    fill="none"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth={2}
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"
    />
  </svg>
);

export const UserPlusIcon = ({ className = '' }) => (
  <svg
    className={className}
    aria-hidden="true"
    fill="none"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth={1.8}
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M15 19a6 6 0 00-12 0m6-8a4 4 0 100-8 4 4 0 000 8zm9-4v6m3-3h-6"
    />
  </svg>
);

export const HistoryIcon = ({ className = '' }) => (
  <svg
    className={className}
    aria-hidden="true"
    fill="none"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth={1.8}
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M4.5 8.5V4.75m0 0h3.75m-3.75 0A8.5 8.5 0 1 1 3.6 14M12 7.5V12l3 2"
    />
  </svg>
);

export const AdminIcon = ({ className = '' }) => (
  <svg
    className={className}
    aria-hidden="true"
    fill="none"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth={1.8}
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M12 3 5 6v5c0 4.4 2.8 8.2 7 10 4.2-1.8 7-5.6 7-10V6l-7-3Zm-3 9 2 2 4-5"
    />
  </svg>
);

export const KeyIcon = ({ className = '' }) => (
  <svg
    className={className}
    aria-hidden="true"
    fill="none"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth={1.8}
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M10.5 13.5a5 5 0 11-2-2L21 11.5v4h-3v3h-3v-3h-4.5"
    />
  </svg>
);

export const LogoutIcon = ({ className = '' }) => (
  <svg
    className={className}
    aria-hidden="true"
    fill="none"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth={1.8}
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M10 5H5v14h5m4-3 4-4-4-4m4 4H9"
    />
  </svg>
);

export const ConnectionIndicator = ({ className = '', isConnected = false }) => (
  <div
    className={`w-2 h-2 rounded-full ${className}`}
    style={{
      backgroundColor: isConnected ? '#22c55e' : '#d1d5db'
    }}
  />
);

export const NetworkIcon = ({ className = '', style = {} }: { className?: string; style?: React.CSSProperties }) => (
  <svg
    className={className}
    style={style}
    fill="none"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth={2}
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M8.111 16.404a5.5 5.5 0 017.778 0M12 20h.01m-7.08-7.071c3.904-3.905 10.236-3.905 14.141 0M1.394 9.393c5.857-5.857 15.355-5.857 21.213 0"
    />
  </svg>
);

export const FullscreenEnterIcon = ({ className = '' }) => (
  <svg
    className={className}
    fill="none"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth={2.2}
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M3 9V3h6M21 9V3h-6M3 15v6h6M21 15v6h-6"
    />
  </svg>
);

export const FullscreenExitIcon = ({ className = '' }) => (
  <svg
    className={className}
    fill="none"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth={2.2}
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M9 3v6H3M15 3v6h6M9 21v-6H3M15 21v-6h6"
    />
  </svg>
);

export const ShareIcon = ({ className = '' }) => (
  <svg
    className={className}
    aria-hidden="true"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth={1.8}
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <circle cx="18" cy="5" r="2.6" />
    <circle cx="6" cy="12" r="2.6" />
    <circle cx="18" cy="19" r="2.6" />
    <path d="M8.3 10.8 15.7 6.4M8.3 13.2l7.4 4.4" />
  </svg>
);

export const LinkIcon = ({ className = '' }) => (
  <svg
    className={className}
    aria-hidden="true"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth={1.8}
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <path d="M10.5 13.5a4 4 0 0 0 5.7 0l2.6-2.6a4 4 0 0 0-5.7-5.7l-1.3 1.3" />
    <path d="M13.5 10.5a4 4 0 0 0-5.7 0l-2.6 2.6a4 4 0 0 0 5.7 5.7l1.3-1.3" />
  </svg>
);

/** Crossed blades: the challenge affordance in the online-players panel. */
export const ChallengeIcon = ({ className = '' }) => (
  <svg
    className={className}
    aria-hidden="true"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth={1.8}
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <path d="M4 3h3l10.5 10.5" />
    <path d="M20 3h-3L6.5 13.5" />
    <path d="m14.5 16.5 2.5-2.5 4 4-2.5 2.5z" />
    <path d="m9.5 16.5-2.5-2.5-4 4 2.5 2.5z" />
  </svg>
);
