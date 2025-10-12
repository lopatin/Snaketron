import React, { useState } from 'react';

interface InviteFriendsModalProps {
  isOpen: boolean;
  onClose: () => void;
  lobbyCode: string | null;
}

export const InviteFriendsModal: React.FC<InviteFriendsModalProps> = ({
  isOpen,
  onClose,
  lobbyCode,
}) => {
  const [copiedCode, setCopiedCode] = useState(false);
  const [copiedUrl, setCopiedUrl] = useState(false);

  if (!isOpen) return null;

  const lobbyUrl = lobbyCode
    ? `${window.location.origin}/lobby/${lobbyCode}`
    : '';

  const handleCopyCode = async () => {
    if (lobbyCode) {
      try {
        await navigator.clipboard.writeText(lobbyCode);
        setCopiedCode(true);
        setTimeout(() => setCopiedCode(false), 2000);
      } catch (err) {
        console.error('Failed to copy code:', err);
      }
    }
  };

  const handleCopyUrl = async () => {
    if (lobbyUrl) {
      try {
        await navigator.clipboard.writeText(lobbyUrl);
        setCopiedUrl(true);
        setTimeout(() => setCopiedUrl(false), 2000);
      } catch (err) {
        console.error('Failed to copy URL:', err);
      }
    }
  };

  return (
    <div
      className="fixed inset-0 flex items-center justify-center p-4 z-50"
      onClick={onClose}
      style={{ backgroundColor: 'rgba(255, 255, 255, 0.7)' }}
    >
      <div
        className="bg-white rounded-lg p-8 w-full max-w-lg"
        onClick={(e) => e.stopPropagation()}
        style={{
          border: '2px solid rgba(0, 0, 0, 0.2)',
          boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)'
        }}
      >
        <div className="text-center mb-6">
          <h2 className="text-2xl font-black italic uppercase tracking-1 text-black-70 mb-2">
            Invite Friends
          </h2>
          <p className="text-sm text-black-70 opacity-60">
            Share this code or link with your friends
          </p>
        </div>

        <div className="mb-5">
          <label className="block text-xs font-black italic uppercase tracking-1 text-black-70 mb-2 opacity-50">
            Code
          </label>
          <div className="flex gap-2">
            <div className="flex-1 px-4 py-3 border-2 border-black-70 rounded-lg font-mono text-2xl text-center uppercase tracking-widest text-black-70">
              {lobbyCode || 'XXXXXXXX'}
            </div>
            <button
              onClick={handleCopyCode}
              className="px-5 py-3 border-2 border-black-70 rounded-lg font-black italic uppercase text-xs text-black-70 hover:bg-gray-50 transition-colors"
              style={{ letterSpacing: '1px' }}
            >
              {copiedCode ? 'Copied!' : 'Copy'}
            </button>
          </div>
        </div>

        <div className="mb-6">
          <label className="block text-xs font-black italic uppercase tracking-1 text-black-70 mb-2 opacity-50">
            Link
          </label>
          <div className="flex gap-2">
            <div className="flex-1 px-4 py-3 border-2 border-black-70 rounded-lg text-sm text-black-70 overflow-hidden text-ellipsis whitespace-nowrap">
              {lobbyUrl}
            </div>
            <button
              onClick={handleCopyUrl}
              className="px-5 py-3 border-2 border-black-70 rounded-lg font-black italic uppercase text-xs text-black-70 hover:bg-gray-50 transition-colors"
              style={{ letterSpacing: '1px' }}
            >
              {copiedUrl ? 'Copied!' : 'Copy'}
            </button>
          </div>
        </div>

        <button
          onClick={onClose}
          className="w-full px-6 py-3 border-2 border-black-70 rounded-lg font-black italic uppercase tracking-1 text-black-70 hover:bg-gray-50 transition-colors"
        >
          Close
        </button>
      </div>
    </div>
  );
};
