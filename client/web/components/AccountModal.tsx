import React, { useRef } from 'react';
import { User } from '../types';
import { HistoryIcon } from './Icons';
import { LobbyModal } from './LobbyModal';

export type AccountModalView = 'profile' | 'history';

interface AccountModalProps {
  view: AccountModalView | null;
  user: User | null;
  onClose: () => void;
}

export const AccountModal: React.FC<AccountModalProps> = ({ view, user, onClose }) => {
  const doneButtonRef = useRef<HTMLButtonElement>(null);

  if (!view || !user || user.isGuest) {
    return null;
  }

  const isHistory = view === 'history';

  return (
    <LobbyModal
      isOpen
      onClose={onClose}
      title={isHistory ? 'History' : 'Profile'}
      description={isHistory
        ? 'Completed matches will be collected here.'
        : 'Your Snaketron player details.'}
      initialFocusRef={doneButtonRef}
    >
      {isHistory ? (
        <div className="account-history-empty">
          <HistoryIcon className="account-history-icon" />
          <h3>Match history is coming soon</h3>
        </div>
      ) : (
        <dl className="account-profile-details">
          <div>
            <dt>Username</dt>
            <dd>{user.username}</dd>
          </div>
          <div>
            <dt>Player ID</dt>
            <dd>#{user.id}</dd>
          </div>
          <div>
            <dt>Rating</dt>
            <dd>{user.mmr?.toLocaleString() ?? '—'}</dd>
          </div>
        </dl>
      )}

      <div className="lobby-modal-actions lobby-modal-invite-actions">
        <button
          ref={doneButtonRef}
          type="button"
          className="lobby-modal-button is-secondary"
          onClick={onClose}
        >
          Done
        </button>
      </div>
    </LobbyModal>
  );
};
