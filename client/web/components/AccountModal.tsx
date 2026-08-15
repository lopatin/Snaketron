import React, { useCallback, useRef } from 'react';
import { User } from '../types';
import { api } from '../services/api';
import { LobbyModal } from './LobbyModal';
import { MatchHistoryList } from './MatchHistoryList';
import RankIcon from './RankIcon';
import { formatRankLabel, getRankFromMMR } from '../utils/rank';

export type AccountModalView = 'profile' | 'history';

interface AccountModalProps {
  view: AccountModalView | null;
  user: User | null;
  onClose: () => void;
}

export const AccountModal: React.FC<AccountModalProps> = ({ view, user, onClose }) => {
  const doneButtonRef = useRef<HTMLButtonElement>(null);
  const historyFocusRef = useRef<HTMLDivElement>(null);
  const loadHistory = useCallback(
    (cursor: string | null) => api.getMatchHistory(cursor),
    [],
  );

  if (!view || !user || user.isGuest) {
    return null;
  }

  const isHistory = view === 'history';
  const rank = user.mmr != null ? getRankFromMMR(user.mmr) : null;

  return (
    <LobbyModal
      isOpen
      onClose={onClose}
      title={isHistory ? 'History' : 'Profile'}
      description={isHistory
        ? 'Your completed matches, ratings, and rewards.'
        : 'Your Snaketron player details.'}
      initialFocusRef={isHistory ? historyFocusRef : doneButtonRef}
      size={isHistory ? 'wide' : 'default'}
    >
      {isHistory ? (
        <div ref={historyFocusRef} tabIndex={-1} className="account-history-panel">
          <MatchHistoryList
            variant="compact"
            currentUserId={user.id}
            loadPage={loadHistory}
          />
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
            <dt>Rank</dt>
            <dd className="account-profile-rank">
              {rank ? (
                <>
                  <RankIcon
                    tier={rank.tier}
                    division={rank.division}
                    label={formatRankLabel(rank)}
                  />
                  <span>
                    {formatRankLabel(rank)}
                    <em>{user.mmr?.toLocaleString()} MMR</em>
                  </span>
                </>
              ) : (
                <>
                  <RankIcon tier="unranked" label="Unranked" />
                  <span>Unranked</span>
                </>
              )}
            </dd>
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
