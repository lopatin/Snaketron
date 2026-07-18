import React, { useEffect, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { LobbyMember, User } from '../types';

interface HomeHeaderProps {
  activePage?: 'play' | 'leaderboards';
  currentUser: User | null;
  lobbyMembers: LobbyMember[];
  hasLobby: boolean;
  isInviteDisabled?: boolean;
  onInvite: () => void;
  onJoinGame: () => void;
  onLeaveLobby: () => void;
  onLoginClick: () => void;
  onLogout: () => void;
}

export const HomeHeader: React.FC<HomeHeaderProps> = ({
  activePage = 'play',
  currentUser,
  lobbyMembers,
  hasLobby,
  isInviteDisabled = false,
  onInvite,
  onJoinGame,
  onLeaveLobby,
  onLoginClick,
  onLogout,
}) => {
  const [isSocialOpen, setIsSocialOpen] = useState(false);
  const socialMenuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handlePointerDown = (event: MouseEvent) => {
      if (socialMenuRef.current && !socialMenuRef.current.contains(event.target as Node)) {
        setIsSocialOpen(false);
      }
    };

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setIsSocialOpen(false);
      }
    };

    document.addEventListener('mousedown', handlePointerDown);
    document.addEventListener('keydown', handleKeyDown);
    return () => {
      document.removeEventListener('mousedown', handlePointerDown);
      document.removeEventListener('keydown', handleKeyDown);
    };
  }, []);

  const closeSocialMenu = () => setIsSocialOpen(false);

  return (
    <header className="home-header">
        <nav className="home-top-nav" aria-label="Primary navigation">
          <Link
            to="/"
            className={`home-nav-link ${activePage === 'play' ? 'is-active' : ''}`}
            aria-current={activePage === 'play' ? 'page' : undefined}
          >
            Play
          </Link>
          <Link
            to="/leaderboards"
            className={`home-nav-link ${activePage === 'leaderboards' ? 'is-active' : ''}`}
            aria-current={activePage === 'leaderboards' ? 'page' : undefined}
          >
            Leaderboards
          </Link>

          <div className="home-social-menu" ref={socialMenuRef}>
            <button
              type="button"
              className={`home-nav-link home-social-trigger ${isSocialOpen ? 'is-open' : ''}`}
              onClick={() => setIsSocialOpen((current) => !current)}
              aria-expanded={isSocialOpen}
              aria-haspopup="menu"
            >
              Social
              {lobbyMembers.length > 0 && (
                <span className="home-social-count" aria-label={`${lobbyMembers.length} lobby members`}>
                  {lobbyMembers.length.toString().padStart(2, '0')}
                </span>
              )}
              <svg viewBox="0 0 12 8" aria-hidden="true">
                <path d="M1 1.5 6 6.5l5-5" />
              </svg>
            </button>

            {isSocialOpen && (
              <div className="home-social-panel" role="menu">
                <div className="home-social-panel-heading">
                  <span>{hasLobby ? 'Lobby' : 'Play with friends'}</span>
                  {hasLobby && <span>{lobbyMembers.length} online</span>}
                </div>

                {lobbyMembers.length > 0 && (
                  <div className="home-lobby-roster" aria-label="Lobby members">
                    {lobbyMembers.map((member) => (
                      <div key={`${member.user_id}-${member.joined_at}`} className="home-lobby-member">
                        <span className="home-lobby-member-dot" aria-hidden="true" />
                        <span>{member.username}</span>
                        {member.is_host && <span className="home-lobby-host">Host</span>}
                      </div>
                    ))}
                  </div>
                )}

                <div className="home-social-actions">
                  <button
                    type="button"
                    role="menuitem"
                    onClick={() => {
                      closeSocialMenu();
                      onInvite();
                    }}
                    disabled={isInviteDisabled}
                  >
                    <span>Invite friends</span>
                    <span aria-hidden="true">↗</span>
                  </button>
                  <button
                    type="button"
                    role="menuitem"
                    onClick={() => {
                      closeSocialMenu();
                      onJoinGame();
                    }}
                  >
                    <span>Join by code</span>
                    <span aria-hidden="true">+</span>
                  </button>
                  {hasLobby && (
                    <button
                      type="button"
                      role="menuitem"
                      className="is-destructive"
                      onClick={() => {
                        closeSocialMenu();
                        onLeaveLobby();
                      }}
                    >
                      <span>Leave lobby</span>
                      <span aria-hidden="true">×</span>
                    </button>
                  )}
                </div>
              </div>
            )}
          </div>
        </nav>

        <div className="home-account">
          {currentUser && !currentUser.isGuest ? (
            <>
              <span className="home-account-name">{currentUser.username}</span>
              <button type="button" onClick={onLogout} className="home-account-action">
                Log out
              </button>
            </>
          ) : (
            <button type="button" onClick={onLoginClick} className="home-account-action">
              Log in
            </button>
          )}
        </div>
    </header>
  );
};
