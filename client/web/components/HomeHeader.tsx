import React, { useEffect, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { LobbyMember, User } from '../types';
import type { AccountModalView } from './AccountModal';
import {
  AdminIcon,
  FullscreenEnterIcon,
  FullscreenExitIcon,
  HistoryIcon,
  KeyIcon,
  LogoutIcon,
  UserIcon,
  UserPlusIcon,
} from './Icons';
import { useCrazyGames } from '../contexts/CrazyGamesContext';
import { useWallet } from '../contexts/WalletContext';
import { BUX_UNIT, formatBux, shouldShowBuxChip } from '../utils/walletChip';
import SnakeBuxIcon from './SnakeBuxIcon';
import WalletModal from './WalletModal';
import { useFullscreen } from '../hooks/useFullscreen';
import { useInputSurface } from '../hooks/useInputSurface';

interface HomeHeaderProps {
  activePage?: 'play' | 'leaderboards' | 'skins';
  currentUser: User | null;
  lobbyMembers: LobbyMember[];
  hasLobby: boolean;
  isInviteDisabled?: boolean;
  onInvite: () => void;
  onJoinGame: () => void;
  onLeaveLobby: () => void;
  onAuthClick: () => void;
  onOpenAccount: (view: AccountModalView) => void;
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
  onAuthClick,
  onOpenAccount,
  onLogout,
}) => {
  const { isCrazyGamesBuild, userAccountAvailable } = useCrazyGames();
  const { balanceBux, buxAvailable } = useWallet();
  const [walletOpen, setWalletOpen] = useState(false);
  const fullscreen = useFullscreen();
  const inputSurface = useInputSurface();
  // The CrazyGames portal owns fullscreen chrome, and desktop users have F11;
  // this toggle exists for phones and tablets browsing snaketron.io directly.
  const showFullscreenToggle =
    inputSurface === 'touch' && fullscreen.supported && !isCrazyGamesBuild;
  const [isSocialOpen, setIsSocialOpen] = useState(false);
  const [isAccountOpen, setIsAccountOpen] = useState(false);
  const socialMenuRef = useRef<HTMLDivElement>(null);
  const accountMenuRef = useRef<HTMLDivElement>(null);
  const accountTriggerRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    const handlePointerDown = (event: MouseEvent) => {
      if (socialMenuRef.current && !socialMenuRef.current.contains(event.target as Node)) {
        setIsSocialOpen(false);
      }
      if (accountMenuRef.current && !accountMenuRef.current.contains(event.target as Node)) {
        setIsAccountOpen(false);
      }
    };

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setIsSocialOpen(false);
        setIsAccountOpen(false);

        if (accountMenuRef.current?.contains(document.activeElement)) {
          accountTriggerRef.current?.focus();
        }
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
  const closeAccountMenu = () => setIsAccountOpen(false);
  const openAccountModal = (view: AccountModalView) => {
    closeAccountMenu();
    accountTriggerRef.current?.focus();
    onOpenAccount(view);
  };

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
          <Link
            to="/skins"
            className={`home-nav-link ${activePage === 'skins' ? 'is-active' : ''}`}
            aria-current={activePage === 'skins' ? 'page' : undefined}
          >
            Skins
          </Link>

          <div className="home-social-menu" ref={socialMenuRef}>
            <button
              type="button"
              className={`home-nav-link home-social-trigger ${isSocialOpen ? 'is-open' : ''}`}
              onClick={() => {
                setIsAccountOpen(false);
                setIsSocialOpen((current) => !current);
              }}
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
                      <div key={`${member.user_id}-${member.ts}`} className="home-lobby-member">
                        <span className="home-lobby-member-dot" aria-hidden="true" />
                        <span>{member.username}</span>
                        {/* Host badge removed: the wire LobbyMember carries no host
                            flag (host is LobbyUpdate.host_user_id, not threaded here). */}
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
                    <UserPlusIcon className="home-social-action-icon" />
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
                    <KeyIcon className="home-social-action-icon" />
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
                      <LogoutIcon className="home-social-action-icon" />
                    </button>
                  )}
                </div>
              </div>
            )}
          </div>
        </nav>

        <div className="home-account home-account-menu" ref={accountMenuRef}>
          {/* Inside the account cluster rather than beside it: the header is a
              two-child flexbox with `space-between`, so a third top-level child
              would silently re-third the layout and move both existing groups. */}
          {shouldShowBuxChip(Boolean(currentUser), balanceBux, buxAvailable) && (
            <button
              type="button"
              className="home-bux-chip"
              title={`${formatBux(balanceBux)} ${BUX_UNIT}`}
              onClick={() => setWalletOpen(true)}
              data-testid="home-bux-chip"
            >
              <SnakeBuxIcon size={22} />
              <span className="home-bux-amount">{formatBux(balanceBux)}</span>
            </button>
          )}
          {showFullscreenToggle && (
            <button
              type="button"
              className="home-fullscreen-toggle"
              onClick={fullscreen.toggle}
              aria-label={fullscreen.active ? 'Exit full screen' : 'Enter full screen'}
              title={fullscreen.active ? 'Exit full screen' : 'Full screen'}
              data-testid="home-fullscreen-toggle"
            >
              {fullscreen.active
                ? <FullscreenExitIcon className="home-fullscreen-icon" />
                : <FullscreenEnterIcon className="home-fullscreen-icon" />}
            </button>
          )}
          {isCrazyGamesBuild ? (
            currentUser?.authSource === 'crazygames' ? (
              <>
                <button
                  ref={accountTriggerRef}
                  id="crazygames-account-menu-trigger"
                  type="button"
                  className={`home-account-action home-account-trigger flex items-center gap-2 ${isAccountOpen ? 'is-open' : ''}`}
                  onClick={() => {
                    setIsSocialOpen(false);
                    setIsAccountOpen((current) => !current);
                  }}
                  aria-label={`Playing as ${currentUser.username} through CrazyGames; progress saves automatically`}
                  aria-expanded={isAccountOpen}
                  aria-haspopup="menu"
                  aria-controls="crazygames-account-menu"
                  title="CrazyGames account linked · progress saves automatically"
                >
                  {currentUser.avatarUrl && (
                    <img
                      src={currentUser.avatarUrl}
                      alt=""
                      className="h-7 w-7 rounded-full border border-black/20 object-cover"
                      referrerPolicy="no-referrer"
                    />
                  )}
                  <span className="home-account-username">{currentUser.username}</span>
                  <svg viewBox="0 0 12 8" aria-hidden="true">
                    <path d="M1 1.5 6 6.5l5-5" />
                  </svg>
                </button>

                {isAccountOpen && (
                  <div
                    id="crazygames-account-menu"
                    className="home-social-panel home-account-panel"
                    role="menu"
                    aria-labelledby="crazygames-account-menu-trigger"
                  >
                    <div className="home-social-actions home-account-actions">
                      <button
                        type="button"
                        role="menuitem"
                        onClick={() => openAccountModal('profile')}
                      >
                        <span>Profile</span>
                        <UserIcon className="home-social-action-icon" />
                      </button>
                      <button
                        type="button"
                        role="menuitem"
                        onClick={() => openAccountModal('history')}
                      >
                        <span>History</span>
                        <HistoryIcon className="home-social-action-icon" />
                      </button>
                    </div>
                  </div>
                )}
              </>
            ) : (
              <button
                type="button"
                onClick={onAuthClick}
                className="home-account-action"
                disabled={!userAccountAvailable}
                title={userAccountAvailable ? undefined : 'CrazyGames account login is unavailable in this embed'}
              >
                {userAccountAvailable ? 'Sign in with CrazyGames' : 'Playing as guest'}
              </button>
            )
          ) : currentUser && !currentUser.isGuest ? (
            <>
              <button
                ref={accountTriggerRef}
                id="account-menu-trigger"
                type="button"
                className={`home-account-action home-account-trigger ${isAccountOpen ? 'is-open' : ''}`}
                onClick={() => {
                  setIsSocialOpen(false);
                  setIsAccountOpen((current) => !current);
                }}
                aria-expanded={isAccountOpen}
                aria-haspopup="menu"
                aria-controls="account-menu"
              >
                <span className="home-account-username">{currentUser.username}</span>
                <svg viewBox="0 0 12 8" aria-hidden="true">
                  <path d="M1 1.5 6 6.5l5-5" />
                </svg>
              </button>

              {isAccountOpen && (
                <div
                  id="account-menu"
                  className="home-social-panel home-account-panel"
                  role="menu"
                  aria-labelledby="account-menu-trigger"
                >
                  <div className="home-social-actions home-account-actions">
                    <button
                      type="button"
                      role="menuitem"
                      onClick={() => openAccountModal('profile')}
                    >
                      <span>Profile</span>
                      <UserIcon className="home-social-action-icon" />
                    </button>
                    <button
                      type="button"
                      role="menuitem"
                      onClick={() => openAccountModal('history')}
                    >
                      <span>History</span>
                      <HistoryIcon className="home-social-action-icon" />
                    </button>
                    {currentUser.isAdmin && (
                      <Link
                        to="/admin"
                        role="menuitem"
                        onClick={closeAccountMenu}
                      >
                        <span>Admin</span>
                        <AdminIcon className="home-social-action-icon" />
                      </Link>
                    )}
                    <button
                      type="button"
                      role="menuitem"
                      className="is-destructive"
                      onClick={() => {
                        closeAccountMenu();
                        onLogout();
                      }}
                    >
                      <span>Logout</span>
                      <LogoutIcon className="home-social-action-icon" />
                    </button>
                  </div>
                </div>
              )}
            </>
          ) : currentUser?.isGuest ? (
            <>
              <button
                ref={accountTriggerRef}
                id="guest-account-menu-trigger"
                type="button"
                className={`home-account-action home-account-trigger ${isAccountOpen ? 'is-open' : ''}`}
                onClick={() => {
                  setIsSocialOpen(false);
                  setIsAccountOpen((current) => !current);
                }}
                aria-expanded={isAccountOpen}
                aria-haspopup="menu"
                aria-controls="guest-account-menu"
              >
                <span className="home-account-guest-label">
                  <span className="home-account-username">{currentUser.username}</span>
                  <span className="home-account-guest-suffix">(guest)</span>
                </span>
                <svg viewBox="0 0 12 8" aria-hidden="true">
                  <path d="M1 1.5 6 6.5l5-5" />
                </svg>
              </button>

              {isAccountOpen && (
                <div
                  id="guest-account-menu"
                  className="home-social-panel home-account-panel"
                  role="menu"
                  aria-labelledby="guest-account-menu-trigger"
                >
                  <div className="home-social-actions home-account-actions">
                    <button
                      type="button"
                      role="menuitem"
                      onClick={() => {
                        closeAccountMenu();
                        accountTriggerRef.current?.focus();
                        onAuthClick();
                      }}
                    >
                      <span>Sign in</span>
                      <UserIcon className="home-social-action-icon" />
                    </button>
                    <button
                      type="button"
                      role="menuitem"
                      className="is-destructive"
                      onClick={() => {
                        closeAccountMenu();
                        onLogout();
                      }}
                    >
                      <span>Logout</span>
                      <LogoutIcon className="home-social-action-icon" />
                    </button>
                  </div>
                </div>
              )}
            </>
          ) : (
            <button
              type="button"
              onClick={onAuthClick}
              className="home-account-action"
            >
              Sign in
            </button>
          )}
        </div>
      {walletOpen ? <WalletModal onClose={() => setWalletOpen(false)} /> : null}
    </header>
  );
};
