import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { RegionSelector } from './RegionSelector';
import { Region, User } from '../types';

interface MobileHeaderProps {
  regions: Region[];
  currentRegionId: string;
  onRegionChange: (regionId: string) => void;
  currentUser?: User | null;
  onLoginClick: () => void;
  lobbyUsers: string[];
  onInvite?: () => void;
  isInviteDisabled?: boolean;
}

export const MobileHeader: React.FC<MobileHeaderProps> = ({
  regions,
  currentRegionId,
  onRegionChange,
  currentUser,
  onLoginClick,
  lobbyUsers,
  onInvite,
  isInviteDisabled = false
}) => {
  const [isSidebarOpen, setIsSidebarOpen] = useState(false);

  return (
    <>
      <header className="border-t-3 border-b-3 border-white py-5 pb-[18px] site-header">
        <div className="px-5 flex justify-between items-center">
          {/* Hamburger Menu */}
          <button
            onClick={() => setIsSidebarOpen(!isSidebarOpen)}
            className="text-gray-400 hover:text-black-70 transition-colors p-2"
            aria-label="Menu"
          >
            <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
            </svg>
          </button>

          {/* Logo */}
          <Link to="/">
            <img src="/SnaketronLogo.png" alt="Snaketron" className="h-6 w-auto opacity-80" />
          </Link>

          {/* User/Login */}
          {currentUser && !currentUser.isGuest ? (
            <span className="text-sm text-black-70 font-bold uppercase">
              {currentUser.username}
            </span>
          ) : (
            <button
              onClick={onLoginClick}
              className="text-sm text-black-70 font-bold uppercase hover:opacity-70 transition-opacity"
            >
              LOGIN
            </button>
          )}
        </div>
      </header>

      {/* Mobile Sidebar */}
      {isSidebarOpen && (
        <div className="fixed top-0 left-0 bottom-0 w-80 z-50 sidebar flex flex-col bg-white">
            {/* Close Button */}
            <button
              onClick={() => setIsSidebarOpen(false)}
              className="absolute top-4 right-4 text-black-70 hover:text-black transition-colors z-10"
              aria-label="Close menu"
            >
              <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>

            {/* Spacer to push content to center */}
            <div className="flex-1" />

            {/* Centered Content */}
            <div className="flex flex-col">
              {/* Navigation */}
              <nav className="flex flex-col items-end pr-8 gap-3 mb-8">
              <Link
                to="/"
                onClick={() => setIsSidebarOpen(false)}
                className="text-black-70 font-black italic uppercase tracking-1 opacity-100 underline underline-offset-6"
              >
                Play
              </Link>
              <a
                href="#"
                onClick={() => setIsSidebarOpen(false)}
                className="text-black-70 font-black italic uppercase tracking-1 opacity-70 hover:opacity-100 transition-opacity"
              >
                Leaderboards
              </a>
              <Link
                to="/spectate"
                onClick={() => setIsSidebarOpen(false)}
                className="text-black-70 font-black italic uppercase tracking-1 opacity-70 hover:opacity-100 transition-opacity"
              >
                Spectate
              </Link>
            </nav>

            {/* Region Selector */}
            <div className="pr-8 mb-8 flex justify-end">
              <RegionSelector
                regions={regions}
                currentRegionId={currentRegionId}
                onRegionChange={onRegionChange}
              />
            </div>

            {/* Lobby Info */}
            <div className="pr-8 mb-8">
              <div className="flex flex-col items-end">
                <h3 className="text-xs font-black uppercase tracking-1 text-black-70 mb-3 text-right">
                  Lobby
                </h3>
                {lobbyUsers.length === 0 || (lobbyUsers.length === 1 && lobbyUsers[0] === currentUser?.username) ? (
                  <button
                    onClick={onInvite}
                    disabled={isInviteDisabled}
                    className="px-4 py-2 text-xs border border-black-70 rounded font-bold uppercase bg-white text-black-70 hover:bg-gray-50 transition-colors cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed"
                    style={{ letterSpacing: '1px' }}
                  >
                    Invite Friends
                  </button>
                ) : (
                  <div className="space-y-2 flex flex-col items-end">
                    {lobbyUsers.map((username, index) => (
                      <div
                        key={index}
                        className="text-sm text-black-70 flex items-center gap-2"
                      >
                        <span className={username === currentUser?.username ? 'font-bold' : ''}>
                          {username}
                        </span>
                        <div className="w-2 h-2 rounded-full bg-green-500" />
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
            </div>

            {/* Spacer to push content to center */}
            <div className="flex-1" />

            {/* Social Icons */}
            <div className="px-6 pb-8">
              <div className="flex justify-center gap-4">
                <a
                  href="https://reddit.com"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-black-70 opacity-70 hover:opacity-100 transition-opacity"
                  aria-label="Reddit"
                >
                  <svg className="w-6 h-6" fill="currentColor" viewBox="0 0 24 24">
                    <path d="M12 0A12 12 0 0 0 0 12a12 12 0 0 0 12 12 12 12 0 0 0 12-12A12 12 0 0 0 12 0zm5.01 4.744c.688 0 1.25.561 1.25 1.249a1.25 1.25 0 0 1-2.498.056l-2.597-.547-.8 3.747c1.824.07 3.48.632 4.674 1.488.308-.309.73-.491 1.207-.491.968 0 1.754.786 1.754 1.754 0 .716-.435 1.333-1.01 1.614a3.111 3.111 0 0 1 .042.52c0 2.694-3.13 4.87-7.004 4.87-3.874 0-7.004-2.176-7.004-4.87 0-.183.015-.366.043-.534A1.748 1.748 0 0 1 4.028 12c0-.968.786-1.754 1.754-1.754.463 0 .898.196 1.207.49 1.207-.883 2.878-1.43 4.744-1.487l.885-4.182a.342.342 0 0 1 .14-.197.35.35 0 0 1 .238-.042l2.906.617a1.214 1.214 0 0 1 1.108-.701zM9.25 12C8.561 12 8 12.562 8 13.25c0 .687.561 1.248 1.25 1.248.687 0 1.248-.561 1.248-1.249 0-.688-.561-1.249-1.249-1.249zm5.5 0c-.687 0-1.248.561-1.248 1.25 0 .687.561 1.248 1.249 1.248.688 0 1.249-.561 1.249-1.249 0-.687-.562-1.249-1.25-1.249zm-5.466 3.99a.327.327 0 0 0-.231.094.33.33 0 0 0 0 .463c.842.842 2.484.913 2.961.913.477 0 2.105-.056 2.961-.913a.361.361 0 0 0 .029-.463.33.33 0 0 0-.464 0c-.547.533-1.684.73-2.512.73-.828 0-1.979-.196-2.512-.73a.326.326 0 0 0-.232-.095z"/>
                  </svg>
                </a>
                <a
                  href="https://discord.com"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-black-70 opacity-70 hover:opacity-100 transition-opacity"
                  aria-label="Discord"
                >
                  <svg className="w-6 h-6" fill="currentColor" viewBox="0 0 24 24">
                    <path d="M20.317 4.37a19.791 19.791 0 0 0-4.885-1.515.074.074 0 0 0-.079.037c-.21.375-.444.864-.608 1.25a18.27 18.27 0 0 0-5.487 0 12.64 12.64 0 0 0-.617-1.25.077.077 0 0 0-.079-.037A19.736 19.736 0 0 0 3.677 4.37a.07.07 0 0 0-.032.027C.533 9.046-.32 13.58.099 18.057a.082.082 0 0 0 .031.057 19.9 19.9 0 0 0 5.993 3.03.078.078 0 0 0 .084-.028c.462-.63.874-1.295 1.226-1.994a.076.076 0 0 0-.041-.106 13.107 13.107 0 0 1-1.872-.892.077.077 0 0 1-.008-.128 10.2 10.2 0 0 0 .372-.292.074.074 0 0 1 .077-.01c3.928 1.793 8.18 1.793 12.062 0a.074.074 0 0 1 .078.01c.12.098.246.198.373.292a.077.077 0 0 1-.006.127 12.299 12.299 0 0 1-1.873.892.077.077 0 0 0-.041.107c.36.698.772 1.362 1.225 1.993a.076.076 0 0 0 .084.028 19.839 19.839 0 0 0 6.002-3.03.077.077 0 0 0 .032-.054c.5-5.177-.838-9.674-3.549-13.66a.061.061 0 0 0-.031-.03zM8.02 15.33c-1.183 0-2.157-1.085-2.157-2.419 0-1.333.956-2.419 2.157-2.419 1.21 0 2.176 1.096 2.157 2.42 0 1.333-.956 2.418-2.157 2.418zm7.975 0c-1.183 0-2.157-1.085-2.157-2.419 0-1.333.955-2.419 2.157-2.419 1.21 0 2.176 1.096 2.157 2.42 0 1.333-.946 2.418-2.157 2.418z"/>
                  </svg>
                </a>
                <a
                  href="https://twitter.com"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-black-70 opacity-70 hover:opacity-100 transition-opacity"
                  aria-label="Twitter"
                >
                  <svg className="w-6 h-6" fill="currentColor" viewBox="0 0 24 24">
                    <path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"/>
                  </svg>
                </a>
                <a
                  href="https://github.com"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-black-70 opacity-70 hover:opacity-100 transition-opacity"
                  aria-label="GitHub"
                >
                  <svg className="w-6 h-6" fill="currentColor" viewBox="0 0 24 24">
                    <path d="M12 .297c-6.63 0-12 5.373-12 12 0 5.303 3.438 9.8 8.205 11.385.6.113.82-.258.82-.577 0-.285-.01-1.04-.015-2.04-3.338.724-4.042-1.61-4.042-1.61C4.422 18.07 3.633 17.7 3.633 17.7c-1.087-.744.084-.729.084-.729 1.205.084 1.838 1.236 1.838 1.236 1.07 1.835 2.809 1.305 3.495.998.108-.776.417-1.305.76-1.605-2.665-.3-5.466-1.332-5.466-5.93 0-1.31.465-2.38 1.235-3.22-.135-.303-.54-1.523.105-3.176 0 0 1.005-.322 3.3 1.23.96-.267 1.98-.399 3-.405 1.02.006 2.04.138 3 .405 2.28-1.552 3.285-1.23 3.285-1.23.645 1.653.24 2.873.12 3.176.765.84 1.23 1.91 1.23 3.22 0 4.61-2.805 5.625-5.475 5.92.42.36.81 1.096.81 2.22 0 1.606-.015 2.896-.015 3.286 0 .315.21.69.825.57C20.565 22.092 24 17.592 24 12.297c0-6.627-5.373-12-12-12"/>
                  </svg>
                </a>
              </div>
            </div>
        </div>
      )}
    </>
  );
};
