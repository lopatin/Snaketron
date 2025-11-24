import React, { useState } from 'react';
import { Link, useLocation } from 'react-router-dom';
import { RegionSelector } from './RegionSelector';
import { Region, LobbyMember } from '../types';

interface SidebarProps {
  regions: Region[];
  currentRegionId: string;
  onRegionChange: (regionId: string) => void;
  lobbyMembers: LobbyMember[];
  lobbyCode: string | null;
  currentUserId?: number;
  onInvite?: () => void;
  onLeaveLobby?: () => void;
  onStartGame?: () => void;
  onJoinGame?: () => void;
  isInviteDisabled?: boolean;
}

export const Sidebar: React.FC<SidebarProps> = ({
  regions,
  currentRegionId,
  onRegionChange,
  lobbyMembers,
  lobbyCode,
  currentUserId,
  onInvite,
  onLeaveLobby,
  onStartGame,
  onJoinGame,
  isInviteDisabled = false
}) => {
  const location = useLocation();
  const isPlayPage = location.pathname === '/';
  const isLeaderboardsPage = location.pathname === '/leaderboards';

  return (
    <aside className="sidebar h-screen flex flex-col">
      {/* Spacer to push content to center */}
      <div className="flex-1" />

      {/* Centered Content */}
      <div className="flex flex-col">
        {/* Navigation */}
        <nav className="flex flex-col items-end pr-8 gap-3 mb-8">
        <Link
          to="/"
          className={`text-black-70 font-black italic uppercase tracking-1 transition-opacity ${
            isPlayPage
              ? 'opacity-100 underline underline-offset-6'
              : 'opacity-70 hover:opacity-100'
          }`}
        >
          Play
        </Link>
        <Link
          to="/leaderboards"
          className={`text-black-70 font-black italic uppercase tracking-1 transition-opacity ${
            isLeaderboardsPage
              ? 'opacity-100 underline underline-offset-6'
              : 'opacity-70 hover:opacity-100'
          }`}
        >
          Leaderboards
        </Link>
        <a
          href="#"
          onClick={(e) => {
            e.preventDefault();
            onJoinGame?.();
          }}
          className="text-black-70 font-black italic uppercase tracking-1 opacity-70 hover:opacity-100 transition-opacity"
        >
          Join Game
        </a>
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
          {lobbyMembers.length > 0 && (
            <>
              {/* Lobby Title */}
              <h3 className="text-xs font-black uppercase tracking-1 text-black-70 mb-3 text-right">
                Lobby
              </h3>

              {/* Lobby Members */}
              <div className="space-y-2 flex flex-col items-end mb-6">
                {lobbyMembers.map((member) => (
                  <div
                    key={`${member.user_id}-${member.joined_at}`}
                    className="text-sm text-black-70 flex items-center gap-2"
                  >
                    <span className={member.user_id === currentUserId ? 'font-bold' : ''}>
                      {member.username}
                    </span>
                    <div className="w-2 h-2 rounded-full bg-green-500" />
                  </div>
                ))}
              </div>

              {/* Lobby Actions */}
              <div className="mb-4 flex flex-col gap-2 items-end">
                {onLeaveLobby && (
                  <button
                    onClick={onLeaveLobby}
                    className="px-4 py-2 text-xs border border-black-70 rounded font-bold uppercase bg-white text-black-70 hover:bg-gray-50 transition-colors cursor-pointer"
                    style={{ letterSpacing: '1px' }}
                  >
                    Leave Lobby
                  </button>
                )}
              </div>
            </>
          )}

          {/* Always show Invite Friends button */}
          {onInvite && (
            <button
              onClick={onInvite}
              disabled={isInviteDisabled}
              className="px-4 py-2 text-xs border border-black-70 rounded font-bold uppercase bg-white text-black-70 hover:bg-gray-50 transition-colors cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed"
              style={{ letterSpacing: '1px' }}
            >
              Invite Friends
            </button>
          )}
        </div>
      </div>
      </div>

      {/* Spacer to push content to center */}
      <div className="flex-1" />

      {/* Social Icons */}
      <div className="px-6 pb-8">
        <div className="flex justify-center">
          <a
            href="https://github.com/lopatin/snaketron"
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
    </aside>
  );
};
