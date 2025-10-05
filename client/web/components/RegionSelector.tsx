import React, { useState, useRef, useEffect } from 'react';
import { Region } from '../types';
import { UserIcon, ConnectionIndicator, NetworkIcon } from './Icons';

interface RegionSelectorProps {
  regions: Region[];
  currentRegionId: string;
  onRegionChange: (regionId: string) => void;
}

export const RegionSelector: React.FC<RegionSelectorProps> = ({
  regions,
  currentRegionId,
  onRegionChange
}) => {
  const [isOpen, setIsOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);

  const currentRegion = regions.find(r => r.id === currentRegionId);

  // Close dropdown when clicking outside
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (dropdownRef.current && !(dropdownRef.current as HTMLElement).contains(event.target as Node)) {
        setIsOpen(false);
      }
    }

    document.addEventListener('mousedown', handleClickOutside);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, []);

  const handleRegionSelect = (regionId: string) => {
    onRegionChange(regionId);
    setIsOpen(false);
  };

  return (
    <div className="relative" ref={dropdownRef}>
      {/* Current Region Button */}
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="text-sm text-black-70 font-bold uppercase tracking-1 bg-transparent border border-black-70 rounded px-3 py-1 cursor-pointer hover:bg-gray-50 transition-colors flex items-center gap-2"
      >
        {currentRegion && (
          <ConnectionIndicator isConnected={currentRegion.isConnected} className="flex-shrink-0" />
        )}
        {currentRegion?.name || 'Select Region'}
        {currentRegion && (
          <div className="flex items-center gap-1" style={{ letterSpacing: '0' }}>
            <UserIcon className="w-3 h-3 text-gray-500" />
            <span className="tabular-nums">{currentRegion.userCount}</span>
          </div>
        )}
        <svg
          className={`w-3 h-3 transition-transform ${isOpen ? 'rotate-180' : ''}`}
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
        >
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>

      {/* Dropdown List */}
      {isOpen && (
        <div
          className="absolute right-0 mt-2 bg-white rounded-lg z-50 region-dropdown"
          style={{
            minWidth: '320px',
            border: '3px solid white',
            boxShadow: 'inset 0 0 0 0.5px rgba(0, 0, 0, 0.7), .5px .5px 0 1.5px rgba(0, 0, 0, 0.7)',
            background: 'linear-gradient(to bottom, #ffffff, #fafafa)'
          }}
        >
          {regions.map((region, index) => (
            <div
              key={region.id}
              style={{
                boxShadow: index < regions.length - 1 ? 'inset 0 -0.5px 0 0 rgba(0, 0, 0, 0.1)' : 'none'
              }}
            >
              <button
                onClick={() => handleRegionSelect(region.id)}
                className="group w-full px-4 py-2 flex items-center gap-3 text-left cursor-pointer"
              >
                {/* Connection Indicator */}
                <ConnectionIndicator isConnected={region.isConnected} className="flex-shrink-0" />

                {/* Region Name */}
                <div className="flex-1">
                  <div className="text-sm font-bold text-black-70 uppercase tracking-1 group-hover:text-black transition-colors">
                    {region.name}
                  </div>
                </div>

                {/* User Count - Fixed width for alignment */}
                <div className="flex items-center gap-1.5" style={{ width: '60px' }}>
                  <UserIcon className="w-3.5 h-3.5 text-gray-500" />
                  <span className="text-sm font-bold text-gray-700 tabular-nums">
                    {region.userCount}
                  </span>
                </div>

                {/* Ping - Fixed width for alignment */}
                <div className="flex items-center gap-1.5" style={{ width: '70px' }}>
                  <NetworkIcon
                    className="w-3.5 h-3.5"
                    style={{
                      color: region.ping === null ? '#9ca3af' :
                             region.ping < 50 ? '#22c55e' :
                             region.ping < 100 ? '#f7b731' : '#ef4444'
                    }}
                  />
                  <span
                    className="text-sm font-semibold tabular-nums"
                    style={{
                      color: region.ping === null ? '#9ca3af' :
                             region.ping < 50 ? '#22c55e' :
                             region.ping < 100 ? '#f7b731' : '#ef4444'
                    }}
                  >
                    {region.ping !== null ? `${region.ping}ms` : 'N/A'}
                  </span>
                </div>
              </button>
            </div>
          ))}
        </div>
      )}
    </div>
  );
};
