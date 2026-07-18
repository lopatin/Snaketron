import React, { useState, useRef, useEffect } from 'react';
import { Region } from '../types';
import { UserIcon, ConnectionIndicator, NetworkIcon } from './Icons';

interface RegionSelectorProps {
  regions: Region[];
  currentRegionId: string;
  onRegionChange: (regionId: string) => void;
  placement?: 'top' | 'bottom';
}

export const RegionSelector: React.FC<RegionSelectorProps> = ({
  regions,
  currentRegionId,
  onRegionChange,
  placement = 'bottom',
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
        type="button"
        onClick={() => setIsOpen(!isOpen)}
        className="region-selector-trigger"
        aria-expanded={isOpen}
        aria-haspopup="listbox"
      >
        <span className="region-selector-icon" aria-hidden="true">
          <NetworkIcon className="w-4 h-4" />
          {currentRegion && (
            <ConnectionIndicator
              isConnected={currentRegion.isConnected}
              className="region-selector-connection"
            />
          )}
        </span>
        <span className="region-selector-label">{currentRegion?.name || 'Select Region'}</span>
        {currentRegion && (
          <span className="region-selector-population">
            <UserIcon className="w-3 h-3 text-gray-500" />
            <span className="tabular-nums">{currentRegion.userCount}</span>
          </span>
        )}
        <svg
          className={`region-selector-chevron ${isOpen ? 'rotate-180' : ''}`}
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
          className={`region-selector-dropdown absolute bg-white rounded-lg z-50 region-dropdown ${
            placement === 'top' ? 'bottom-full mb-2 left-0' : 'right-0 mt-2'
          }`}
          role="listbox"
          aria-label="Game regions"
        >
          {regions.map((region, index) => (
            <div
              key={region.id}
              style={{
                boxShadow: index < regions.length - 1 ? 'inset 0 -0.5px 0 0 rgba(0, 0, 0, 0.1)' : 'none'
              }}
            >
              <button
                type="button"
                onClick={() => handleRegionSelect(region.id)}
                className="region-selector-option group w-full px-4 py-2 flex items-center gap-3 text-left cursor-pointer"
                role="option"
                aria-selected={region.id === currentRegionId}
              >
                {/* Connection Indicator */}
                <ConnectionIndicator isConnected={region.isConnected} className="flex-shrink-0" />

                {/* Region Name */}
                <div className="flex-1 min-w-0">
                  <div className="text-sm font-bold text-black-70 uppercase tracking-1 group-hover:text-black transition-colors">
                    {region.name}
                  </div>
                </div>

                {/* User Count - Fixed width for alignment */}
                <div className="flex items-center gap-1.5" style={{ width: '50px' }}>
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
