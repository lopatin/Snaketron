import React from 'react';
import { useLatency } from '../contexts/LatencyContext';
import { useWebSocket } from '../contexts/WebSocketContext';

interface LatencySettingsProps {
  onClose?: () => void;
}

export const LatencySettings: React.FC<LatencySettingsProps> = ({ onClose }) => {
  const { settings, updateSettings } = useLatency();
  const { latencyMs } = useWebSocket();

  return (
    <div className="p-6 bg-white rounded-lg shadow-lg">
      <h2 className="text-xl font-bold mb-4">Network Latency Settings</h2>
      
      <div className="mb-4">
        <p className="text-sm text-gray-600 mb-2">
          Current real latency: <span className="font-mono">{latencyMs}ms</span>
        </p>
      </div>

      <div className="mb-4">
        <label className="flex items-center">
          <input
            type="checkbox"
            checked={settings.enabled}
            onChange={(e) => updateSettings({ enabled: e.target.checked })}
            className="mr-2"
          />
          <span className="font-medium">Enable artificial latency</span>
        </label>
      </div>

      {settings.enabled && (
        <>
          <div className="mb-4">
            <label className="block mb-2">
              <span className="text-sm font-medium">Send delay: {settings.sendDelayMs}ms</span>
              <input
                type="range"
                min="0"
                max="1000"
                step="10"
                value={settings.sendDelayMs}
                onChange={(e) => updateSettings({ sendDelayMs: parseInt(e.target.value) })}
                className="w-full mt-1"
              />
            </label>
            <div className="flex justify-between text-xs text-gray-500">
              <span>0ms</span>
              <span>500ms</span>
              <span>1000ms</span>
            </div>
          </div>

          <div className="mb-4">
            <label className="block mb-2">
              <span className="text-sm font-medium">Receive delay: {settings.receiveDelayMs}ms</span>
              <input
                type="range"
                min="0"
                max="1000"
                step="10"
                value={settings.receiveDelayMs}
                onChange={(e) => updateSettings({ receiveDelayMs: parseInt(e.target.value) })}
                className="w-full mt-1"
              />
            </label>
            <div className="flex justify-between text-xs text-gray-500">
              <span>0ms</span>
              <span>500ms</span>
              <span>1000ms</span>
            </div>
          </div>

          <div className="mt-4 p-3 bg-gray-100 rounded text-sm">
            <p className="font-medium mb-1">Total artificial latency:</p>
            <p>Send: {settings.sendDelayMs}ms</p>
            <p>Receive: {settings.receiveDelayMs}ms</p>
            <p>Round trip: {settings.sendDelayMs + settings.receiveDelayMs}ms</p>
          </div>
        </>
      )}

      {onClose && (
        <div className="mt-6">
          <button
            onClick={onClose}
            className="w-full py-2 px-4 bg-black text-white font-medium rounded hover:bg-gray-800 transition-colors"
          >
            Close
          </button>
        </div>
      )}
    </div>
  );
};