import React, { useState, useRef, useEffect } from 'react';
import { ChatMessage } from '../types';

interface LobbyChatProps {
  messages: ChatMessage[];
  onSendMessage: (message: string) => void;
  currentUsername?: string;
  title?: string;
  isActive?: boolean;
  inactiveMessage?: string;
}

interface LobbyChatUIProps extends LobbyChatProps {
  initialExpanded?: boolean;
}

export const LobbyChat: React.FC<LobbyChatUIProps> = ({
  messages,
  onSendMessage,
  currentUsername,
  title = 'Lobby Chat',
  isActive = true,
  inactiveMessage = 'Chat inactive',
  initialExpanded = false
}) => {
  const [inputValue, setInputValue] = useState('');
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const trimmed = inputValue.trim();
    if (trimmed && currentUsername && isActive) {
      onSendMessage(trimmed);
      setInputValue('');
    }
  };

  if (!initialExpanded) {
    return null; // Hidden in mobile mode
  }

  const canSendMessage = Boolean(currentUsername && isActive);
  const statusMessage = !currentUsername
    ? 'Login to chat'
    : isActive
      ? null
      : inactiveMessage;

  // Calculate dynamic height based on message count (capped at reasonable max)
  const baseHeight = 120;
  const maxHeight = 260;
  const messageHeight = Math.min(messages.length * 24 + baseHeight, maxHeight); // ~24px per message

  return (
    <div className="fixed bottom-4 right-4 z-30 w-80" style={{ maxHeight: '300px' }}>
      {/* Compact Chat Window */}
      <div
        className="relative flex flex-col"
        style={{
          background: 'linear-gradient(to top, rgba(255, 255, 255, 0.98) 0%, rgba(255, 255, 255, 0.98) 70%, rgba(255, 255, 255, 0) 100%)',
          borderRadius: '8px',
          height: `${messageHeight}px`,
          transition: 'height 0.3s ease'
        }}
      >
        <div className="px-4 pt-3 pb-1">
          <div className="text-xs font-bold uppercase tracking-1 text-black-70">
            {title}
          </div>
        </div>

        {/* Messages - No border, compact spacing */}
        <div className="flex-1 overflow-y-auto px-4 pt-2 pb-3 space-y-1">
          {messages.length === 0 ? (
            <div className="text-xs text-gray-400 italic py-4">
              No messages yet
            </div>
          ) : (
            messages.map((msg) => (
              <div key={msg.id}>
                {msg.type === 'system' ? (
                  <div className="text-xs text-gray-400 italic py-0.5">
                    {msg.message}
                  </div>
                ) : (
                  <div className="text-xs leading-relaxed">
                    <span className="font-bold text-black-70">{msg.username ?? 'Player'}:</span>
                    <span className="text-black-70 ml-1">{msg.message}</span>
                  </div>
                )}
              </div>
            ))
          )}
          <div ref={messagesEndRef} />
        </div>

        {/* Input - Compact, no border */}
        {statusMessage ? (
          <div className="px-4 pb-3 text-xs text-gray-400 italic">
            {statusMessage}
          </div>
        ) : (
          <form onSubmit={handleSubmit} className="px-4 pb-3">
            <div className="flex gap-2">
              <input
                type="text"
                value={inputValue}
                onChange={(e) => setInputValue(e.target.value)}
                placeholder="Say something..."
                className="flex-1 px-2 py-1 text-xs bg-white border border-gray-300 rounded focus:outline-none focus:border-black-70 transition-colors"
                maxLength={200}
                disabled={!canSendMessage}
              />
              <button
                type="submit"
                disabled={!inputValue.trim() || !canSendMessage}
                className={`
                  px-3 py-1 rounded font-bold uppercase text-xs tracking-1
                  transition-all
                  ${inputValue.trim() && canSendMessage
                    ? 'bg-black-70 text-white hover:bg-black cursor-pointer'
                    : 'bg-gray-300 text-gray-400 cursor-not-allowed'
                  }
                `}
              >
                Send
              </button>
            </div>
          </form>
        )}
      </div>
    </div>
  );
};
