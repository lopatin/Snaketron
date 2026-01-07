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
  /**
   * When false the component stays unmounted. Used to hide chat on mobile layouts.
   */
  initialExpanded?: boolean;
  /**
    * Enables the auto-open behavior when new messages arrive.
    * Should be false while the user is in an active match.
    */
  autoOpenEligible?: boolean;
}

export const LobbyChat: React.FC<LobbyChatUIProps> = ({
  messages,
  onSendMessage,
  currentUsername,
  title = 'Lobby Chat',
  isActive = true,
  inactiveMessage = 'Chat inactive',
  initialExpanded = false,
  autoOpenEligible = true
}) => {
  const [inputValue, setInputValue] = useState('');
  const [isExpanded, setIsExpanded] = useState(false);
  const [unreadCount, setUnreadCount] = useState(0);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const previousMessageCountRef = useRef(0);
  const hasInitializedRef = useRef(false);
  const hasManuallyCollapsedRef = useRef(false);

  const scrollToBottom = (behavior: ScrollBehavior = 'smooth') => {
    if (isExpanded) {
      messagesEndRef.current?.scrollIntoView({ behavior });
    }
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages, isExpanded]);

  useEffect(() => {
    if (!hasInitializedRef.current) {
      previousMessageCountRef.current = messages.length;
      hasInitializedRef.current = true;
      return;
    }

    if (messages.length > previousMessageCountRef.current) {
      const delta = messages.length - previousMessageCountRef.current;

      if (!isExpanded) {
        setUnreadCount((current) => current + delta);
        if (autoOpenEligible && !hasManuallyCollapsedRef.current) {
          setIsExpanded(true);
        }
      }
    } else if (messages.length < previousMessageCountRef.current) {
      setUnreadCount(0);
    }

    previousMessageCountRef.current = messages.length;
  }, [messages.length, isExpanded, autoOpenEligible]);

  useEffect(() => {
    if (isExpanded) {
      setUnreadCount(0);
    }
  }, [isExpanded]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const trimmed = inputValue.trim();
    if (trimmed && currentUsername && isActive) {
      onSendMessage(trimmed);
      setInputValue('');
      scrollToBottom('auto');
    }
  };

  const handleCollapse = () => {
    setIsExpanded(false);
    hasManuallyCollapsedRef.current = true;
  };

  const handleExpand = () => {
    setIsExpanded(true);
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

  const hasUnread = unreadCount > 0;
  const unreadLabel = unreadCount > 99 ? '99+' : unreadCount.toString();

  return (
    <div className="fixed bottom-4 right-4 z-30 flex flex-col items-end gap-3">
      {!isExpanded && (
        <button
          type="button"
          onClick={handleExpand}
          className={`
            group relative flex items-center gap-2 rounded-full border-2 px-3 py-1.5 font-bold uppercase tracking-1 text-xs
            transition-transform duration-150 cursor-pointer
            ${hasUnread
              ? 'border-black-70 text-black-70 bg-white hover:-translate-y-0.5'
              : 'border-gray-300 text-gray-500 bg-white/90 opacity-80'
            }
          `}
          aria-label="Open chat"
        >
          <span
            className={`
              relative -ml-1 flex h-8 w-8 items-center justify-center rounded-full
              ${hasUnread ? 'bg-black-70 text-white' : 'bg-gray-200 text-gray-600'}
            `}
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="1.5"
              className="h-4 w-4"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M5 5h10a3 3 0 013 3v4a3 3 0 01-3 3H9l-4 3V8a3 3 0 013-3z"
              />
            </svg>
            {hasUnread && (
              <span className="absolute -top-1 -right-1 rounded-full bg-rose-500 text-white text-[10px] font-bold px-1.5 py-0.5 shadow">
                {unreadLabel}
              </span>
            )}
          </span>
          <span className="text-sm font-semibold normal-case">{title}</span>
        </button>
      )}

      {isExpanded && (
        <div className="w-80 rounded-2xl border border-black/20 bg-white shadow-lg overflow-hidden">
          <div className="flex items-center justify-between px-4 py-3 bg-white">
            <div className="text-xs font-semibold uppercase tracking-1 text-black-70">
              {title}
            </div>
            <button
              type="button"
              onClick={handleCollapse}
              className="flex h-7 w-7 items-center justify-center text-black-70 hover:text-black cursor-pointer transition-colors"
              aria-label="Minimize chat"
            >
              <svg
                xmlns="http://www.w3.org/2000/svg"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="1.8"
                className="h-4 w-4"
              >
                <path strokeLinecap="round" strokeLinejoin="round" d="M6 15h12" />
              </svg>
            </button>
          </div>

          <div className="flex flex-col" style={{ maxHeight: '320px' }}>
            <div className="flex-1 overflow-y-auto px-4 pt-2 pb-3 space-y-1">
              {messages.length === 0 ? (
                <div className="text-xs text-gray-500 italic py-4">
                  No messages yet
                </div>
              ) : (
                messages.map((msg) => (
                  <div key={msg.id}>
                    {msg.type === 'system' ? (
                      <div className="text-xs text-gray-500 italic py-0.5">
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

            {statusMessage ? (
              <div className="px-4 pb-3 text-xs text-gray-500 italic">
                {statusMessage}
              </div>
            ) : (
              <form onSubmit={handleSubmit} className="px-4 pb-4 pt-2">
                <div className="flex gap-2">
                  <input
                    type="text"
                    value={inputValue}
                    onChange={(e) => setInputValue(e.target.value)}
                    placeholder="Say something..."
                    className="flex-1 px-3 py-2 text-xs bg-white border border-gray-300 rounded-lg focus:outline-none focus:border-black-70 transition-colors"
                    maxLength={200}
                    disabled={!canSendMessage}
                  />
                  <button
                    type="submit"
                    disabled={!inputValue.trim() || !canSendMessage}
                    className={`
                      px-3 py-2 rounded-lg font-bold uppercase text-[11px] tracking-1 border
                      transition-all
                      ${inputValue.trim() && canSendMessage
                        ? 'bg-white border-black-70 text-black-70 hover:bg-gray-50 cursor-pointer'
                        : 'bg-gray-50 border-gray-200 text-gray-400 cursor-not-allowed'
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
      )}
    </div>
  );
};
