import React, { useState, useRef, useEffect, useId } from 'react';
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
  const panelId = useId();
  const panelTitleId = useId();
  const triggerRef = useRef<HTMLButtonElement>(null);
  const collapseButtonRef = useRef<HTMLButtonElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const messagesContainerRef = useRef<HTMLDivElement>(null);
  const previousMessageCountRef = useRef(0);
  const hasInitializedRef = useRef(false);
  const hasManuallyCollapsedRef = useRef(false);
  const shouldFocusPanelRef = useRef(false);
  const shouldRestoreTriggerFocusRef = useRef(false);
  const canSendMessage = Boolean(currentUsername && isActive);

  const scrollToBottom = (behavior: ScrollBehavior = 'smooth') => {
    const messagesContainer = messagesContainerRef.current;
    if (isExpanded && messagesContainer) {
      const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
      messagesContainer.scrollTo({
        top: messagesContainer.scrollHeight,
        behavior: prefersReducedMotion ? 'auto' : behavior
      });
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

  useEffect(() => {
    if (isExpanded && shouldFocusPanelRef.current) {
      shouldFocusPanelRef.current = false;
      (canSendMessage ? inputRef.current : collapseButtonRef.current)?.focus();
    } else if (!isExpanded && shouldRestoreTriggerFocusRef.current) {
      shouldRestoreTriggerFocusRef.current = false;
      triggerRef.current?.focus();
    }
  }, [isExpanded, canSendMessage]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const trimmed = inputValue.trim();
    if (trimmed && currentUsername && isActive) {
      onSendMessage(trimmed);
      setInputValue('');
      scrollToBottom('auto');
    }
  };

  const handleCollapse = (event: React.MouseEvent<HTMLButtonElement>) => {
    shouldRestoreTriggerFocusRef.current = event.detail === 0;
    setIsExpanded(false);
    hasManuallyCollapsedRef.current = true;
  };

  const handleExpand = (event: React.MouseEvent<HTMLButtonElement>) => {
    shouldFocusPanelRef.current = event.detail === 0;
    setIsExpanded(true);
  };

  if (!initialExpanded) {
    return null; // Hidden in mobile mode
  }

  const statusMessage = !currentUsername
    ? 'Login to chat'
    : isActive
      ? null
      : inactiveMessage;

  const hasUnread = unreadCount > 0;
  const unreadLabel = unreadCount > 99 ? '99+' : unreadCount.toString();

  return (
    <div
      className={`home-lobby-chat fixed bottom-4 right-4 z-30 flex flex-col items-end gap-3 ${isExpanded ? 'is-expanded' : ''}`}
    >
      {!isExpanded && (
        <button
          ref={triggerRef}
          type="button"
          onClick={handleExpand}
          className={`home-chat-trigger ${hasUnread ? 'has-unread' : ''}`}
          aria-label={hasUnread
            ? `Open ${title}, ${unreadLabel} unread ${unreadCount === 1 ? 'message' : 'messages'}`
            : `Open ${title}`
          }
          aria-controls={panelId}
          aria-expanded={false}
        >
          <span
            className={`home-chat-icon ${hasUnread ? 'has-unread' : ''}`}
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
              <span className="absolute -top-1 -right-1 rounded bg-red-500 text-white text-[10px] font-bold px-1.5 py-0.5">
                {unreadLabel}
              </span>
            )}
          </span>
          <span className="home-chat-label">{title}</span>
        </button>
      )}

      {isExpanded && (
        <div
          id={panelId}
          className="home-chat-panel"
          role="region"
          aria-labelledby={panelTitleId}
        >
          <div className="home-chat-panel-header">
            <div id={panelTitleId} className="home-chat-panel-title">
              <span
                className={`home-chat-panel-status ${canSendMessage ? 'is-active' : ''}`}
                aria-hidden="true"
              />
              {title}
            </div>
            <button
              ref={collapseButtonRef}
              type="button"
              onClick={handleCollapse}
              className="home-chat-collapse"
              aria-label="Minimize chat"
              aria-controls={panelId}
              aria-expanded={true}
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

          <div className="home-chat-content">
            <div
              ref={messagesContainerRef}
              className="home-chat-messages"
              role="log"
              aria-live="polite"
              aria-relevant="additions text"
              aria-label={`${title} messages`}
            >
              {messages.length === 0 ? (
                <div className="home-chat-empty">
                  No messages yet
                </div>
              ) : (
                messages.map((msg) => (
                  <div key={msg.id} className="home-chat-message">
                    {msg.type === 'system' ? (
                      <div className="home-chat-system-message">
                        {msg.message}
                      </div>
                    ) : (
                      <div className="home-chat-player-message">
                        <span className="home-chat-username">{msg.username ?? 'Player'}</span>
                        <span className="home-chat-message-text">{msg.message}</span>
                      </div>
                    )}
                  </div>
                ))
              )}
            </div>

            {statusMessage ? (
              <div className="home-chat-unavailable">
                {statusMessage}
              </div>
            ) : (
              <form onSubmit={handleSubmit} className="home-chat-composer">
                <div className="home-chat-composer-row">
                  <input
                    ref={inputRef}
                    type="text"
                    value={inputValue}
                    onChange={(e) => setInputValue(e.target.value)}
                    placeholder="Say something..."
                    aria-label="Chat message"
                    className="home-chat-input"
                    maxLength={200}
                    disabled={!canSendMessage}
                  />
                  <button
                    type="submit"
                    disabled={!inputValue.trim() || !canSendMessage}
                    className={`
                      home-chat-send
                      ${inputValue.trim() && canSendMessage
                        ? 'is-ready'
                        : ''
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
