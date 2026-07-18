import React from 'react';

export const SocialFooter: React.FC = () => {
  const currentYear = new Date().getFullYear();

  return (
    <footer className="home-social-footer">
      <div className="home-social-icon-row" aria-label="Snaketron social links">
        <a
          href="https://github.com/lopatin/snaketron"
          target="_blank"
          rel="noopener noreferrer"
          className="home-social-icon"
          aria-label="View Snaketron on GitHub"
          title="GitHub"
        >
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M12 .7C5.7.7.7 5.8.7 12.1c0 5 3.2 9.3 7.7 10.8.6.1.8-.3.8-.6v-2.2c-3.1.7-3.8-1.3-3.8-1.3-.5-1.3-1.3-1.7-1.3-1.7-1-.7.1-.7.1-.7 1.1.1 1.7 1.2 1.7 1.2 1 1.7 2.6 1.2 3.3.9.1-.7.4-1.2.7-1.5-2.5-.3-5.1-1.3-5.1-5.6 0-1.2.4-2.3 1.2-3.1-.1-.3-.5-1.4.1-3 0 0 1-.3 3.1 1.2a10.8 10.8 0 0 1 5.5 0c2.2-1.5 3.1-1.2 3.1-1.2.6 1.6.2 2.7.1 3 .8.8 1.2 1.9 1.2 3.1 0 4.4-2.6 5.3-5.1 5.6.4.3.8 1 .8 2.1v3.2c0 .3.2.7.8.6a11.4 11.4 0 0 0 7.7-10.8C23.3 5.8 18.3.7 12 .7Z" />
          </svg>
        </a>

        <span
          className="home-social-icon is-placeholder"
          role="img"
          aria-label="Twitter link coming soon"
          title="Twitter"
        >
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M23.95 4.57a10 10 0 0 1-2.82.78 4.96 4.96 0 0 0 2.16-2.73 9.9 9.9 0 0 1-3.13 1.19 4.92 4.92 0 0 0-8.38 4.48A13.97 13.97 0 0 1 1.64 3.16a4.82 4.82 0 0 0-.67 2.48c0 1.71.87 3.21 2.19 4.09a4.9 4.9 0 0 1-2.23-.61v.06a4.92 4.92 0 0 0 3.95 4.83 4.99 4.99 0 0 1-2.21.08 4.94 4.94 0 0 0 4.6 3.42 9.87 9.87 0 0 1-6.1 2.1c-.39 0-.78-.02-1.17-.07a13.99 13.99 0 0 0 7.56 2.21c9.05 0 14-7.5 14-13.99 0-.21 0-.42-.02-.63A9.94 9.94 0 0 0 24 4.59Z" />
          </svg>
        </span>

        <span
          className="home-social-icon is-placeholder"
          role="img"
          aria-label="Reddit link coming soon"
          title="Reddit"
        >
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M14.55 4.4 13.9 7.5c1.65.18 3.15.65 4.34 1.35a2.3 2.3 0 1 1 1.32 4.17c.02.18.03.36.03.54 0 3.35-3.4 6.07-7.59 6.07s-7.59-2.72-7.59-6.07c0-.18.01-.36.03-.54a2.3 2.3 0 1 1 1.32-4.17c1.64-.96 3.84-1.52 6.24-1.52.27 0 .54.01.8.02l.82-3.88 3.81.81a1.7 1.7 0 1 1-.28 1.3l-2.6-.55v-.63Zm-6.18 7.64a1.2 1.2 0 1 0 0 2.4 1.2 1.2 0 0 0 0-2.4Zm7.26 0a1.2 1.2 0 1 0 0 2.4 1.2 1.2 0 0 0 0-2.4Zm-.2 4.12a.67.67 0 0 0-.94-.08c-.63.53-1.5.79-2.49.79s-1.86-.26-2.49-.79a.67.67 0 1 0-.86 1.03c.88.73 2.04 1.1 3.35 1.1s2.47-.37 3.35-1.1a.67.67 0 0 0 .08-.95Z" />
          </svg>
        </span>

      </div>
      <p className="home-copyright">© {currentYear} Snaketron</p>
    </footer>
  );
};
