import React from 'react';
import { Link } from 'react-router-dom';
import { useCrazyGames } from '../contexts/CrazyGamesContext';
import { ANALYTICS_BUILD_CONFIG } from '../services/analytics';

/**
 * The player-facing privacy notice, reachable from the footer of every build.
 *
 * It was originally a CrazyGames-only page, because the portal requires a
 * notice from any game collecting personal data beyond its SDK's events. It is
 * now global for the same reason it was needed there: the website and itch
 * builds collect the same account and progression data, and all three report
 * analytics.
 *
 * Each section is shown only where it is true, so the page never describes a
 * data flow this build does not have.
 */
export const PrivacyPolicy: React.FC = () => {
  const { isCrazyGamesBuild } = useCrazyGames();
  // Analytics is compiled in, not configured at runtime, so a build without
  // keys genuinely reports nothing and must not claim otherwise.
  const reportsAnalytics = ANALYTICS_BUILD_CONFIG !== null;

  return (
    <main className="min-h-screen px-5 py-10 text-gray-900">
      <article className="mx-auto max-w-2xl border-2 border-black bg-white p-6 shadow-[8px_8px_0_#000] sm:p-10">
        <Link to="/" className="text-sm font-bold text-blue-700 hover:underline">
          ← Back to Snaketron
        </Link>
        <h1 className="mt-6 text-3xl font-black uppercase tracking-1">Privacy</h1>
        {isCrazyGamesBuild ? (
          <p className="mt-5 leading-7">
            Snaketron uses your CrazyGames account ID, username, and profile picture to sign you in
            automatically and keep your game progress available across devices.
          </p>
        ) : (
          <p className="mt-5 leading-7">
            Snaketron keeps your account and your game progress on its own servers so that your
            profile, rating, and match history follow you between sessions and devices.
          </p>
        )}

        <h2 className="mt-8 text-lg font-black uppercase">What Snaketron stores</h2>
        <ul className="mt-3 list-disc space-y-2 pl-6 leading-6">
          {isCrazyGamesBuild && (
            <li>Your verified CrazyGames account ID and current profile name and picture.</li>
          )}
          <li>Your Snaketron profile, XP, ratings, rankings, scores, match results, and history.</li>
          <li>Your tutorial, lobby, and control preferences.</li>
        </ul>

        <h2 className="mt-8 text-lg font-black uppercase">How sign-in works</h2>
        {isCrazyGamesBuild ? (
          <p className="mt-3 leading-7">
            CrazyGames gives Snaketron a short-lived sign-in token. Snaketron sends it directly to
            its server for verification and does not save it. We do not receive your CrazyGames
            password or email address. After verification, a separate Snaketron session token and a
            local preference mirror are kept only in the current browser tab, including across
            reloads; the server remains the source of truth for linked-account progress and
            settings. Signed-out guests can play without an account and keep their settings in that
            tab. When the current authenticated guest later signs in to a new CrazyGames-linked
            account, CrazyGames asks for permission before the server attaches eligible guest
            progress and settings in one verified transaction. If you decline, the
            CrazyGames-linked account starts without that guest data.
          </p>
        ) : (
          <p className="mt-3 leading-7">
            You can play as a guest without an account. If you create one, Snaketron stores your
            username and a hashed password, and keeps a session token in this browser so you stay
            signed in. Guest preferences are stored only in this browser until you create an
            account.
          </p>
        )}

        {reportsAnalytics && (
          <>
            <h2 className="mt-8 text-lg font-black uppercase">Analytics</h2>
            <p className="mt-3 leading-7">
              Snaketron sends gameplay analytics to{' '}
              <a
                className="font-bold text-blue-700 hover:underline"
                href="https://gameanalytics.com/privacy"
                target="_blank"
                rel="noopener noreferrer"
              >
                GameAnalytics
              </a>{' '}
              to understand which modes people play and where the game is too hard or too slow. This
              covers matches played, mode and queue, scores, match length, how a life ended, whether
              you play with a keyboard or touch controls, and your Snaketron account number.
              GameAnalytics also records the usual technical details of a web request, including
              your device, browser, and approximate country. We never send your username or email
              address{isCrazyGamesBuild ? ', and never your CrazyGames profile' : ''}. Your account
              number is meaningless to GameAnalytics on its own — only Snaketron can connect it to a
              player.
            </p>
            <p className="mt-3 leading-7">
              To opt out on this device, open{' '}
              <Link className="font-bold text-blue-700 hover:underline" to="/?analytics=off">
                snaketron.io/?analytics=off
              </Link>
              . The choice is remembered in this browser, and no analytics are sent from it again.
            </p>
          </>
        )}

        <h2 className="mt-8 text-lg font-black uppercase">Retention and deletion</h2>
        <p className="mt-3 leading-7">
          Account mappings and gameplay records are retained while needed to provide your Snaketron
          account, progression, multiplayer history, integrity, and security. You may request
          deletion or ask what is stored by emailing{' '}
          <a className="font-bold text-blue-700 hover:underline" href="mailto:alerts@snaketron.io">
            alerts@snaketron.io
          </a>.
          We will verify the request
          {isCrazyGamesBuild ? ' through your CrazyGames account' : ''}, revoke active sessions, and
          delete or anonymize eligible account, profile, preference, and gameplay records
          {reportsAnalytics ? ', including the analytics records held by GameAnalytics' : ''}.
          Records that must temporarily remain for security, legal obligations, or backup expiry are
          isolated and removed when that period ends.
        </p>

        <p className="mt-8 border-t border-gray-300 pt-5 text-sm text-gray-600">
          Last updated August 19, 2026.
        </p>
      </article>
    </main>
  );
};
