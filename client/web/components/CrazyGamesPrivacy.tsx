import React from 'react';
import { Link } from 'react-router-dom';

export const CrazyGamesPrivacy: React.FC = () => (
  <main className="min-h-screen px-5 py-10 text-gray-900">
    <article className="mx-auto max-w-2xl border-2 border-black bg-white p-6 shadow-[8px_8px_0_#000] sm:p-10">
      <Link to="/" className="text-sm font-bold text-blue-700 hover:underline">
        ← Back to Snaketron
      </Link>
      <h1 className="mt-6 text-3xl font-black uppercase tracking-1">Privacy on CrazyGames</h1>
      <p className="mt-5 leading-7">
        Snaketron uses your CrazyGames account ID, username, and profile picture to sign you in
        automatically and keep your game progress available across devices.
      </p>

      <h2 className="mt-8 text-lg font-black uppercase">What Snaketron stores</h2>
      <ul className="mt-3 list-disc space-y-2 pl-6 leading-6">
        <li>Your verified CrazyGames account ID and current profile name and picture.</li>
        <li>Your Snaketron profile, XP, ratings, rankings, scores, match results, and history.</li>
        <li>Your tutorial, lobby, and control preferences.</li>
      </ul>

      <h2 className="mt-8 text-lg font-black uppercase">How sign-in works</h2>
      <p className="mt-3 leading-7">
        CrazyGames gives Snaketron a short-lived sign-in token. Snaketron sends it directly to its
        server for verification and does not save it. We do not receive your CrazyGames password or
        email address. After verification, a separate Snaketron session token and a local preference
        mirror are kept only in the current browser tab, including across reloads; the server remains
        the source of truth for linked-account progress and settings. Signed-out guests can play
        without an account and keep their settings in that tab. When the current authenticated guest
        later signs in to a new CrazyGames-linked account, CrazyGames asks for permission before the
        server attaches eligible guest progress and settings in one verified transaction. If you
        decline, the CrazyGames-linked account starts without that guest data.
      </p>

      <h2 className="mt-8 text-lg font-black uppercase">Retention and deletion</h2>
      <p className="mt-3 leading-7">
        Account mappings and gameplay records are retained while needed to provide your Snaketron
        account, progression, multiplayer history, integrity, and security. You may request deletion
        or ask what is stored by emailing{' '}
        <a className="font-bold text-blue-700 hover:underline" href="mailto:alerts@snaketron.io">
          alerts@snaketron.io
        </a>.
        We will verify the request through your CrazyGames account, revoke active sessions, and
        delete or anonymize eligible account, profile, preference, and gameplay records. Records that
        must temporarily remain for security, legal obligations, or backup expiry are isolated and
        removed when that period ends.
      </p>

      <p className="mt-8 border-t border-gray-300 pt-5 text-sm text-gray-600">
        Last updated August 9, 2026.
      </p>
    </article>
  </main>
);
