import React, {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import { api } from '../services/api';
import { coerceBalance } from '../utils/walletChip';
import { useAuth } from './AuthContext';

/**
 * How many Snakebux the signed-in player has.
 *
 * A context rather than a fetch inside the header, because two places need the
 * same number and one of them already knows it sooner: the purchase route
 * answers with the post-transaction balance, so a buy can *push* the new figure
 * in rather than the header discovering it on a second round trip. That is the
 * difference between the chip updating as the dialog closes and it lagging a
 * request behind.
 */
interface WalletContextValue {
  /** `null` while unknown — signed out, still loading, or the fetch failed. */
  balanceBux: number | null;
  /**
   * Whether Snakebux exist as far as this player is concerned.
   *
   * `null` while unknown, and that is a third state rather than a slow `false`:
   * rendering "no Snakebux here" for the moment before the answer arrives, and
   * then popping a balance into the header, is worse than showing nothing and
   * then showing the truth.
   *
   * The signal is the shop being empty. `GET /api/wallet/packs` already returns
   * `[]` for both reasons Snakebux can be unavailable — a deployment with no
   * merchant account, and a distribution that may not sell — and the client
   * does not need to know which happened, only that there is no way to get any.
   *
   * Wiring it to Xsolla is therefore all it takes to turn the currency on:
   * the shop stops being empty and every surface guarded on this appears.
   */
  buxAvailable: boolean | null;
  refresh: () => Promise<void>;
  /**
   * Record a balance the caller already has, from a response that carried one.
   *
   * Always the server's number, never one computed here: only `purchased`
   * actually debits, `alreadyOwned` and `priceChanged` do not, and any of them
   * can race a credit landing from a payment provider. Subtracting a price
   * locally would be right most of the time, which is the worst kind of wrong
   * for money.
   */
  applyBalance: (balance: number | null) => void;
}

const WalletContext = createContext<WalletContextValue | null>(null);

export const WalletProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const { user } = useAuth();
  const [balanceBux, setBalanceBux] = useState<number | null>(null);
  const [buxAvailable, setBuxAvailable] = useState<boolean | null>(null);
  const requestSequence = useRef(0);

  const applyBalance = useCallback((balance: number | null) => {
    if (balance !== null) {
      // A pushed balance is newer than anything in flight, so it also cancels
      // whatever a slower fetch would have written over it.
      requestSequence.current += 1;
      setBalanceBux(balance);
    }
  }, []);

  // Keyed on the account's identity, not the object: equipping a skin
  // replaces `user` in place, and a balance does not change because a snake
  // changed colour.
  const userId = user?.id ?? null;
  const refresh = useCallback(async () => {
    // Guarded on the resolved user rather than on a stored token: the token is
    // read synchronously from localStorage while the account it names is
    // confirmed asynchronously, so a revoked session has a token and no user —
    // and `/api/wallet` is auth-gated, so fetching anyway throws on every page
    // load for every signed-out visitor.
    if (userId === null) {
      setBalanceBux(null);
      setBuxAvailable(null);
      return;
    }

    const sequence = (requestSequence.current += 1);
    const requestedFor = userId;
    try {
      const wallet = await api.getWallet();
      // Two guards, and they catch different things: the sequence discards a
      // slow response overtaken by a newer one, and the id discards a response
      // that belongs to whoever was signed in when it started. Painting
      // somebody else's balance is a support ticket.
      if (sequence === requestSequence.current && requestedFor === userId) {
        setBalanceBux(coerceBalance(wallet.balanceBux));
      }
    } catch {
      // A wallet that will not load is a chip that does not appear. There is
      // nothing here worth interrupting a player over, and no balance is
      // better than a hopeful zero — zero is itself a balance.
      if (sequence === requestSequence.current) {
        setBalanceBux(null);
      }
    }
  }, [userId]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  // Asked once per session rather than when the wallet is opened, because the
  // answer decides whether the *header* shows anything at all — and a header
  // that has to open a modal to find out what to render has already rendered.
  useEffect(() => {
    if (userId === null) {
      return undefined;
    }
    let cancelled = false;
    const ask = async (): Promise<void> => {
      // Retried once, because the failure mode is asymmetric. A shop that will
      // not load is not evidence either way, so the currency stays hidden —
      // which is right, but it means one dropped request at page load hides a
      // real balance for the rest of the session with nothing to prompt
      // another look. One retry costs a request and covers the transient case.
      for (let attempt = 0; attempt < 2 && !cancelled; attempt += 1) {
        try {
          const packs = await api.buxPacks();
          if (!cancelled) {
            setBuxAvailable(packs.length > 0);
          }
          return;
        } catch {
          if (attempt === 0) {
            await new Promise((resolve) => setTimeout(resolve, 3_000));
          }
        }
      }
      if (!cancelled) {
        setBuxAvailable(null);
      }
    };
    void ask();
    return () => {
      cancelled = true;
    };
  }, [userId]);

  const value = useMemo(
    () => ({ balanceBux, buxAvailable, refresh, applyBalance }),
    [applyBalance, balanceBux, buxAvailable, refresh],
  );

  return <WalletContext.Provider value={value}>{children}</WalletContext.Provider>;
};

export const useWallet = (): WalletContextValue => {
  const context = useContext(WalletContext);
  if (!context) {
    throw new Error('useWallet must be used within a WalletProvider');
  }
  return context;
};
