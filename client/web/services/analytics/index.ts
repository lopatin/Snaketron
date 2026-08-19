/**
 * GameAnalytics integration.
 *
 * Read `ANALYTICS.md` in the repository root for how keys are configured and
 * how an operator keeps their own play out of the numbers.
 */

export { analytics, ERROR_SEVERITY } from './gameAnalytics.ts';
export type { ErrorSeverity, GameAnalyticsSdk } from './gameAnalytics.ts';
export { ANALYTICS_BUILD_CONFIG, ANALYTICS_SUPPORTED_DISTRIBUTION } from './config.ts';
export type { AccountDimension, InputDimension } from './config.ts';
export { queueIntentEvents } from './events.ts';
export { resolveAnalyticsDecision } from './exclusion.ts';
export type { AnalyticsDecision, ExclusionReason } from './exclusion.ts';
