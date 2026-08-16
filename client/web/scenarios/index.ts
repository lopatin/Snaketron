import type { ScenarioScript } from '../types';
import comboFrenzyJson from './combo-frenzy.json';
import demolitionCutoffJson from './demolition-cutoff.json';
import teamBankJson from './team-bank.json';

export type ScenarioFixtureTone = 'blue' | 'coral' | 'gold';

export interface ScenarioFixture {
  id: string;
  label: string;
  summary: string;
  callout: string;
  tone: ScenarioFixtureTone;
  script: ScenarioScript;
}

// Serde supplies compact-script defaults that ts-rs intentionally represents
// as the fully materialized Rust shape. The checked-in JSON is validated by
// common's scenario CI and again by ScenarioPlayer at the WASM boundary.
const asScenarioScript = (value: unknown): ScenarioScript => (
  value as ScenarioScript
);

export const SCENARIO_FIXTURES: readonly ScenarioFixture[] = [
  {
    id: 'demolition-cutoff',
    label: 'Cut-off demolition',
    summary: 'A two-snake lane cut ending in a body collision.',
    callout: 'Read the lane. Take the angle.',
    tone: 'coral',
    script: asScenarioScript(demolitionCutoffJson),
  },
  {
    id: 'combo-frenzy',
    label: 'Combo frenzy',
    summary: 'Eight rapid pickups climb through the full combo ladder.',
    callout: 'Keep the chain alive.',
    tone: 'blue',
    script: asScenarioScript(comboFrenzyJson),
  },
  {
    id: 'team-bank',
    label: 'Fourteen-point bank',
    summary: 'A loaded runner crosses home and cashes the carry.',
    callout: 'Bring the haul home.',
    tone: 'gold',
    script: asScenarioScript(teamBankJson),
  },
] as const;

export const scenarioFixtureById = (id: string | null | undefined): ScenarioFixture => (
  SCENARIO_FIXTURES.find((fixture) => fixture.id === id) ?? SCENARIO_FIXTURES[0]
);
