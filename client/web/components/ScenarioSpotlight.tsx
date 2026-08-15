import React from 'react';
import { scenarioFixtureById } from '../scenarios';
import ScenarioCanvas, { type ScenarioCanvasSource } from './ScenarioCanvas';
import './ScenarioCanvas.css';

const SPOTLIGHT = scenarioFixtureById('demolition-cutoff');
const SPOTLIGHT_SOURCE: ScenarioCanvasSource = {
  kind: 'script',
  script: SPOTLIGHT.script,
};

/** Static marketing embed: deliberately independent of auth and sockets. */
export const ScenarioSpotlight: React.FC = () => (
  <aside
    className="scenario-marketing"
    aria-labelledby="scenario-marketing-title"
    data-testid="scenario-marketing"
  >
    <header className="scenario-marketing__header">
      <div>
        <p className="scenario-marketing__eyebrow">Replay desk</p>
        <h2 id="scenario-marketing-title">See the turn</h2>
      </div>
      <span className="scenario-marketing__tag">Real engine</span>
    </header>

    <ScenarioCanvas
      source={SPOTLIGHT_SOURCE}
      autoPlay
      loop
      controls
      aspectRatio={16 / 10}
      label="Cut-off demolition featured replay"
    />

    <p className="scenario-marketing__copy">
      <strong>Take the angle</strong>
      <span>One authored play, rendered by the same rules as every live arena.</span>
    </p>
  </aside>
);
