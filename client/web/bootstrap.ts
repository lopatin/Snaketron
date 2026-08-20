// A dependency graph that contains any wasm must all be imported
// asynchronously. Initialize the portal SDK first so account availability and
// settings are resolved before React selects its session/storage path.
import { isScenarioCaptureMode } from './utils/scenarioCaptureMode';

const boot = isScenarioCaptureMode()
  // Capture must not wait for, or even initialize, an external portal SDK.
  ? import('./main')
  : import('./services/crazyGames')
    .then(async ({ crazyGames }) => {
      await crazyGames.init();
      crazyGames.loadingStart();
    })
    .catch((error) => {
      // Continue to the React shell so it can expose the deterministic account
      // error/guest path instead of leaving a blank iframe.
      console.error('CrazyGames initialization failed:', error);
    })
    .then(() => import('./main'));

boot.catch((error) => console.error('Error importing `main.tsx`:', error));
