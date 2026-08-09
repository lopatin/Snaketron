// A dependency graph that contains any wasm must all be imported
// asynchronously. Initialize the portal SDK first so account availability and
// settings are resolved before React selects its session/storage path.
import('./services/crazyGames')
  .then(async ({ crazyGames }) => {
    await crazyGames.init();
    crazyGames.loadingStart();
  })
  .catch((error) => {
    // Continue to the React shell so it can expose the deterministic account
    // error/guest path instead of leaving a blank iframe.
    console.error('CrazyGames initialization failed:', error);
  })
  .then(() => import('./main'))
  .catch((error) => console.error('Error importing `main.tsx`:', error));
