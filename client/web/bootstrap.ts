// A dependency graph that contains any wasm must all be imported
// asynchronously. Initialize the portal SDK first so its cloud data and
// settings are ready before React reads any persisted preferences.
import('./services/crazyGames')
  .then(async ({ crazyGames }) => {
    await crazyGames.init();
    crazyGames.loadingStart();
  })
  .catch((error) => {
    // The game deliberately remains playable if the SDK script is blocked or
    // the build is hosted outside an enabled CrazyGames environment.
    console.error('CrazyGames initialization failed:', error);
  })
  .then(() => import('./main'))
  .catch((error) => console.error('Error importing `main.tsx`:', error));
