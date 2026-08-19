const webpack = require('webpack');
const path = require('path');
const CopyWebpackPlugin = require("copy-webpack-plugin");
const HtmlWebpackPlugin = require('html-webpack-plugin');

const isProduction = process.env.NODE_ENV === 'production';

// Optional itch.io HTML5 target (ITCH_BUILD=true): the bundle is served from
// a deep, unknown path on itch's static host (html-classic.itch.zone), so
// asset URLs must resolve relative to the page ('auto' publicPath + base
// href "./" in index.html) and routing must not rely on the History API.
const isItchBuild = process.env.ITCH_BUILD === 'true';
const isCrazyGamesBuild = process.env.CRAZYGAMES_BUILD === 'true';
// Factory evidence must not execute mutable third-party analytics code. The
// resulting cached bundle is otherwise the normal production web renderer.
const isFactoryRendererBuild = process.env.SNAKETRON_FACTORY_RENDERER_BUILD === 'true';

if (isItchBuild && isCrazyGamesBuild) {
  throw new Error('ITCH_BUILD and CRAZYGAMES_BUILD are mutually exclusive release targets');
}

const isEmbeddedBuild = isItchBuild || isCrazyGamesBuild;

// GameAnalytics ships only in the ordinary web build. The itch and CrazyGames
// packages are reviewed release artifacts whose packaging invariants exclude
// third-party trackers (see CRAZYGAMES.md), so the keys are dropped rather
// than merely left unused: an embedded bundle must not even contain them.
const gameAnalyticsGameKey = isEmbeddedBuild ? '' : (process.env.GAMEANALYTICS_GAME_KEY || '');
const gameAnalyticsSecretKey = isEmbeddedBuild ? '' : (process.env.GAMEANALYTICS_SECRET_KEY || '');

if (!isEmbeddedBuild && Boolean(gameAnalyticsGameKey) !== Boolean(gameAnalyticsSecretKey)) {
  throw new Error(
    'GAMEANALYTICS_GAME_KEY and GAMEANALYTICS_SECRET_KEY must be set together: '
    + 'GameAnalytics signs every request with the secret, so half a pair would '
    + 'fail every call at runtime instead of staying inert.',
  );
}

module.exports = {
  entry: "./bootstrap.ts",
  output: {
    path: path.resolve(__dirname, "dist"),
    filename: isProduction ? "[name].[contenthash].js" : "[name].js",
    chunkFilename: isProduction ? "[name].[contenthash].js" : "[name].js",
    assetModuleFilename: isProduction ? "[name].[contenthash][ext]" : "[name][ext]",
    publicPath: isEmbeddedBuild ? 'auto' : '/',
    clean: true,
  },
  resolve: {
    extensions: ['.ts', '.tsx', '.js', '.jsx'],
    // Resolve the checked-in/generated package beside this worktree directly.
    // A shared node_modules directory may contain a file: symlink into another
    // checkout, which would otherwise let stale WASM silently back the UI.
    alias: {
      'wasm-snaketron': path.resolve(__dirname, '../pkg'),
      // Stub the analytics SDK out of the reviewed release packages entirely.
      //
      // Dropping the keys is not enough on its own: the dynamic import is
      // still statically reachable, so webpack emits the ~93 KB vendor chunk
      // into the ZIP even though nothing will ever request it. `false` here
      // resolves it to an empty module, so the embedded artifact contains no
      // third-party tracker code at all — the invariant CRAZYGAMES.md states
      // and the packaging script checks.
      ...(isEmbeddedBuild ? { gameanalytics: false } : {}),
    },
  },
  module: {
    rules: [
      {
        test: /\.(ts|tsx|js|jsx)$/, // Target TypeScript and JavaScript files
        exclude: /node_modules/, // IMPORTANT: Don't run babel on node_modules
        use: {
          loader: 'babel-loader'
          // Babel options are read from babel.config.js by default
        }
      },
      {
        test: /\.css$/i, // Regex to match .css files
        use: [
          'style-loader', // 3. Injects styles into DOM (adds <style> tags)
          'css-loader',   // 2. Translates CSS into CommonJS modules
          'postcss-loader' // 1. Process CSS with PostCSS (Tailwind)
        ],
      },
    ],
  },
  mode: isProduction ? "production" : "development",
  plugins: [
    new CopyWebpackPlugin({
      patterns: [
        'SnaketronLogo.png',
        { from: 'public/images', to: 'images' },
        {
          from: path.resolve(
            __dirname,
            '../../.claude/skills/snaketron-create-video/assets/fonts/Inter-Variable.ttf',
          ),
          to: 'capture-fonts/Inter-Variable.ttf',
        },
        {
          from: path.resolve(
            __dirname,
            '../../.claude/skills/snaketron-create-video/assets/fonts/BarlowCondensed-ExtraBoldItalic.ttf',
          ),
          to: 'capture-fonts/BarlowCondensed-ExtraBoldItalic.ttf',
        },
      ]
    }),
    new HtmlWebpackPlugin({
      template: path.resolve(__dirname, 'index.html'),
      filename: 'index.html',
      scriptLoading: 'defer',
      inject: 'body',
      templateParameters: {
        itchBuild: isItchBuild,
        crazyGamesBuild: isCrazyGamesBuild,
        embeddedBuild: isEmbeddedBuild,
        factoryRendererBuild: isFactoryRendererBuild,
      },
    }),
    new webpack.DefinePlugin({
      'process.env.REACT_APP_WS_URL': JSON.stringify(process.env.REACT_APP_WS_URL || ''),
      'process.env.REACT_APP_API_URL': JSON.stringify(process.env.REACT_APP_API_URL || ''),
      'process.env.REACT_APP_ENVIRONMENT': JSON.stringify(process.env.REACT_APP_ENVIRONMENT || 'development'),
      'process.env.ITCH_BUILD': JSON.stringify(isItchBuild ? 'true' : ''),
      'process.env.CRAZYGAMES_BUILD': JSON.stringify(isCrazyGamesBuild ? 'true' : ''),
      'process.env.CRAZYGAMES_DATA_ENABLED': JSON.stringify(process.env.CRAZYGAMES_DATA_ENABLED || ''),
      // GameAnalytics keys are compiled in, not fetched, so a bundle either
      // reports or provably cannot. A checkout without them — every developer
      // machine, CI, and any fork — never loads the SDK. See ANALYTICS.md.
      'process.env.GAMEANALYTICS_GAME_KEY': JSON.stringify(gameAnalyticsGameKey),
      'process.env.GAMEANALYTICS_SECRET_KEY': JSON.stringify(gameAnalyticsSecretKey),
      'process.env.GAMEANALYTICS_BUILD': JSON.stringify(process.env.GAMEANALYTICS_BUILD || ''),
      'process.env.NODE_ENV': JSON.stringify(process.env.NODE_ENV || 'development')
    })
  ],
  experiments: {
    asyncWebAssembly: true,
  },
  devServer: {
    historyApiFallback: true,
    // The /qa/skins sidebar writes skin tuning back to the repo.
    //
    // Deliberately a dev-server middleware and nothing else: it exists only
    // while someone is running `npm start` on a checkout, so there is no
    // production surface to secure, and the file it writes is the same one
    // `client/src/skin/sprite.rs` compiles in. Live preview happens in wasm;
    // this is what makes the change outlive the page.
    setupMiddlewares: (middlewares, devServer) => {
      const fs = require('fs');
      const tuningPath = path.join(__dirname, '..', 'design', 'sprites', 'tuning.json');

      devServer.app.post('/qa/skin-tuning', (req, res) => {
        let body = '';
        req.on('data', (chunk) => {
          body += chunk;
          // A dev endpoint still should not be a way to fill the disk.
          if (body.length > 64 * 1024) req.destroy();
        });
        req.on('end', () => {
          let incoming;
          try {
            incoming = JSON.parse(body);
          } catch (error) {
            res.status(400).json({ error: 'not JSON' });
            return;
          }
          let current = {};
          try {
            current = JSON.parse(fs.readFileSync(tuningPath, 'utf8'));
          } catch (error) {
            // A missing or broken file is replaced rather than merged into.
          }
          for (const [id, values] of Object.entries(incoming)) {
            // Only ids the file already knows, and only the two numbers that
            // mean anything — so a typo cannot invent a skin or smuggle a
            // field the Rust side will silently ignore.
            if (!Object.prototype.hasOwnProperty.call(current, id)) continue;
            const speed = Number(values.anim_speed);
            const drift = Number(values.drift_cells);
            if (!Number.isFinite(speed) || !Number.isFinite(drift)) continue;
            current[id] = { anim_speed: speed, drift_cells: drift };
          }
          fs.writeFileSync(tuningPath, JSON.stringify(current, null, 2) + '\n');
          res.json({ saved: true, path: tuningPath });
        });
      });

      return middlewares;
    },
    static: {
      directory: path.join(__dirname, 'dist'),
    },
    port: 3000,
    hot: true,
    open: false,
  },
};
