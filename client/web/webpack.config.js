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

if (isItchBuild && isCrazyGamesBuild) {
  throw new Error('ITCH_BUILD and CRAZYGAMES_BUILD are mutually exclusive release targets');
}

const isEmbeddedBuild = isItchBuild || isCrazyGamesBuild;

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
      },
    }),
    new webpack.DefinePlugin({
      'process.env.REACT_APP_WS_URL': JSON.stringify(process.env.REACT_APP_WS_URL || ''),
      'process.env.REACT_APP_API_URL': JSON.stringify(process.env.REACT_APP_API_URL || ''),
      'process.env.REACT_APP_ENVIRONMENT': JSON.stringify(process.env.REACT_APP_ENVIRONMENT || 'development'),
      'process.env.ITCH_BUILD': JSON.stringify(isItchBuild ? 'true' : ''),
      'process.env.CRAZYGAMES_BUILD': JSON.stringify(isCrazyGamesBuild ? 'true' : ''),
      'process.env.CRAZYGAMES_DATA_ENABLED': JSON.stringify(process.env.CRAZYGAMES_DATA_ENABLED || ''),
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
