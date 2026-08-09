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
        { from: 'public/images', to: 'images' }
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
      'process.env.CRAZYGAMES_ADS_ENABLED': JSON.stringify(process.env.CRAZYGAMES_ADS_ENABLED || ''),
      'process.env.CRAZYGAMES_DATA_ENABLED': JSON.stringify(process.env.CRAZYGAMES_DATA_ENABLED || ''),
      'process.env.NODE_ENV': JSON.stringify(process.env.NODE_ENV || 'development')
    })
  ],
  experiments: {
    asyncWebAssembly: true,
  },
  devServer: {
    historyApiFallback: true,
    static: {
      directory: path.join(__dirname, 'dist'),
    },
    port: 3000,
    hot: true,
    open: false,
  },
};
