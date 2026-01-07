const webpack = require('webpack');
const path = require('path');
const CopyWebpackPlugin = require("copy-webpack-plugin");
const HtmlWebpackPlugin = require('html-webpack-plugin');

const isProduction = process.env.NODE_ENV === 'production';

module.exports = {
  entry: "./bootstrap.ts",
  output: {
    path: path.resolve(__dirname, "dist"),
    filename: isProduction ? "[name].[contenthash].js" : "[name].js",
    chunkFilename: isProduction ? "[name].[contenthash].js" : "[name].js",
    assetModuleFilename: isProduction ? "[name].[contenthash][ext]" : "[name][ext]",
    publicPath: '/',
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
    new CopyWebpackPlugin([
      'SnaketronLogo.png',
      { from: 'public/images', to: 'images' }
    ]),
    new HtmlWebpackPlugin({
      template: path.resolve(__dirname, 'index.html'),
      filename: 'index.html',
      scriptLoading: 'defer',
      inject: 'body',
    }),
    new webpack.DefinePlugin({
      'process.env.REACT_APP_WS_URL': JSON.stringify(process.env.REACT_APP_WS_URL || ''),
      'process.env.REACT_APP_API_URL': JSON.stringify(process.env.REACT_APP_API_URL || ''),
      'process.env.REACT_APP_ENVIRONMENT': JSON.stringify(process.env.REACT_APP_ENVIRONMENT || 'development'),
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
