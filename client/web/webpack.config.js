const CopyWebpackPlugin = require("copy-webpack-plugin");
const path = require('path');

module.exports = {
  entry: "./bootstrap.js",
  output: {
    path: path.resolve(__dirname, "dist"),
    filename: "bootstrap.js",
  },
  module: {
    rules: [
      {
        test: /\.(js|jsx)$/, // Target both .js and .jsx files
        exclude: /node_modules/, // IMPORTANT: Don't run babel on node_modules
        use: {
          loader: 'babel-loader'
          // Babel options are read from babel.config.js by default
        }
      },
      {
        test: /\.css$/i, // Regex to match .css files
        use: [
          'style-loader', // 2. Injects styles into DOM (adds <style> tags)
          'css-loader'    // 1. Translates CSS into CommonJS modules
        ],
      },
    ],
  },
  mode: "development",
  plugins: [
    new CopyWebpackPlugin(['index.html', 'SnaketronLogo.png'])
  ],
  experiments: {
    asyncWebAssembly: true,
  },
};
