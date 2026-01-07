module.exports = {
    presets: [
        '@babel/preset-env', // Handles modern JavaScript syntax
        // Configures preset-react for JSX transformation
        // 'runtime: automatic' avoids needing `import React from 'react'` in every file just for JSX (requires React 17+)
        ['@babel/preset-react', { runtime: 'automatic' }],
        '@babel/preset-typescript' // Handles TypeScript syntax
    ]
};