#!/usr/bin/env node
/**
 * Wrapper that runs prettier with the XML plugin.
 * Resolves the plugin relative to prettier's own location so it works
 * both locally and in pre-commit's isolated node environment.
 */
const path = require("path");
const { execFileSync } = require("child_process");

// Find node_modules by locating prettier's package directory
const prettierDir = path.dirname(require.resolve("prettier/package.json"));
const nodeModules = path.dirname(prettierDir);
const pluginPath = path.join(
  nodeModules,
  "@prettier",
  "plugin-xml",
  "src",
  "plugin.js",
);
const args = ["--write", "--plugin=" + pluginPath, ...process.argv.slice(2)];

try {
  execFileSync("prettier", args, {
    stdio: "inherit",
    env: { ...process.env, NODE_PATH: nodeModules },
  });
} catch (error) {
  process.exit(error.status || 1);
}
