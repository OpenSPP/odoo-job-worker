#!/usr/bin/env node
/**
 * Wrapper that sets NODE_PATH so prettier can find plugins installed
 * in the pre-commit virtual environment's node_modules.
 */
const path = require("path");
const { execFileSync } = require("child_process");

const nodeModules = path.resolve(
  path.dirname(process.execPath),
  "..",
  "lib",
  "node_modules",
);
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
