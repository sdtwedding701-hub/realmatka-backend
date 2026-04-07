import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const backendRoot = path.resolve(__dirname, "..");
const workspaceRoot = path.resolve(backendRoot, "..");

async function loadEnvFile(filePath, { override = false } = {}) {
  try {
    const raw = await readFile(filePath, "utf8");
    for (const line of raw.split(/\r?\n/)) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) continue;
      const separatorIndex = trimmed.indexOf("=");
      if (separatorIndex === -1) continue;
      const key = trimmed.slice(0, separatorIndex).trim();
      if (!key || (!override && process.env[key] != null)) continue;
      const value = trimmed.slice(separatorIndex + 1).trim().replace(/^['"]|['"]$/g, "");
      process.env[key] = value;
    }
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
}

async function main() {
  await loadEnvFile(path.join(backendRoot, ".env.production"));
  await loadEnvFile(path.join(backendRoot, ".env.local"));
  await loadEnvFile(path.join(workspaceRoot, ".env.production"));
  await loadEnvFile(path.join(workspaceRoot, ".env.local"));
  await loadEnvFile(path.join(workspaceRoot, ".env.backend.local"), { override: true });

  const { upsertChartRecord } = await import("../standalone/db.mjs");

  const inputPath = process.argv[2];
  if (!inputPath) {
    throw new Error("Usage: node scripts/import-chart-seed.mjs <chart-seed.json>");
  }

  const absolutePath = path.resolve(inputPath);
  const payload = JSON.parse(await readFile(absolutePath, "utf8"));
  const slug = String(payload.slug || "").trim();
  const jodi = Array.isArray(payload.jodi) ? payload.jodi : [];
  const panna = Array.isArray(payload.panna) ? payload.panna : [];

  if (!slug) {
    throw new Error("Chart seed must include slug");
  }
  if (!jodi.length || !panna.length) {
    throw new Error("Chart seed must include both jodi and panna rows");
  }

  await upsertChartRecord(slug, "jodi", jodi);
  await upsertChartRecord(slug, "panna", panna);

  console.log(`Imported chart seed for ${slug}`);
  console.log(`Jodi rows: ${jodi.length}`);
  console.log(`Panna rows: ${panna.length}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
