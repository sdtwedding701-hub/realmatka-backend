import { readFile, readdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const backendRoot = path.resolve(__dirname, "..");
const workspaceRoot = path.resolve(backendRoot, "..");
const dataDir = path.join(workspaceRoot, "data");

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
  const files = (await readdir(dataDir))
    .filter((name) => name.endsWith(".chart.json"))
    .sort((left, right) => left.localeCompare(right));

  const imported = [];

  for (const fileName of files) {
    const absolutePath = path.join(dataDir, fileName);
    const payload = JSON.parse(await readFile(absolutePath, "utf8"));
    const slug = String(payload.slug || "").trim();
    const jodi = Array.isArray(payload.jodi) ? payload.jodi : [];
    const panna = Array.isArray(payload.panna) ? payload.panna : [];

    if (!slug || !jodi.length || !panna.length) {
      console.warn(`Skipped ${fileName}: missing slug/jodi/panna rows`);
      continue;
    }

    await upsertChartRecord(slug, "jodi", jodi);
    await upsertChartRecord(slug, "panna", panna);
    imported.push({ slug, fileName, jodiRows: jodi.length, pannaRows: panna.length });
    console.log(`Imported ${slug} from ${fileName}`);
  }

  console.log(JSON.stringify({ imported }, null, 2));
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exitCode = 1;
});
