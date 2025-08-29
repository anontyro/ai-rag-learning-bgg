/*
 Enrich BoardGameGeek data from a CSV using BGG XML API.
 Usage:
   pnpm --filter chromadb-api enrich:games
 or
   pnpm --filter chromadb-api tsx scripts/enrich-boardgames.ts --input=../../data/boardgames_ranks.csv --output=../../data/boardgames_enriched.csv --limit=25
 Resumable:
   If the output file exists, previously enriched fields are merged and the script only enriches the next N missing rows.
*/

import { XMLParser } from "fast-xml-parser";
import { parse as csvParse } from "csv-parse";
import { stringify as csvStringify } from "csv-stringify";
import fs from "node:fs";
import path from "node:path";

// Simple CLI arg parser
function getArg(name: string, def?: string): string | undefined {
  const prefix = `--${name}=`;
  const arg = process.argv.find((a) => a.startsWith(prefix));
  return arg ? arg.slice(prefix.length) : def;
}

function sleep(ms: number) {
  return new Promise((res) => setTimeout(res, ms));
}

function toArray<T>(v: T | T[] | undefined | null): T[] {
  if (v == null) return [];
  return Array.isArray(v) ? v : [v];
}

// Fields we consider as enrichment outputs
const ENRICH_FIELDS = [
  "description",
  "minplayers",
  "maxplayers",
  "playingtime",
  "minage",
  "categories",
  "mechanics",
  "primaryName",
] as const;

async function readCsv(filePath: string): Promise<Record<string, string>[]> {
  return new Promise((resolve, reject) => {
    const rows: Record<string, string>[] = [];
    fs.createReadStream(filePath)
      .pipe(
        csvParse({
          columns: true,
          skip_empty_lines: true,
          trim: true,
        })
      )
      .on("data", (row) => rows.push(row))
      .on("end", () => resolve(rows))
      .on("error", reject);
  });
}

async function writeCsv(
  filePath: string,
  rows: Record<string, any>[]
): Promise<void> {
  return new Promise((resolve, reject) => {
    const headers = Array.from(
      rows.reduce<Set<string>>((acc, r) => {
        Object.keys(r).forEach((k) => acc.add(k));
        return acc;
      }, new Set<string>())
    );

    const stringifier = csvStringify({ header: true, columns: headers });
    const writable = fs.createWriteStream(filePath);
    stringifier.on("error", reject);
    writable.on("finish", resolve);
    stringifier.pipe(writable);
    for (const row of rows) stringifier.write(row);
    stringifier.end();
  });
}

function isRowEnriched(row: Record<string, any>): boolean {
  const d = (row["description"] ?? "").toString().trim();
  return d.length > 0; // description as primary completion signal
}

function mergeEnriched(
  base: Record<string, any>,
  existing?: Record<string, any>
): Record<string, any> {
  const merged: Record<string, any> = { ...base };
  if (existing) {
    for (const f of ENRICH_FIELDS) {
      if (
        existing[f] !== undefined &&
        existing[f] !== null &&
        existing[f] !== ""
      ) {
        merged[f] = existing[f];
      }
    }
  }
  // Ensure all enrich fields exist
  for (const f of ENRICH_FIELDS) {
    if (!(f in merged)) merged[f] = "";
  }
  return merged;
}

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "",
  trimValues: true,
  parseTagValue: true,
  parseAttributeValue: true,
});

async function fetchBggXml(id: string, attempt = 1): Promise<any | null> {
  const url = `https://boardgamegeek.com/xmlapi/boardgame/${encodeURIComponent(
    id
  )}?stats=1`;
  const MAX_ATTEMPTS = 6;
  try {
    const res = await fetch(url, {
      headers: { "User-Agent": "boardgames-rag-learning-script/0.1" },
    });
    if (!res.ok) {
      if (attempt < MAX_ATTEMPTS) {
        await sleep(1000 * attempt);
        return fetchBggXml(id, attempt + 1);
      }
      return null;
    }
    const xml = await res.text();
    const data = parser.parse(xml);
    return data;
  } catch (e) {
    if (attempt < MAX_ATTEMPTS) {
      await sleep(1000 * attempt);
      return fetchBggXml(id, attempt + 1);
    }
    return null;
  }
}

function extractEnrichment(id: string, xmlRoot: any) {
  // Legacy XML API shape: { boardgames: { boardgame: {...} } }
  const bgRoot = xmlRoot?.boardgames?.boardgame;
  if (!bgRoot) return null;
  const bg = Array.isArray(bgRoot) ? bgRoot[0] : bgRoot;

  const nameNodes = toArray(bg?.name);
  const primaryName =
    nameNodes.find((n: any) => n?.primary === true || n?.primary === "true")?.[
      "#text"
    ] ??
    nameNodes[0]?.["#text"] ??
    "";

  const description: string = (bg?.description ?? "").toString();
  const minplayers = Number(bg?.minplayers ?? "") || undefined;
  const maxplayers = Number(bg?.maxplayers ?? "") || undefined;
  const playingtime = Number(bg?.playingtime ?? "") || undefined;
  const minage = Number(bg?.age ?? bg?.minage ?? "") || undefined;

  const categories = toArray(bg?.boardgamecategory)
    .map((c: any) => (typeof c === "string" ? c : c?.["#text"] ?? ""))
    .filter(Boolean);
  const mechanics = toArray(bg?.boardgamemechanic)
    .map((m: any) => (typeof m === "string" ? m : m?.["#text"] ?? ""))
    .filter(Boolean);

  return {
    id,
    primaryName,
    description,
    minplayers,
    maxplayers,
    playingtime,
    minage,
    categories: categories.join("; "),
    mechanics: mechanics.join("; "),
  };
}

async function main() {
  const input = getArg(
    "input",
    path.resolve(process.cwd(), "../../data/boardgames_ranks.csv")
  )!;
  const output = getArg(
    "output",
    path.resolve(process.cwd(), "../../data/boardgames_enriched.csv")
  )!;
  const limitStr = getArg("limit", "25")!;
  const limit = Math.max(0, parseInt(limitStr, 10) || 25);

  if (!fs.existsSync(input)) {
    console.error(`Input CSV not found: ${input}`);
    process.exit(1);
  }

  console.log(`Reading CSV: ${input}`);
  const rows = await readCsv(input);
  console.log(`Rows: ${rows.length}`);

  // If output exists, read and build a map of existing enrichment by id
  let existingMap: Map<string, Record<string, any>> = new Map();
  if (fs.existsSync(output)) {
    try {
      console.log(`Found existing output. Resuming from: ${output}`);
      const existingRows = await readCsv(output);
      existingMap = new Map(existingRows.map((r) => [String(r.id).trim(), r]));
    } catch (e) {
      console.warn(
        "Failed to read existing output for resume; proceeding fresh.",
        e
      );
    }
  }

  // Merge existing enrichment into the working rows array
  const enriched: Record<string, any>[] = rows.map((r) => {
    const id = String(r.id).trim();
    const existing = existingMap.get(id);
    return mergeEnriched(r, existing);
  });

  // Determine indices still missing enrichment
  const missingIndices: number[] = [];
  for (let i = 0; i < enriched.length; i++) {
    if (!isRowEnriched(enriched[i])) {
      missingIndices.push(i);
    }
  }

  const toProcess = missingIndices.slice(0, limit);
  console.log(
    `Missing: ${missingIndices.length}. Processing next batch: ${toProcess.length}`
  );

  for (let batchIdx = 0; batchIdx < toProcess.length; batchIdx++) {
    const i = toProcess[batchIdx];
    const row = enriched[i];
    const id = String(row.id).trim();
    if (!id) continue;
    console.log(
      `[${batchIdx + 1}/${toProcess.length}] Fetching BGG data for id=${id}`
    );
    const xml = await fetchBggXml(id);
    const enrich = xml ? extractEnrichment(id, xml) : null;
    await sleep(500); // be polite to BGG API

    enriched[i] = {
      ...row,
      description: enrich?.description ?? "",
      minplayers: enrich?.minplayers ?? "",
      maxplayers: enrich?.maxplayers ?? "",
      playingtime: enrich?.playingtime ?? "",
      minage: enrich?.minage ?? "",
      categories: enrich?.categories ?? "",
      mechanics: enrich?.mechanics ?? "",
      primaryName: enrich?.primaryName ?? "",
    };
  }

  // Ensure output directory exists
  fs.mkdirSync(path.dirname(output), { recursive: true });

  console.log(`Writing enriched CSV: ${output}`);
  await writeCsv(output, enriched);
  console.log("Done.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
