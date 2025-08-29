/*
  Ingest boardgames into ChromaDB.
  - Reads a CSV (default: ../../data/boardgames_enriched.csv)
  - Creates/gets a Chroma collection (default: boardgames)
  - Builds documents from primaryName + description (HTML stripped / entities decoded)
  - Stores useful metadata (ranks, players/time, categories/mechanics arrays, etc.)
  - Optional: --embed uses Ollama to create vectors and include them on upsert

  Usage examples:
  tsx scripts/ingest-boardgames.ts \
    --input=../../data/boardgames_enriched.csv \
    --collection=boardgames \
    --limit=1000 \
    --embed --embeddingModel=nomic-embed-text

  Env:
  - CHROMA_URL (default http://localhost:8000)
  - OLLAMA_URL (default http://localhost:11434)
*/

import fs from "node:fs";
import path from "node:path";
import { parse as csvParse } from "csv-parse";
import { ChromaClient } from "chromadb";

// -------- CLI helpers --------
function getArg(name: string, def?: string): string | undefined {
  const flag = `--${name}=`;
  const found = process.argv.find((a) => a.startsWith(flag));
  if (found) return found.slice(flag.length);
  return def;
}

function getBoolArg(name: string): boolean {
  const flag = `--${name}`;
  return process.argv.includes(flag);
}

// -------- General utils --------
function decodeEntities(input: string): string {
  // Minimal entity decoding and HTML stripping sufficient for BGG text
  const map: Record<string, string> = {
    "&amp;": "&",
    "&quot;": '"',
    "&#039;": "'",
    "&apos;": "'",
    "&rsquo;": "'",
    "&lsquo;": "'",
    "&ldquo;": '"',
    "&rdquo;": '"',
    "&ndash;": "–",
    "&mdash;": "—",
  };
  let out = input.replace(/<br\s*\/?\s*>/gi, "\n");
  out = out.replace(/<[^>]+>/g, "");
  out = out.replace(/&[a-z#0-9]+;/gi, (m) => map[m] ?? m);
  // collapse whitespace
  out = out.replace(/\s+/g, " ").trim();
  return out;
}

function toNumber(x: any): number | undefined {
  const n = Number(String(x ?? "").trim());
  return Number.isFinite(n) ? n : undefined;
}

function toBool01(x: any): boolean | undefined {
  const s = String(x ?? "").trim();
  if (s === "1") return true;
  if (s === "0") return false;
  return undefined;
}

function splitSemiList(x: any): string[] | undefined {
  if (x == null) return undefined;
  const s = String(x);
  const arr = s
    .split(";")
    .map((p) => p.trim())
    .filter(Boolean);
  return arr.length ? arr : undefined;
}

async function readCsv(filePath: string): Promise<Record<string, string>[]> {
  return new Promise((resolve, reject) => {
    const rows: Record<string, string>[] = [];
    fs.createReadStream(filePath)
      .pipe(
        csvParse({
          columns: true,
          skipEmptyLines: true,
          trim: true,
        })
      )
      .on("data", (row) => rows.push(row))
      .on("end", () => resolve(rows))
      .on("error", reject);
  });
}

async function ensureCollection(client: ChromaClient, name: string) {
  return client.getOrCreateCollection({ name });
}

async function embedWithOllama(
  texts: string[],
  opts: { ollamaUrl: string; model: string }
): Promise<number[][]> {
  const url = new URL("/api/embeddings", opts.ollamaUrl).toString();
  const out: number[][] = [];
  for (const t of texts) {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ model: opts.model, input: t }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`Ollama embed failed: ${res.status} ${res.statusText} — ${body}`);
    }
    const json = (await res.json()) as any;
    const emb: number[] | undefined = json?.embedding;
    if (!emb || !Array.isArray(emb)) {
      throw new Error(`Unexpected embeddings response shape: ${JSON.stringify(json).slice(0, 200)}...`);
    }
    out.push(emb);
  }
  return out;
}

async function main() {
  const input = getArg("input", path.resolve(process.cwd(), "../../data/boardgames_enriched.csv"))!;
  const collectionName = getArg("collection", "boardgames")!;
  const limitStr = getArg("limit");
  const limit = limitStr ? Math.max(0, parseInt(limitStr, 10) || 0) : undefined;
  const doEmbed = getBoolArg("embed");
  const embeddingModel = getArg("embeddingModel", "nomic-embed-text")!;

  const CHROMA_URL = process.env.CHROMA_URL || "http://localhost:8000";
  const OLLAMA_URL = process.env.OLLAMA_URL || "http://localhost:11434";

  if (!fs.existsSync(input)) {
    console.error(`Input CSV not found: ${input}`);
    process.exit(1);
  }

  console.log(`Reading CSV: ${input}`);
  const rows = await readCsv(input);
  console.log(`Rows: ${rows.length}`);

  const sliced = typeof limit === "number" && limit > 0 ? rows.slice(0, limit) : rows;

  const client = new ChromaClient({ path: CHROMA_URL });
  const collection = await ensureCollection(client, collectionName);

  const batchSize = 100; // tune as needed
  let processed = 0;

  for (let i = 0; i < sliced.length; i += batchSize) {
    const batch = sliced.slice(i, i + batchSize);

    const ids: string[] = [];
    const documents: string[] = [];
    const metadatas: any[] = [];

    for (const r of batch) {
      const id = String(r.id ?? "").trim();
      if (!id) continue;

      const primaryName = (r.primaryName ?? r.name ?? "").toString();
      const description = decodeEntities((r.description ?? "").toString());
      const document = [primaryName, description].filter(Boolean).join("\n\n");

      // collect ranks automatically
      const rankFields: Record<string, number | undefined> = {};
      for (const [k, v] of Object.entries(r)) {
        if (k.endsWith("_rank") || k === "rank") {
          const n = toNumber(v);
          if (n !== undefined) rankFields[k] = n;
        }
      }

      const metadata = {
        name: r.name ?? undefined,
        primaryName: primaryName || undefined,
        yearpublished: toNumber(r.yearpublished),
        bayesaverage: toNumber(r.bayesaverage),
        average: toNumber(r.average),
        usersrated: toNumber(r.usersrated),
        is_expansion: toBool01(r.is_expansion),
        minplayers: toNumber(r.minplayers),
        maxplayers: toNumber(r.maxplayers),
        playingtime: toNumber(r.playingtime),
        minage: toNumber(r.minage),
        categories: splitSemiList(r.categories),
        mechanics: splitSemiList(r.mechanics),
        ...rankFields,
      };

      ids.push(id);
      documents.push(document);
      metadatas.push(metadata);
    }

    let embeddings: number[][] | undefined = undefined;
    if (doEmbed) {
      console.log(`Embedding ${ids.length} docs via Ollama (${embeddingModel})...`);
      embeddings = await embedWithOllama(documents, { ollamaUrl: OLLAMA_URL, model: embeddingModel });
    }

    if (ids.length) {
      console.log(`Upserting batch ${i / batchSize + 1} — size ${ids.length}`);
      await collection.upsert({ ids, documents, metadatas, embeddings });
      processed += ids.length;
    }
  }

  console.log(`Done. Processed: ${processed}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
