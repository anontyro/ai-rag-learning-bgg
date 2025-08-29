/*
 Loop enrichment until input dataset fully enriched.
 Usage:
   pnpm enrich:games:loop
 or
   tsx scripts/loop-enrich-boardgames.ts --input=../../data/boardgames_ranks.csv --output=../../data/boardgames_enriched.csv --limit=100 --interval=30

 Behavior:
 - Runs enrich-boardgames.ts in batches (limit each run).
 - Waits `interval` seconds between runs to allow easy cancellation without data loss.
 - Resumes automatically because enrich-boardgames.ts merges previous output.
*/

import { parse as csvParse } from "csv-parse";
import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";

function getArg(name: string, def?: string): string | undefined {
  const prefix = `--${name}=`;
  const arg = process.argv.find((a) => a.startsWith(prefix));
  return arg ? arg.slice(prefix.length) : def;
}

function sleep(ms: number) {
  return new Promise((res) => setTimeout(res, ms));
}

async function readCsv(filePath: string): Promise<Record<string, string>[]> {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(filePath)) return resolve([]);
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

async function countMissing(input: string, output: string): Promise<number> {
  const inputRows = await readCsv(input);
  const outRows = await readCsv(output);
  const outMap = new Map(outRows.map((r) => [String(r.id).trim(), r]));
  let missing = 0;
  for (const r of inputRows) {
    const id = String(r.id).trim();
    if (!id) continue;
    const o = outMap.get(id);
    const d = (o?.["description"] ?? "").toString().trim();
    if (!d) missing++;
  }
  return missing;
}

async function runEnrichOnce(args: string[]): Promise<number> {
  return new Promise((resolve) => {
    const tsxBinBase = path.resolve(process.cwd(), "node_modules/.bin/tsx");
    const tsxBin = process.platform === "win32" ? `${tsxBinBase}.cmd` : tsxBinBase;
    const cmd = fs.existsSync(tsxBin) ? tsxBin : "tsx"; // fallback to PATH
    const child = spawn(cmd, ["scripts/enrich-boardgames.ts", ...args], {
      stdio: "inherit",
      env: process.env,
    });
    child.on("close", (code) => resolve(code ?? 1));
  });
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
  const limit = parseInt(getArg("limit", "100")!, 10) || 100;
  const intervalSec = parseInt(getArg("interval", "30")!, 10) || 30;

  console.log(
    `Loop enrich starting with limit=${limit}, interval=${intervalSec}s\nInput: ${input}\nOutput: ${output}`
  );

  let stoppingRequested = false;
  let childRunning = false;
  let stopAfterChild = false;

  process.on("SIGINT", async () => {
    if (childRunning) {
      console.log("\nSIGINT received. Will stop after current batch completes and writes output...");
      stopAfterChild = true;
    } else {
      console.log("\nSIGINT received. Stopping before next batch. Progress is saved.");
      stoppingRequested = true;
    }
  });

  while (true) {
    if (stoppingRequested) break;

    // Check remaining before starting a new batch
    const remainingBefore = await countMissing(input, output);
    if (remainingBefore === 0) {
      console.log("All rows are enriched. Nothing to do.");
      break;
    }

    console.log(`Remaining to enrich: ${remainingBefore}`);

    // Run one enrich batch
    childRunning = true;
    const args = [
      `--input=${input}`,
      `--output=${output}`,
      `--limit=${limit}`,
    ];
    const code = await runEnrichOnce(args);
    childRunning = false;
    if (code !== 0) {
      console.error(`enrich-boardgames.ts exited with code ${code}. Stopping loop.`);
      break;
    }

    if (stopAfterChild) {
      console.log("Stopping as requested after the batch completed.");
      break;
    }

    const remainingAfter = await countMissing(input, output);
    console.log(`Remaining after batch: ${remainingAfter}`);
    if (remainingAfter === 0) {
      console.log("Enrichment complete. Exiting loop.");
      break;
    }

    // Wait with countdown to allow easy exit
    const total = Math.max(1, intervalSec);
    for (let s = total; s > 0; s--) {
      if (stoppingRequested) break;
      process.stdout.write(`Next batch in ${s}s...\r`);
      await sleep(1000);
    }
    process.stdout.write("\n");
    if (stoppingRequested) {
      console.log("Stopping before starting the next batch. Progress is saved.");
      break;
    }
  }

  console.log("Loop enrich finished.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
