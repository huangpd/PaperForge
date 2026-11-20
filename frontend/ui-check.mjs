// Browser-level check of the running console (real API, real data). Needs Google Chrome and `npm i --no-save playwright`.
//   make up && node frontend/ui-check.mjs <output-dir>      (screenshots go to <output-dir>/shots; create it first)
import { chromium } from "playwright";
const out = process.argv[2];
const browser = await chromium.launch({ channel: "chrome" });
const page = await (await browser.newContext({ viewport: { width: 1440, height: 900 }, locale: "en-US" })).newPage();
const problems = [];
page.on("console", (m) => m.type() === "error" && problems.push(`console: ${m.text().slice(0, 160)}`));
page.on("pageerror", (e) => problems.push(`pageerror: ${String(e).slice(0, 160)}`));
page.on("response", (r) => r.url().includes("/api/") && r.status() >= 400 && problems.push(`http ${r.status()} ${r.url()}`));
const base = "http://localhost:5173/#";
const check = (name, ok) => console.log(ok ? "PASS" : "FAIL", name);

// Explorer: search, open drawer, three tabs
await page.goto(base + "/explorer", { waitUntil: "networkidle" });
await page.fill('input[placeholder="title, journal, author"]', "genomics");
await page.click('button:has-text("Search")'); await page.waitForTimeout(700);
const total = await page.locator(".card-h h3").nth(0).innerText();
check("search shows filtered total: " + total, /papers/.test(total) && !total.startsWith("101,000"));
await page.locator("tbody tr.click").first().click(); await page.waitForTimeout(800);
check("drawer opens with standardized view", await page.locator(".drawer .dl").count() > 0);
await page.click('.drawer .tabs button:has-text("Raw vs standardized")'); await page.waitForTimeout(500);
const pres = await page.locator(".drawer pre.json").allInnerTexts();
check("raw and standardized JSON side by side", pres.length === 2 && pres[0].includes('"DOI"') && pres[1].includes('"content_hash"') === false && pres[1].includes('"doi"'));
await page.screenshot({ path: `${out}/shots/explorer-compare.png` });
await page.click('.drawer .tabs button:has-text("Versions")'); await page.waitForTimeout(500);
check("versions tab lists the current version", (await page.locator(".drawer table tbody tr").count()) >= 1 && (await page.locator(".drawer .badge.ok", { hasText: "current" }).count()) === 1);
await page.screenshot({ path: `${out}/shots/explorer-versions.png` });
const dl = await page.evaluate(async () => (await fetch("/api/v1/papers/export?format=csv&limit=2&q=genomics")).text());
check("export link returns csv", dl.startsWith("doi,title"));

// Lineage: click a node, then jump to catalog
await page.goto(base + "/lineage", { waitUntil: "networkidle" }); await page.waitForTimeout(900);
await page.screenshot({ path: `${out}/shots/lineage.png` });
const box = await page.locator("canvas").first().boundingBox();
let hit = false;
for (let x = box.x + 20; x < box.x + box.width - 20 && !hit; x += 14) {   // scan for a node under the pointer (no DOM for canvas nodes)
  for (let y = box.y + box.height / 2 - 120; y < box.y + box.height / 2 + 120 && !hit; y += 14) {
    await page.mouse.move(x, y);
    if (await page.locator("div[style*='z-index'] b, .echarts-tooltip b").count()) { hit = true; await page.mouse.click(x, y); }
  }
}
await page.waitForTimeout(500);
check("clicking a lineage node shows its neighbours", (await page.locator("text=Upstream (").count()) > 0);
await page.screenshot({ path: `${out}/shots/lineage-selected.png` });

// Pipeline + catalog link
await page.goto(base + "/pipeline", { waitUntil: "networkidle" });
await page.waitForSelector(".stage", { timeout: 15000 });
const stageNames = await page.locator(".stage h4").allInnerTexts();
check(`pipeline stages deduplicated (${stageNames.length})`, stageNames.length > 5 && new Set(stageNames.map((s) => s.split("\n")[0])).size === stageNames.length);
check("no failed stage after the successful sync", (await page.locator(".stage.bad").count()) === 0);
await page.locator("tbody tr.click").first().click(); await page.waitForTimeout(300);
check("run row expands to its tasks", (await page.locator("td[colspan='7'] table").count()) === 1);
await page.screenshot({ path: `${out}/shots/pipeline.png` });
await page.goto(base + "/catalog?dataset=dws.journal_statistics", { waitUntil: "networkidle" }); await page.waitForTimeout(500);
check("catalog deep link shows dataset with columns", (await page.locator("h3:has-text('Columns')").count()) === 1 && (await page.locator("text=incremental").count()) === 0);
await page.goto(base + "/quality", { waitUntil: "networkidle" });
await page.selectOption("select >> nth=0", "error"); await page.waitForTimeout(300);
const sevs = await page.locator("tbody td:nth-child(4) .badge").allInnerTexts();
check("quality severity filter works", sevs.length > 0 && sevs.every((s) => s === "error"));
console.log(problems.length ? "PROBLEMS:\n" + problems.join("\n") : "no console errors, no failed API calls");
await browser.close();
