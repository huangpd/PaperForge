// Language-switch check of the running console (needs Chrome + `npm i --no-save playwright`): node frontend/ui-check-i18n.mjs <output-dir>
import { chromium } from "playwright";
const out = process.argv[2];
const browser = await chromium.launch({ channel: "chrome" });
const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, locale: "en-US" });
const page = await ctx.newPage();
const problems = [];
page.on("console", (m) => m.type() === "error" && problems.push(m.text().slice(0, 160)));
page.on("pageerror", (e) => problems.push(String(e).slice(0, 160)));
const check = (n, ok) => console.log(ok ? "PASS" : "FAIL", n);
const cjk = /[一-鿿]/;
await page.goto("http://localhost:5173/#/", { waitUntil: "networkidle" });
check("defaults to English for an English browser", (await page.locator("h1").innerText()) === "Overview");
await page.click('.lang button:has-text("中文")'); await page.waitForTimeout(400);
check("toggle switches the page title to Chinese", (await page.locator("h1").innerText()) === "总览");
check("document lang updated", (await page.evaluate(() => document.documentElement.lang)) === "zh-CN");
await page.reload({ waitUntil: "networkidle" });
check("choice persists after reload", (await page.locator("h1").innerText()) === "总览");

const pages = [["", "总览"], ["pipeline", "流水线"], ["explorer", "数据浏览"], ["quality", "数据质量"], ["lineage", "数据血缘"], ["catalog", "数据目录"]];
for (const [p, h1] of pages) {
  await page.goto(`http://localhost:5173/#/${p}`, { waitUntil: "networkidle" });
  await page.waitForTimeout(1200);
  const title = await page.locator("h1").innerText();
  const lines = (await page.locator("main").innerText()).split("\n").map((s) => s.trim()).filter(Boolean);
  // static UI words only: short, letters-only, no digits/punctuation typical of data, not code-ish
  const english = [...new Set(lines.filter((s) => !cjk.test(s) && /^[A-Za-z][A-Za-z /↔→&-]{2,38}$/.test(s)))];
  console.log(`${title === h1 ? "PASS" : "FAIL"} ${p || "overview"}: h1=${title}; untranslated-looking lines: ${JSON.stringify(english.slice(0, 14))}`);
}
// explorer drawer in Chinese
await page.goto("http://localhost:5173/#/explorer", { waitUntil: "networkidle" });
await page.locator("tbody tr.click").first().click(); await page.waitForTimeout(800);
const tabs = await page.locator(".drawer .tabs button").allInnerTexts();
check("drawer tabs translated: " + tabs.join(" | "), tabs[0] === "标准化结果" && tabs[1] === "原始 vs 标准化" && tabs[2].startsWith("版本"));
await page.click('.drawer .tabs button:has-text("版本")'); await page.waitForTimeout(500);
check("status/current badge translated in versions", (await page.locator(".drawer .badge", { hasText: "当前" }).count()) === 1);
await page.screenshot({ path: `${out}/shots/zh-explorer.png` });
// quality page chart + status badges + filter options in Chinese
await page.goto("http://localhost:5173/#/quality", { waitUntil: "networkidle" }); await page.waitForTimeout(1200);
const opts = await page.locator("select").first().locator("option").allInnerTexts();
check("severity filter options translated: " + opts.join("/"), opts.join("/") === "全部级别/错误/警告/指标");
await page.screenshot({ path: `${out}/shots/zh-quality.png` });
await page.selectOption("select >> nth=0", "error"); await page.waitForTimeout(300);
const sev = await page.locator("tbody td:nth-child(4) .badge").allInnerTexts();
check("filter still works by value while labels are Chinese", sev.length > 0 && sev.every((s) => s === "错误"));
await page.goto("http://localhost:5173/#/pipeline", { waitUntil: "networkidle" }); await page.waitForTimeout(1200);
await page.screenshot({ path: `${out}/shots/zh-pipeline.png` });
// back to English
await page.click('.lang button:has-text("EN")'); await page.waitForTimeout(400);
check("switching back restores English", (await page.locator("h1").innerText()) === "Pipeline" && (await page.locator(".stage h4").first().innerText()).includes("Ingest"));
// a browser with Chinese locale and no stored choice starts in Chinese
const ctx2 = await browser.newContext({ locale: "zh-CN" }); const p2 = await ctx2.newPage();
await p2.goto("http://localhost:5173/#/", { waitUntil: "networkidle" });
check("Chinese browser starts in Chinese", (await p2.locator("h1").innerText()) === "总览");
console.log(problems.length ? "PROBLEMS:\n" + problems.join("\n") : "no console errors");
await browser.close();
