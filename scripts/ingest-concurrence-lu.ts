/**
 * Ingestion crawler for the Conseil de la concurrence / Autorite de la
 * concurrence du Luxembourg (concurrence.public.lu).
 *
 * Scrapes decisions, opinions (avis), engagements, and merger-related decisions
 * and inserts them into the SQLite database used by the CdlC MCP server.
 *
 * The site is organised under:
 *   /fr/decisions/ententes/                — cartel decisions
 *   /fr/decisions/abus-de-position-dominante/ — abuse of dominance
 *   /fr/decisions/classements/             — dismissed complaints
 *   /fr/decisions/mesures-conservatoires/   — interim measures
 *   /fr/decisions/amendes-astreintes/       — fines & penalty payments
 *   /fr/decisions/engagements/             — commitment decisions
 *   /fr/avis-enquetes/avis/                — opinions on draft legislation
 *   /fr/avis-enquetes/enquetes/            — sector inquiries
 *
 * Decision pages live at /{category}/{year}/{slug}.html and may link to a
 * PDF under /content/dam/concurrence/fr/decisions/... or /dam-assets/...
 *
 * Usage:
 *   npx tsx scripts/ingest-concurrence-lu.ts                 # full crawl
 *   npx tsx scripts/ingest-concurrence-lu.ts --resume        # skip already-ingested case numbers
 *   npx tsx scripts/ingest-concurrence-lu.ts --dry-run       # parse but do not write to DB
 *   npx tsx scripts/ingest-concurrence-lu.ts --force         # delete DB and start fresh
 *   npx tsx scripts/ingest-concurrence-lu.ts --max-pages 3   # limit listing pages per category
 *   npx tsx scripts/ingest-concurrence-lu.ts --decisions-only # skip opinions/inquiries
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";
import * as cheerio from "cheerio";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const BASE_URL = "https://concurrence.public.lu";
const DB_PATH = process.env["CDLC_DB_PATH"] ?? "data/cdlc.db";
const RATE_LIMIT_MS = 1_500;
const MAX_RETRIES = 3;
const RETRY_BACKOFF_MS = 3_000;
const REQUEST_TIMEOUT_MS = 30_000;

/**
 * Decision categories on concurrence.public.lu.
 * Each entry maps to a listing page under /fr/decisions/ or /fr/avis-enquetes/.
 * `kind` indicates whether items go into the decisions or mergers table.
 */
const DECISION_CATEGORIES: Array<{
  slug: string;
  label: string;
  basePath: string;
  kind: "decision" | "opinion" | "inquiry";
  defaultType: string;
}> = [
  {
    slug: "ententes",
    label: "Ententes anticoncurrentielles",
    basePath: "/fr/decisions/ententes",
    kind: "decision",
    defaultType: "cartel",
  },
  {
    slug: "abus-de-position-dominante",
    label: "Abus de position dominante",
    basePath: "/fr/decisions/abus-de-position-dominante",
    kind: "decision",
    defaultType: "abuse_of_dominance",
  },
  {
    slug: "classements",
    label: "Classements",
    basePath: "/fr/decisions/classements",
    kind: "decision",
    defaultType: "dismissed",
  },
  {
    slug: "mesures-conservatoires",
    label: "Mesures conservatoires",
    basePath: "/fr/decisions/mesures-conservatoires",
    kind: "decision",
    defaultType: "interim_measures",
  },
  {
    slug: "amendes-astreintes",
    label: "Amendes et astreintes",
    basePath: "/fr/decisions/amendes-astreintes",
    kind: "decision",
    defaultType: "fine_penalty",
  },
  {
    slug: "engagements",
    label: "Engagements",
    basePath: "/fr/decisions/engagements",
    kind: "decision",
    defaultType: "commitments",
  },
  {
    slug: "avis",
    label: "Avis (opinions)",
    basePath: "/fr/avis-enquetes/avis",
    kind: "opinion",
    defaultType: "opinion",
  },
  {
    slug: "enquetes",
    label: "Enquetes sectorielles",
    basePath: "/fr/avis-enquetes/enquetes",
    kind: "inquiry",
    defaultType: "sector_inquiry",
  },
];

// ---------------------------------------------------------------------------
// CLI flags
// ---------------------------------------------------------------------------

const args = process.argv.slice(2);
const FLAG_RESUME = args.includes("--resume");
const FLAG_DRY_RUN = args.includes("--dry-run");
const FLAG_FORCE = args.includes("--force");
const FLAG_DECISIONS_ONLY = args.includes("--decisions-only");

function getFlagValue(flag: string): string | undefined {
  const idx = args.indexOf(flag);
  if (idx === -1 || idx + 1 >= args.length) return undefined;
  return args[idx + 1];
}

const MAX_PAGES = getFlagValue("--max-pages")
  ? parseInt(getFlagValue("--max-pages")!, 10)
  : Infinity;

// ---------------------------------------------------------------------------
// French month map
// ---------------------------------------------------------------------------

const FRENCH_MONTHS: Record<string, string> = {
  janvier: "01",
  "\u00e9vrier": "02",
  fevrier: "02",
  "f\u00e9vrier": "02",
  mars: "03",
  avril: "04",
  mai: "05",
  juin: "06",
  juillet: "07",
  "ao\u00fbt": "08",
  aout: "08",
  septembre: "09",
  octobre: "10",
  novembre: "11",
  "d\u00e9cembre": "12",
  decembre: "12",
};

/** Parse a French date like "19 decembre 2019" to "2019-12-19". */
function parseFrenchDate(raw: string): string | null {
  const cleaned = raw.trim().toLowerCase();
  const match = cleaned.match(/(\d{1,2})\s+(\S+)\s+(\d{4})/);
  if (!match) return null;
  const [, day, monthName, year] = match;
  const month = FRENCH_MONTHS[monthName!];
  if (!month || !day || !year) return null;
  return `${year}-${month}-${day.padStart(2, "0")}`;
}

/**
 * Parse date strings found on the site.
 * Handles: "DD/MM/YYYY", "DD.MM.YYYY", "DD monthName YYYY", "YYYY-MM-DD".
 */
function parseDate(raw: string): string | null {
  const trimmed = raw.trim();

  // ISO format
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) return trimmed;

  // DD/MM/YYYY or DD.MM.YYYY
  const slashMatch = trimmed.match(/^(\d{1,2})[./](\d{1,2})[./](\d{4})$/);
  if (slashMatch) {
    const [, d, m, y] = slashMatch;
    return `${y}-${m!.padStart(2, "0")}-${d!.padStart(2, "0")}`;
  }

  // French textual date
  return parseFrenchDate(trimmed);
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

let lastRequestTime = 0;

async function rateLimit(): Promise<void> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < RATE_LIMIT_MS) {
    await sleep(RATE_LIMIT_MS - elapsed);
  }
  lastRequestTime = Date.now();
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchWithRetry(
  url: string,
  retries = MAX_RETRIES,
): Promise<string> {
  await rateLimit();
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
      const res = await fetch(url, {
        signal: controller.signal,
        headers: {
          "User-Agent":
            "AnsvarCdlCIngester/1.0 (+https://ansvar.eu; competition-law-research)",
          Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "fr-LU,fr-FR;q=0.9,fr;q=0.8,en;q=0.5",
        },
      });
      clearTimeout(timeoutId);
      if (!res.ok) {
        throw new Error(`HTTP ${res.status} ${res.statusText} for ${url}`);
      }
      return await res.text();
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      if (attempt < retries) {
        const backoff = RETRY_BACKOFF_MS * attempt;
        log(
          `  WARN: attempt ${attempt}/${retries} failed for ${url}: ${msg} — retrying in ${backoff}ms`,
        );
        await sleep(backoff);
      } else {
        throw new Error(
          `Failed after ${retries} attempts for ${url}: ${msg}`,
        );
      }
    }
  }
  throw new Error("fetchWithRetry fell through");
}

/**
 * Fetch a PDF and extract raw text.
 * Uses a simple heuristic: extract text streams from PDF binary.
 * For the Luxembourg competition authority PDFs, the text is typically
 * embedded as UTF-8 text objects. We extract readable text spans.
 */
async function fetchPdfText(url: string): Promise<string | null> {
  await rateLimit();
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    const res = await fetch(url, {
      signal: controller.signal,
      headers: {
        "User-Agent":
          "AnsvarCdlCIngester/1.0 (+https://ansvar.eu; competition-law-research)",
        Accept: "application/pdf,*/*;q=0.8",
        "Accept-Language": "fr-LU,fr-FR;q=0.9,fr;q=0.8",
      },
    });
    clearTimeout(timeoutId);
    if (!res.ok) return null;

    const buffer = Buffer.from(await res.arrayBuffer());
    // Simple PDF text extraction: find text between BT/ET operators and
    // parenthesised strings. This is not a full parser but works for the
    // text-heavy legal PDFs on this site.
    const raw = buffer.toString("latin1");
    const textChunks: string[] = [];

    // Extract text from Tj/TJ operators
    const parenRegex = /\(([^)]{2,})\)/g;
    let m: RegExpExecArray | null;
    while ((m = parenRegex.exec(raw)) !== null) {
      const chunk = m[1]!;
      // Skip binary/control sequences
      if (/[\x00-\x08\x0e-\x1f]/.test(chunk)) continue;
      // Decode PDF escape sequences
      const decoded = chunk
        .replace(/\\n/g, "\n")
        .replace(/\\r/g, "\r")
        .replace(/\\t/g, "\t")
        .replace(/\\\\/g, "\\")
        .replace(/\\([()])/g, "$1");
      if (decoded.trim().length > 1) {
        textChunks.push(decoded);
      }
    }

    if (textChunks.length === 0) return null;

    const text = textChunks
      .join(" ")
      .replace(/\s{3,}/g, "\n\n")
      .trim();

    return text.length > 50 ? text : null;
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

const stats = {
  decisionsScraped: 0,
  decisionsInserted: 0,
  decisionsSkipped: 0,
  mergersScraped: 0,
  mergersInserted: 0,
  mergersSkipped: 0,
  pdfsFetched: 0,
  errors: 0,
  sectorsUpserted: 0,
};

function log(msg: string): void {
  const ts = new Date().toISOString().slice(0, 19).replace("T", " ");
  console.log(`[${ts}] ${msg}`);
}

// ---------------------------------------------------------------------------
// Database setup
// ---------------------------------------------------------------------------

function initDb(): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
    log(`Created directory ${dir}`);
  }

  if (FLAG_FORCE && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    log(`Deleted existing database at ${DB_PATH}`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);
  log(`Database ready at ${DB_PATH}`);
  return db;
}

// ---------------------------------------------------------------------------
// Sector normalisation
// ---------------------------------------------------------------------------

const SECTOR_MAP: Record<string, { id: string; name: string; name_en: string }> = {
  "services financiers": { id: "financial_services", name: "Services financiers", name_en: "Financial services" },
  banque: { id: "financial_services", name: "Services financiers", name_en: "Financial services" },
  assurance: { id: "financial_services", name: "Services financiers", name_en: "Financial services" },
  finance: { id: "financial_services", name: "Services financiers", name_en: "Financial services" },
  "t\u00e9l\u00e9communications": { id: "telecommunications", name: "T\u00e9l\u00e9communications", name_en: "Telecommunications" },
  telecoms: { id: "telecommunications", name: "T\u00e9l\u00e9communications", name_en: "Telecommunications" },
  "\u00e9nergie": { id: "energy", name: "\u00c9nergie", name_en: "Energy" },
  energie: { id: "energy", name: "\u00c9nergie", name_en: "Energy" },
  carburants: { id: "energy", name: "\u00c9nergie", name_en: "Energy" },
  "commerce de d\u00e9tail": { id: "retail", name: "Commerce de d\u00e9tail", name_en: "Retail" },
  distribution: { id: "retail", name: "Commerce de d\u00e9tail", name_en: "Retail" },
  "grande distribution": { id: "retail", name: "Commerce de d\u00e9tail", name_en: "Retail" },
  "m\u00e9dias": { id: "media", name: "M\u00e9dias", name_en: "Media" },
  medias: { id: "media", name: "M\u00e9dias", name_en: "Media" },
  audiovisuel: { id: "media", name: "M\u00e9dias", name_en: "Media" },
  construction: { id: "construction", name: "Construction", name_en: "Construction" },
  "b\u00e2timent": { id: "construction", name: "Construction", name_en: "Construction" },
  batiment: { id: "construction", name: "Construction", name_en: "Construction" },
  "sant\u00e9": { id: "healthcare", name: "Sant\u00e9", name_en: "Healthcare" },
  sante: { id: "healthcare", name: "Sant\u00e9", name_en: "Healthcare" },
  pharmacie: { id: "healthcare", name: "Sant\u00e9", name_en: "Healthcare" },
  "\u00e9conomie num\u00e9rique": { id: "digital_economy", name: "\u00c9conomie num\u00e9rique", name_en: "Digital economy" },
  "num\u00e9rique": { id: "digital_economy", name: "\u00c9conomie num\u00e9rique", name_en: "Digital economy" },
  numerique: { id: "digital_economy", name: "\u00c9conomie num\u00e9rique", name_en: "Digital economy" },
  transport: { id: "transport", name: "Transport", name_en: "Transport" },
  transports: { id: "transport", name: "Transport", name_en: "Transport" },
  taxi: { id: "transport", name: "Transport", name_en: "Transport" },
  immobilier: { id: "real_estate", name: "Immobilier", name_en: "Real estate" },
  agriculture: { id: "agriculture", name: "Agriculture", name_en: "Agriculture" },
  agroalimentaire: { id: "food_industry", name: "Agroalimentaire", name_en: "Food industry" },
  "caf\u00e9": { id: "food_industry", name: "Agroalimentaire", name_en: "Food industry" },
  cafe: { id: "food_industry", name: "Agroalimentaire", name_en: "Food industry" },
  "professions lib\u00e9rales": { id: "liberal_professions", name: "Professions lib\u00e9rales", name_en: "Liberal professions" },
  "professions r\u00e9glement\u00e9es": { id: "regulated_professions", name: "Professions r\u00e9glement\u00e9es", name_en: "Regulated professions" },
  notariat: { id: "regulated_professions", name: "Professions r\u00e9glement\u00e9es", name_en: "Regulated professions" },
};

function normaliseSector(rawSector: string): { id: string; name: string; name_en: string } {
  const key = rawSector.trim().toLowerCase();
  const mapped = SECTOR_MAP[key];
  if (mapped) return mapped;
  // Generate a slug from the raw text
  const id = key
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "");
  return { id, name: rawSector.trim(), name_en: rawSector.trim() };
}

// ---------------------------------------------------------------------------
// Case type classification
// ---------------------------------------------------------------------------

/**
 * Classify the decision type from the case number and category context.
 *
 * Luxembourg case number patterns:
 *   YYYY-FO-NN   — formal investigation (enquete formelle)
 *   YYYY-D-NN    — decision
 *   YYYY-MC-NN   — mesures conservatoires (interim measures)
 *   YYYY-AA-NN   — amendes/astreintes (fines & penalty payments)
 *   YYYY-C-NN    — classement (dismissed)
 *   YYYY-RP-NN   — rejet de plainte (complaint rejection)
 *   YYYY-AV-NN   — avis (opinion)
 *   YYYY-E-NN    — engagements (commitment decision)
 */
function classifyCaseType(caseNumber: string, categoryDefault: string): string {
  const upper = caseNumber.toUpperCase();
  if (/-MC-/.test(upper)) return "interim_measures";
  if (/-AA-/.test(upper)) return "fine_penalty";
  if (/-AV-/.test(upper)) return "opinion";
  if (/-E-/.test(upper)) return "commitments";
  if (/-C-/.test(upper)) return "dismissed";
  if (/-RP-/.test(upper)) return "dismissed";
  if (/-D-/.test(upper)) return "decision";
  if (/-FO-/.test(upper)) return "investigation";
  if (/-M[/-]/.test(upper)) return "merger";
  return categoryDefault;
}

// ---------------------------------------------------------------------------
// Outcome normalisation
// ---------------------------------------------------------------------------

function normaliseOutcome(raw: string): string {
  const lower = raw.toLowerCase();
  if (/amende|sanction p[ée]cuniaire/.test(lower)) return "fine";
  if (/astreinte/.test(lower)) return "penalty_payment";
  if (/engagement/.test(lower)) return "commitments";
  if (/injonction/.test(lower)) return "injunction";
  if (/non-lieu|hors de cause|rejet|classement|class[ée]e?\s+sans/.test(lower)) return "dismissed";
  if (/mesure.+conservatoire/.test(lower)) return "interim_measures";
  if (/sous conditions|sous r[ée]serve/.test(lower)) return "cleared_with_conditions";
  if (/autor?is[ée]e?\s+(?:en\s+)?phase\s*2|examen approfondi/.test(lower)) return "cleared_phase2";
  if (/autorisation|autoris[ée]e?/.test(lower)) return "cleared_phase1";
  if (/interdi(?:ction|te?)/.test(lower)) return "prohibited";
  if (/infraction\s+[ée]tablie|pratique.+[ée]tablie/.test(lower)) return "infringement_found";
  if (/irrecevable/.test(lower)) return "inadmissible";
  return raw.trim();
}

// ---------------------------------------------------------------------------
// Listing page parser
// ---------------------------------------------------------------------------

interface ListingItem {
  caseNumber: string;
  title: string;
  date: string | null;
  detailUrl: string;
  pdfUrl: string | null;
  category: string;
}

/**
 * Parse a listing page from concurrence.public.lu.
 *
 * The Luxembourg government CMS renders listing items as linked blocks.
 * Decision pages follow the pattern: /fr/decisions/{category}/{year}/{slug}.html
 * Opinion pages: /fr/avis-enquetes/avis/{year}/{slug}.html
 *
 * We look for <a> tags whose href matches these patterns and extract the
 * case number and title from the link text and surrounding context.
 */
function parseListingPage(
  html: string,
  category: typeof DECISION_CATEGORIES[number],
): ListingItem[] {
  const $ = cheerio.load(html);
  const items: ListingItem[] = [];
  const seen = new Set<string>();

  // Luxembourg CMS case number patterns for decision links
  // e.g. "2023-D-01", "2018-FO-03", "2012-MC-02", "2014-RP-01", "2018-C-10"
  const caseNumberRegex = /(\d{4}-[A-Z]+-\d+)/;

  // Find all links that point to decision or avis detail pages
  $("a[href]").each((_i, el) => {
    const $a = $(el);
    const href = $a.attr("href");
    if (!href) return;

    // Skip navigation, footer, breadcrumb links
    if ($a.closest("nav, footer, .menu, .breadcrumb, header").length > 0) return;

    // Match links that go to detail pages under the expected paths
    const isDecisionLink =
      href.includes("/fr/decisions/") && href.endsWith(".html") && /\/\d{4}\//.test(href);
    const isAvisLink =
      href.includes("/fr/avis-enquetes/") && href.endsWith(".html") && /\/\d{4}\//.test(href);

    if (!isDecisionLink && !isAvisLink) return;

    const rawText = $a.text().trim();
    if (!rawText || rawText.length < 5) return;

    // Try to extract case number from the link text
    let caseNumber: string | null = null;
    const caseMatch = rawText.match(caseNumberRegex);
    if (caseMatch) {
      caseNumber = caseMatch[1]!;
    } else {
      // Try to extract from the URL slug
      const urlSlug = href.split("/").pop()?.replace(".html", "") ?? "";
      const slugMatch = urlSlug.match(caseNumberRegex);
      if (slugMatch) {
        caseNumber = slugMatch[1]!;
      } else {
        // Use the slug as a case identifier (e.g. "decision-2024-e-01")
        caseNumber = urlSlug.toUpperCase();
      }
    }

    if (!caseNumber || seen.has(caseNumber)) return;
    seen.add(caseNumber);

    // Title: the link text, cleaned up
    let title = rawText;
    // Remove the case number prefix if present in the text
    title = title.replace(caseNumberRegex, "").replace(/^\s*[-\u2013\u2014:]\s*/, "").trim();
    if (!title) title = rawText;

    // Look for a date in surrounding context
    let date: string | null = null;
    const $container = $a.parent();
    const containerText = $container.text();
    // French date pattern
    const frenchDateMatch = containerText.match(
      /(\d{1,2})\s+(janvier|f[e\u00e9]vrier|mars|avril|mai|juin|juillet|ao[\u00fbu]t|septembre|octobre|novembre|d[e\u00e9]cembre)\s+(\d{4})/i,
    );
    if (frenchDateMatch) {
      date = parseFrenchDate(frenchDateMatch[0]);
    }
    // DD/MM/YYYY pattern
    if (!date) {
      const slashDate = containerText.match(/(\d{1,2})[./](\d{1,2})[./](\d{4})/);
      if (slashDate) {
        date = parseDate(slashDate[0]);
      }
    }

    // Build absolute URL
    const detailUrl = href.startsWith("http")
      ? href
      : `${BASE_URL}${href.startsWith("/") ? "" : "/"}${href}`;

    // Check for a PDF link in the same container
    let pdfUrl: string | null = null;
    $container.find('a[href$=".pdf"]').each((_j, pdfEl) => {
      const pdfHref = $(pdfEl).attr("href");
      if (pdfHref) {
        pdfUrl = pdfHref.startsWith("http")
          ? pdfHref
          : `${BASE_URL}${pdfHref.startsWith("/") ? "" : "/"}${pdfHref}`;
      }
    });

    items.push({
      caseNumber,
      title,
      date,
      detailUrl,
      pdfUrl,
      category: category.slug,
    });
  });

  return items;
}

// ---------------------------------------------------------------------------
// Detail page parser
// ---------------------------------------------------------------------------

interface DecisionDetail {
  caseNumber: string;
  title: string;
  date: string | null;
  type: string;
  sector: string | null;
  parties: string | null;
  summary: string | null;
  fullText: string;
  outcome: string | null;
  fineAmount: number | null;
  legalBasis: string | null;
  status: string;
}

/**
 * Extract a metadata field value from the page using multiple strategies.
 *
 * The Luxembourg government CMS uses varied HTML patterns:
 *   - <dt>Label</dt><dd>Value</dd> definition lists
 *   - <strong>Label :</strong> Value
 *   - <h2>Label</h2> followed by paragraphs
 *   - Plain text "Label : value" in the body
 */
function extractFieldValue(
  $: cheerio.CheerioAPI,
  pageText: string,
  labels: string[],
): string | null {
  for (const label of labels) {
    // Strategy 1: definition lists
    let found: string | null = null;

    $("dt").each((_i, el) => {
      if (found) return;
      const dtText = $(el).text().trim().toLowerCase();
      if (dtText === label.toLowerCase() || dtText === `${label.toLowerCase()} :`) {
        const $dd = $(el).next("dd");
        if ($dd.length > 0) {
          const val = $dd.text().trim();
          if (val && val.length < 2000) {
            found = val;
          }
        }
      }
    });
    if (found) return found;

    // Strategy 2: <strong>Label :</strong> followed by text
    $("strong, b").each((_i, el) => {
      if (found) return;
      const elText = $(el).text().trim().toLowerCase().replace(/:$/, "").trim();
      if (elText === label.toLowerCase()) {
        const parent = $(el).parent();
        if (parent.length > 0) {
          // Get text after the label element
          const fullParentText = parent.text();
          const labelEnd = fullParentText.toLowerCase().indexOf(elText) + elText.length;
          const afterLabel = fullParentText
            .slice(labelEnd)
            .replace(/^\s*:\s*/, "")
            .trim();
          if (afterLabel && afterLabel.length < 2000) {
            found = afterLabel;
          }
        }
      }
    });
    if (found) return found;

    // Strategy 3: regex on raw page text
    const escaped = label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const re = new RegExp(`${escaped}\\s*:?\\s*([^\\n]{3,300})`, "i");
    const match = pageText.match(re);
    if (match?.[1]) {
      return match[1].trim();
    }
  }

  return null;
}

function parseDecisionDetail(
  html: string,
  fallback: ListingItem,
  categoryConfig: typeof DECISION_CATEGORIES[number],
): DecisionDetail {
  const $ = cheerio.load(html);

  // --- Case number and date from heading ---
  const h1Text = ($("h1").first().text() ?? "").trim();
  let caseNumber = fallback.caseNumber;
  let date = fallback.date;

  const h1CaseMatch = h1Text.match(/(\d{4}-[A-Z]+-\d+)/);
  if (h1CaseMatch) caseNumber = h1CaseMatch[1]!;

  const h1DateMatch = h1Text.match(
    /(\d{1,2})\s+(janvier|f[e\u00e9]vrier|mars|avril|mai|juin|juillet|ao[\u00fbu]t|septembre|octobre|novembre|d[e\u00e9]cembre)\s+(\d{4})/i,
  );
  if (h1DateMatch) {
    date = parseFrenchDate(h1DateMatch[0]);
  }

  // Also try DD/MM/YYYY in heading
  if (!date) {
    const slashDate = h1Text.match(/(\d{1,2})[./](\d{1,2})[./](\d{4})/);
    if (slashDate) date = parseDate(slashDate[0]);
  }

  // --- Title ---
  let title = h1Text || fallback.title;
  // Strip case-number + date prefix from title
  title = title
    .replace(
      /^(?:D[e\u00e9]cision|Avis|Arr[e\u00ea]t[e\u00e9]?)\s+\d{4}-[A-Z]+-\d+\s*(?:du\s+\d{1,2}\s+\S+\s+\d{4}\s*)?[-\u2013\u2014]?\s*/i,
      "",
    )
    .trim();
  if (!title) title = fallback.title;

  // --- Page text for regex extraction ---
  const pageText = $.text();

  // --- Parties ---
  let parties: string[] = [];
  const partiesRaw = extractFieldValue($, pageText, [
    "Entreprise(s) concern\u00e9e(s)",
    "Entreprises concern\u00e9es",
    "Entreprise concern\u00e9e",
    "Parties",
    "Parties concern\u00e9es",
    "Mis en cause",
  ]);
  if (partiesRaw) {
    parties = partiesRaw
      .split(/[,;]/)
      .map((p) => p.trim())
      .filter((p) => p.length > 0);
  }

  // --- Fine amount ---
  let fineAmount: number | null = null;
  const fineText = extractFieldValue($, pageText, [
    "Amende",
    "Amendes",
    "Sanction p\u00e9cuniaire",
    "Sanctions",
    "Montant",
  ]);
  if (fineText) {
    fineAmount = parseFineAmount(fineText);
  }
  // Also scan full page text for fine amounts
  if (fineAmount === null) {
    fineAmount = parseFineAmount(pageText);
  }

  // --- Legal basis ---
  let legalBasisItems: string[] = [];
  const legalRaw = extractFieldValue($, pageText, [
    "Fondement juridique",
    "Base juridique",
    "Base l\u00e9gale",
    "Articles",
    "Disposition(s) applicable(s)",
  ]);
  if (legalRaw) {
    legalBasisItems = legalRaw
      .split(/[,;]/)
      .map((a) => a.trim())
      .filter((a) => a.length > 0);
  }
  // Also scan for common Luxembourg competition law references
  if (legalBasisItems.length === 0) {
    const lawMatches = pageText.match(
      /article\s+\d+\s+(?:de\s+)?(?:la\s+)?(?:loi\s+du\s+23\s+octobre\s+2011|loi\s+du\s+30\s+novembre\s+2022|TFUE|trait[e\u00e9]\s+FUE)/gi,
    );
    if (lawMatches) {
      legalBasisItems = [...new Set(lawMatches.map((m) => m.trim()))];
    }
  }

  // --- Outcome ---
  let outcome: string | null = null;
  const dispositif = extractFieldValue($, pageText, [
    "Dispositif",
    "D\u00e9cision",
    "R\u00e9sultat",
    "Issue",
  ]);
  if (dispositif) {
    outcome = normaliseOutcome(dispositif);
  }
  // Infer outcome from the category if not found
  if (!outcome) {
    if (categoryConfig.slug === "classements") outcome = "dismissed";
    if (categoryConfig.slug === "amendes-astreintes" && fineAmount) outcome = "fine";
    if (categoryConfig.slug === "engagements") outcome = "commitments";
  }

  // --- Status ---
  let status = "final";
  const recours = extractFieldValue($, pageText, [
    "Recours",
    "Appel",
    "Pourvoi",
    "Tribunal administratif",
  ]);
  if (recours && recours.trim().length > 5) {
    status = "appealed";
  }
  // Check for "en cours" / "ongoing" language
  if (/en\s+cours|instruction\s+en\s+cours|proc[e\u00e9]dure\s+ouverte/i.test(pageText)) {
    status = "ongoing";
  }

  // --- Sector ---
  let sector: string | null = null;
  const sectorText = extractFieldValue($, pageText, [
    "Secteur",
    "Secteur(s)",
    "Secteur d'activit\u00e9",
    "Secteur(s) d'activit\u00e9",
    "March\u00e9 concern\u00e9",
  ]);
  if (sectorText) {
    const norm = normaliseSector(sectorText.split(",")[0]!.trim());
    sector = norm.id;
  }
  // Try to infer sector from title keywords
  if (!sector) {
    sector = inferSectorFromText(title + " " + (h1Text || ""));
  }

  // --- Summary ---
  let summary: string | null = null;
  // Look for a "Resume" or "En bref" heading
  $("h2, h3").each((_i, heading) => {
    if (summary) return;
    const headingText = $(heading).text().trim().toLowerCase();
    if (
      headingText.includes("r\u00e9sum\u00e9") ||
      headingText.includes("resume") ||
      headingText.includes("en bref")
    ) {
      const parts: string[] = [];
      let $next = $(heading).next();
      while ($next.length > 0 && !$next.is("h1, h2, h3")) {
        const text = $next.text().trim();
        if (text) parts.push(text);
        $next = $next.next();
      }
      if (parts.length > 0) {
        summary = parts.join("\n\n");
      }
    }
  });

  // --- Full text ---
  let fullText = "";

  // Try content area selectors used by the Luxembourg government CMS
  const contentSelectors = [
    ".content-body",
    ".field--name-body",
    ".field--name-field-contenu",
    "article .content",
    ".article-body",
    "#content-core",
    "main article",
    "main .container",
    "main",
  ];

  for (const sel of contentSelectors) {
    const $content = $(sel);
    if ($content.length > 0) {
      $content
        .find("nav, .menu, .breadcrumb, script, style, .visually-hidden, header, footer")
        .remove();
      const bodyText = $content.text().trim();
      if (bodyText.length > fullText.length) {
        fullText = bodyText;
      }
      break;
    }
  }

  // Fallback: full body minus boilerplate
  if (!fullText || fullText.length < 100) {
    $(
      "nav, footer, header, script, style, .menu, .breadcrumb, .visually-hidden",
    ).remove();
    fullText = $("body").text().trim();
  }

  // Clean up whitespace
  fullText = fullText.replace(/\s{3,}/g, "\n\n").trim();

  // Use summary as fullText if the page content is thin (JS-rendered pages)
  if (fullText.length < 100 && summary) {
    fullText = summary;
  }

  // If fullText is still near-empty, set a placeholder
  if (fullText.length < 20) {
    fullText = `${caseNumber} \u2014 ${title}`;
  }

  // --- Type ---
  const type = classifyCaseType(caseNumber, categoryConfig.defaultType);

  return {
    caseNumber,
    title,
    date,
    type,
    sector,
    parties: parties.length > 0 ? JSON.stringify(parties) : null,
    summary,
    fullText,
    outcome,
    fineAmount,
    legalBasis: legalBasisItems.length > 0 ? JSON.stringify(legalBasisItems) : null,
    status,
  };
}

// ---------------------------------------------------------------------------
// Fine amount parser
// ---------------------------------------------------------------------------

function parseFineAmount(text: string): number | null {
  // Pattern: "N.NNN.NNN euros" or "N,N millions d'euros"
  const millionMatch = text.match(
    /([\d\s,.]+)\s*millions?\s+d[''\u2019]euros/i,
  );
  if (millionMatch) {
    const raw = millionMatch[1]!.replace(/\s/g, "").replace(",", ".");
    const parsed = parseFloat(raw);
    if (!isNaN(parsed)) return parsed * 1_000_000;
  }

  // "NNN.NNN,NN euros" or "NNN NNN euros"
  const euroMatch = text.match(
    /([\d\s.,]+)\s*(?:\u20ac|euros?)/i,
  );
  if (euroMatch) {
    // Handle European number format: "3.500.000,00" or "3 500 000"
    let raw = euroMatch[1]!.trim();
    // If it contains both dots and comma, dots are thousands separators
    if (raw.includes(",") && raw.includes(".")) {
      raw = raw.replace(/\./g, "").replace(",", ".");
    } else if (raw.includes(",")) {
      // Single comma might be decimal separator
      raw = raw.replace(",", ".");
    } else {
      // Dots might be thousands separators if more than one
      const dots = (raw.match(/\./g) || []).length;
      if (dots > 1) {
        raw = raw.replace(/\./g, "");
      }
    }
    raw = raw.replace(/\s/g, "");
    const parsed = parseFloat(raw);
    if (!isNaN(parsed) && parsed >= 100) return parsed;
  }

  return null;
}

// ---------------------------------------------------------------------------
// Sector inference from text
// ---------------------------------------------------------------------------

function inferSectorFromText(text: string): string | null {
  const lower = text.toLowerCase();
  const sectorKeywords: Array<[string, string]> = [
    ["bancaire|banque|cr\u00e9dit", "financial_services"],
    ["assurance", "financial_services"],
    ["telecom|t\u00e9l\u00e9communication|t\u00e9l\u00e9phonie|mobile|haut d\u00e9bit", "telecommunications"],
    ["carburant|\u00e9nergie|\u00e9lectricit\u00e9|gaz|p\u00e9trole", "energy"],
    ["grande.+surface|distribution|commerce.+d\u00e9tail", "retail"],
    ["construction|b\u00e2timent|voirie|routier", "construction"],
    ["h\u00f4pital|sant\u00e9|pharma|m\u00e9dical|m\u00e9dicament", "healthcare"],
    ["num\u00e9rique|plateforme|internet|streaming", "digital_economy"],
    ["m\u00e9dia|presse|audiovisuel|radio|t\u00e9l\u00e9vision", "media"],
    ["transport|taxi|livraison|logistique|postal", "transport"],
    ["immobilier|logement", "real_estate"],
    ["caf\u00e9|alimentaire|agroalimentaire|lait|bi\u00e8re", "food_industry"],
    ["notaire|avocat|profession.+lib\u00e9rale|profession.+r\u00e9glement\u00e9e", "regulated_professions"],
  ];

  for (const [pattern, sectorId] of sectorKeywords) {
    if (new RegExp(pattern!, "i").test(lower)) return sectorId!;
  }
  return null;
}

// ---------------------------------------------------------------------------
// Listing page crawl with pagination
// ---------------------------------------------------------------------------

/**
 * Crawl listing pages for a given category.
 *
 * The Luxembourg CMS uses `?b=N` for pagination offset (items per page),
 * similar to other public.lu government sites. We try both `?b=N` and
 * `?page=N` patterns. If no items are found, we also look for year-based
 * sub-pages (/2007/, /2012/, etc.).
 */
async function crawlCategoryListings(
  category: typeof DECISION_CATEGORIES[number],
): Promise<ListingItem[]> {
  const allItems: ListingItem[] = [];
  const seen = new Set<string>();

  // Strategy 1: paginated listing page
  let page = 0;
  let paginatedPages = 0;
  while (paginatedPages < MAX_PAGES) {
    // The Luxembourg CMS uses ?b= for offset (multiples of items-per-page)
    const offset = page * 10;
    const url = `${BASE_URL}${category.basePath}.html${page > 0 ? `?b=${offset}` : ""}`;
    log(`  Fetching ${category.label} listing (page ${page}): ${url}`);

    let html: string;
    try {
      html = await fetchWithRetry(url);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      log(`    ERROR fetching listing page: ${msg}`);
      stats.errors++;
      break;
    }

    const items = parseListingPage(html, category);
    const newItems = items.filter((it) => !seen.has(it.caseNumber));

    if (newItems.length === 0) {
      log(`    No new items on page ${page} — reached end of listing`);
      break;
    }

    for (const item of newItems) {
      seen.add(item.caseNumber);
      allItems.push(item);
    }

    log(`    Found ${newItems.length} items (total: ${allItems.length})`);
    paginatedPages++;
    page++;
  }

  // Strategy 2: year-based sub-pages
  // The site organises decisions by year: /{category}/{year}/
  // We probe known years (2004 to current year) if the listing page was empty
  // or if we want to be thorough
  if (allItems.length === 0 || allItems.length < 5) {
    const currentYear = new Date().getFullYear();
    const startYear = 2004; // Luxembourg competition law dates from this era

    let yearPages = 0;
    for (let year = currentYear; year >= startYear && yearPages < MAX_PAGES; year--) {
      const yearUrl = `${BASE_URL}${category.basePath}/${year}.html`;
      log(`  Probing year page: ${yearUrl}`);

      let html: string;
      try {
        html = await fetchWithRetry(yearUrl);
      } catch {
        // Year page does not exist — skip silently
        continue;
      }

      const items = parseListingPage(html, category);
      const newItems = items.filter((it) => !seen.has(it.caseNumber));
      for (const item of newItems) {
        seen.add(item.caseNumber);
        allItems.push(item);
      }

      if (newItems.length > 0) {
        log(`    Found ${newItems.length} items for year ${year}`);
      }
      yearPages++;
    }
  }

  // Strategy 3: discover links from the main listing page HTML
  // Some items may be listed without standard pagination — walk all <a> tags
  // pointing to /{category}/{year}/{slug}.html that we have not yet seen
  if (allItems.length === 0) {
    log(`  No items found via pagination or years — trying full-page link scan`);
    const url = `${BASE_URL}${category.basePath}.html`;
    try {
      const html = await fetchWithRetry(url);
      const $ = cheerio.load(html);
      $("a[href]").each((_i, el) => {
        const href = $(el).attr("href");
        if (!href) return;
        if (
          !href.includes(category.basePath.replace("/fr/", "/")) &&
          !href.includes("/decisions/") &&
          !href.includes("/avis-enquetes/")
        )
          return;
        if (!href.endsWith(".html")) return;
        if (!/\/\d{4}\//.test(href)) return;

        const rawText = $(el).text().trim();
        const caseMatch = rawText.match(/(\d{4}-[A-Z]+-\d+)/);
        const caseNumber = caseMatch
          ? caseMatch[1]!
          : (href.split("/").pop()?.replace(".html", "") ?? "").toUpperCase();

        if (!caseNumber || seen.has(caseNumber)) return;
        seen.add(caseNumber);

        const detailUrl = href.startsWith("http")
          ? href
          : `${BASE_URL}${href.startsWith("/") ? "" : "/"}${href}`;

        allItems.push({
          caseNumber,
          title: rawText || caseNumber,
          date: null,
          detailUrl,
          pdfUrl: null,
          category: category.slug,
        });
      });
    } catch {
      // Already logged above
    }
  }

  return allItems;
}

// ---------------------------------------------------------------------------
// PDF fallback: try to find and fetch the decision PDF
// ---------------------------------------------------------------------------

/**
 * For pages where the HTML content is thin (JS-rendered), try to fetch the
 * corresponding PDF from the /content/dam/ or /dam-assets/ paths.
 */
function guessPdfUrls(item: ListingItem): string[] {
  const urls: string[] = [];

  if (item.pdfUrl) {
    urls.push(item.pdfUrl);
  }

  // Pattern: /content/dam/concurrence/fr/decisions/{category}/{year}/{slug}.pdf
  const pathMatch = item.detailUrl.match(
    /\/fr\/(decisions|avis-enquetes)\/(.+)\.html$/,
  );
  if (pathMatch) {
    const [, section, rest] = pathMatch;
    urls.push(
      `${BASE_URL}/content/dam/concurrence/fr/${section}/${rest}.pdf`,
    );
    urls.push(
      `${BASE_URL}/dam-assets/fr/${section}/${rest}.pdf`,
    );
  }

  return urls;
}

// ---------------------------------------------------------------------------
// Merger detection & parsing
// ---------------------------------------------------------------------------

/**
 * Check whether a decision detail looks like a merger/concentration case.
 * We look at case number pattern, title keywords, and type classification.
 */
function isMergerCase(detail: DecisionDetail): boolean {
  // Case number with -M- or -M/ prefix
  if (/-M[/-]/i.test(detail.caseNumber)) return true;
  // Type classified as merger
  if (detail.type === "merger") return true;
  // Title keywords indicating a merger
  const lower = (detail.title + " " + (detail.summary ?? "")).toLowerCase();
  if (/concentration|fusion|acquisition|prise de contr[oô]le|rachat/.test(lower)) return true;
  return false;
}

/**
 * Extract merger-specific fields from a decision detail.
 * Acquiring party and target are parsed from the title or parties field.
 */
function extractMergerFields(detail: DecisionDetail): {
  acquiringParty: string | null;
  target: string | null;
  turnover: number | null;
} {
  let acquiringParty: string | null = null;
  let target: string | null = null;
  let turnover: number | null = null;

  // Try to split title on "/" or " - " or " contre "
  const titleSplitMatch = detail.title.match(
    /^(.+?)\s*(?:\/|–|\u2013|\u2014|\s+contre\s+)\s*(.+)$/,
  );
  if (titleSplitMatch) {
    acquiringParty = titleSplitMatch[1]!.trim();
    target = titleSplitMatch[2]!.trim();
  }

  // Try from parties JSON
  if (!acquiringParty && detail.parties) {
    try {
      const partiesList: string[] = JSON.parse(detail.parties);
      if (partiesList.length >= 2) {
        acquiringParty = partiesList[0]!;
        target = partiesList[1]!;
      } else if (partiesList.length === 1) {
        acquiringParty = partiesList[0]!;
      }
    } catch {
      // Not valid JSON, use as-is
      acquiringParty = detail.parties;
    }
  }

  // Try to find turnover amount in fullText
  if (detail.fullText) {
    const turnoverMatch = detail.fullText.match(
      /chiffre[s]?\s+d[''\u2019]affaires?\s+.*?([\d\s,.]+)\s*(?:millions?\s+d[''\u2019]euros|\u20ac|euros?)/i,
    );
    if (turnoverMatch) {
      const raw = turnoverMatch[1]!.replace(/\s/g, "").replace(",", ".");
      const parsed = parseFloat(raw);
      if (!isNaN(parsed)) {
        turnover = parsed > 1000 ? parsed : parsed * 1_000_000;
      }
    }
  }

  return { acquiringParty, target, turnover };
}

// ---------------------------------------------------------------------------
// Main ingestion
// ---------------------------------------------------------------------------

async function ingestDecisions(db: Database.Database): Promise<void> {
  log("=== Ingesting decisions, opinions, and inquiries ===");

  const existingDecisions = new Set<string>();
  const existingMergers = new Set<string>();
  if (FLAG_RESUME) {
    const decRows = db
      .prepare("SELECT case_number FROM decisions")
      .all() as Array<{ case_number: string }>;
    for (const r of decRows) existingDecisions.add(r.case_number);
    log(`Resume mode: ${existingDecisions.size} existing decisions in DB`);

    const merRows = db
      .prepare("SELECT case_number FROM mergers")
      .all() as Array<{ case_number: string }>;
    for (const r of merRows) existingMergers.add(r.case_number);
    log(`Resume mode: ${existingMergers.size} existing mergers in DB`);
  }

  const insertDecision = db.prepare(`
    INSERT OR IGNORE INTO decisions
      (case_number, title, date, type, sector, parties, summary, full_text,
       outcome, fine_amount, gwb_articles, status)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const insertMerger = db.prepare(`
    INSERT OR IGNORE INTO mergers
      (case_number, title, date, sector, acquiring_party, target, summary,
       full_text, outcome, turnover)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const upsertSectorDecision = db.prepare(`
    INSERT INTO sectors (id, name, name_en, description, decision_count, merger_count)
    VALUES (?, ?, ?, '', 1, 0)
    ON CONFLICT(id) DO UPDATE SET
      decision_count = decision_count + 1
  `);

  const upsertSectorMerger = db.prepare(`
    INSERT INTO sectors (id, name, name_en, description, decision_count, merger_count)
    VALUES (?, ?, ?, '', 0, 1)
    ON CONFLICT(id) DO UPDATE SET
      merger_count = merger_count + 1
  `);

  const categoriesToCrawl = FLAG_DECISIONS_ONLY
    ? DECISION_CATEGORIES.filter((c) => c.kind === "decision")
    : DECISION_CATEGORIES;

  for (const category of categoriesToCrawl) {
    log(`\n--- Category: ${category.label} ---`);
    const items = await crawlCategoryListings(category);
    log(`  Collected ${items.length} items from ${category.label}`);

    for (const item of items) {
      log(`  Scraping ${item.caseNumber}: ${item.detailUrl}`);

      let detail: DecisionDetail;
      try {
        const html = await fetchWithRetry(item.detailUrl);
        detail = parseDecisionDetail(html, item, category);

        // If the HTML page had thin content, try the PDF
        if (detail.fullText.length < 200) {
          const pdfUrls = guessPdfUrls(item);
          for (const pdfUrl of pdfUrls) {
            log(`    HTML content thin — trying PDF: ${pdfUrl}`);
            const pdfText = await fetchPdfText(pdfUrl);
            if (pdfText && pdfText.length > detail.fullText.length) {
              detail.fullText = pdfText;
              stats.pdfsFetched++;
              log(`    PDF text extracted (${pdfText.length} chars)`);
              break;
            }
          }
        }
      } catch (err: unknown) {
        const msg = err instanceof Error ? err.message : String(err);
        log(`    ERROR scraping ${item.caseNumber}: ${msg}`);
        stats.errors++;
        continue;
      }

      // Route to mergers or decisions table
      if (isMergerCase(detail)) {
        stats.mergersScraped++;
        if (FLAG_RESUME && existingMergers.has(detail.caseNumber)) {
          stats.mergersSkipped++;
          continue;
        }

        if (FLAG_DRY_RUN) {
          log(
            `    [DRY RUN] Would insert merger: ${detail.caseNumber} | ${detail.title.slice(0, 60)}... | outcome=${detail.outcome}`,
          );
          continue;
        }

        const merger = extractMergerFields(detail);
        try {
          insertMerger.run(
            detail.caseNumber,
            detail.title,
            detail.date,
            detail.sector,
            merger.acquiringParty,
            merger.target,
            detail.summary,
            detail.fullText,
            detail.outcome,
            merger.turnover,
          );
          stats.mergersInserted++;

          if (detail.sector) {
            const norm = normaliseSector(detail.sector);
            upsertSectorMerger.run(norm.id, norm.name, norm.name_en);
            stats.sectorsUpserted++;
          }
        } catch (err: unknown) {
          const msg = err instanceof Error ? err.message : String(err);
          log(`    ERROR inserting merger ${detail.caseNumber}: ${msg}`);
          stats.errors++;
        }
      } else {
        stats.decisionsScraped++;
        if (FLAG_RESUME && existingDecisions.has(detail.caseNumber)) {
          stats.decisionsSkipped++;
          continue;
        }

        if (FLAG_DRY_RUN) {
          log(
            `    [DRY RUN] Would insert: ${detail.caseNumber} | ${detail.title.slice(0, 60)}... | type=${detail.type} | outcome=${detail.outcome} | fine=${detail.fineAmount}`,
          );
          continue;
        }

        try {
          insertDecision.run(
            detail.caseNumber,
            detail.title,
            detail.date,
            detail.type,
            detail.sector,
            detail.parties,
            detail.summary,
            detail.fullText,
            detail.outcome,
            detail.fineAmount,
            detail.legalBasis, // stored in gwb_articles column (reused from schema)
            detail.status,
          );
          stats.decisionsInserted++;

          // Upsert sector
          if (detail.sector) {
            const norm = normaliseSector(detail.sector);
            upsertSectorDecision.run(norm.id, norm.name, norm.name_en);
            stats.sectorsUpserted++;
          }
        } catch (err: unknown) {
          const msg = err instanceof Error ? err.message : String(err);
          log(`    ERROR inserting ${detail.caseNumber}: ${msg}`);
          stats.errors++;
        }
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Rebuild sector counts
// ---------------------------------------------------------------------------

function rebuildSectorCounts(db: Database.Database): void {
  log("Rebuilding sector counts...");

  db.exec(`
    UPDATE sectors SET
      decision_count = (
        SELECT COUNT(*) FROM decisions WHERE decisions.sector = sectors.id
      ),
      merger_count = (
        SELECT COUNT(*) FROM mergers WHERE mergers.sector = sectors.id
      )
  `);

  const sectors = db
    .prepare("SELECT id, name, decision_count, merger_count FROM sectors ORDER BY decision_count DESC")
    .all() as Array<{ id: string; name: string; decision_count: number; merger_count: number }>;

  for (const s of sectors) {
    log(`  ${s.id}: ${s.decision_count} decisions, ${s.merger_count} mergers`);
  }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  log("Conseil de la concurrence du Luxembourg \u2014 ingestion crawler");
  log(`Config: DB_PATH=${DB_PATH} RATE_LIMIT=${RATE_LIMIT_MS}ms MAX_RETRIES=${MAX_RETRIES}`);
  log(
    `Flags: resume=${FLAG_RESUME} dry-run=${FLAG_DRY_RUN} force=${FLAG_FORCE} decisions-only=${FLAG_DECISIONS_ONLY} max-pages=${MAX_PAGES === Infinity ? "all" : MAX_PAGES}`,
  );

  // In dry-run mode, use an in-memory DB for resume checks and statement prep
  const db = FLAG_DRY_RUN ? (() => {
    const tmpDb = new Database(":memory:");
    tmpDb.pragma("journal_mode = WAL");
    tmpDb.exec(SCHEMA_SQL);
    return tmpDb;
  })() : initDb();

  try {
    await ingestDecisions(db);

    // Rebuild sector counts
    if (!FLAG_DRY_RUN) {
      rebuildSectorCounts(db);
    }
  } finally {
    db.close();
  }

  // --- Summary ---
  log("\n=== Ingestion complete ===");
  log(`  Decisions scraped:   ${stats.decisionsScraped}`);
  log(`  Decisions inserted:  ${stats.decisionsInserted}`);
  log(`  Decisions skipped:   ${stats.decisionsSkipped}`);
  log(`  Mergers scraped:     ${stats.mergersScraped}`);
  log(`  Mergers inserted:    ${stats.mergersInserted}`);
  log(`  Mergers skipped:     ${stats.mergersSkipped}`);
  log(`  PDFs fetched:        ${stats.pdfsFetched}`);
  log(`  Sectors upserted:    ${stats.sectorsUpserted}`);
  log(`  Errors:              ${stats.errors}`);

  if (stats.errors > 0) {
    process.exit(1);
  }
}

main().catch((err) => {
  log(`FATAL: ${err instanceof Error ? err.message : String(err)}`);
  if (err instanceof Error && err.stack) {
    console.error(err.stack);
  }
  process.exit(2);
});
