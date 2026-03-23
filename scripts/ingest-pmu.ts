/**
 * Ingestion crawler for the PMU (Protimonopolny urad SR / Slovak Antimonopoly
 * Office) MCP server.
 *
 * Scrapes competition enforcement decisions and merger control decisions
 * from antimon.gov.sk and populates the SQLite database.
 *
 * Data sources:
 *   - Paginated decision overview (antimon.gov.sk/prehlad-rozhodnuti/?page=N)
 *   - Individual decision detail pages (antimon.gov.sk/{slug}/)
 *   - PDF decision documents (antimon.gov.sk/data/att/{path}.pdf)
 *
 * Content is in Slovak. The PMU publishes decisions covering:
 *   - Kartely (cartel agreements)
 *   - Vertikalne dohody (vertical agreements)
 *   - Zneuzivanie dominantneho postavenia (abuse of dominance)
 *   - Koncentracie (merger control)
 *   - Obmedzovanie sutaze organmi statnej spravy (state body competition
 *     restriction)
 *   - Statna pomoc (state aid)
 *
 * The decisions collection is at /prehlad-rozhodnuti/ with ~23+ pages of ~15-20
 * entries each (roughly 400+ decisions total). Pagination uses ?page=N
 * (0-indexed, Drupal-based CMS). Each detail page has metadata fields:
 *   - Publikovane (publication date)
 *   - Zaciatok konania (proceeding start date)
 *   - Ucastnici konania (parties)
 *   - Typ konania (proceeding type)
 *   - Praktika (legal basis, e.g. "§ 7 ods. 1 pism. b)")
 *   - Sektor/relevantny trh (NACE sector code, e.g. "C.10.86", "G 47.30")
 * and a decision table with case number (e.g. 2026/KOH/SKO/3/5), verdict,
 * instance level, and a PDF download link (/data/att/{hex}/{id}.{hash}.pdf).
 *
 * Also crawls additional case listing pages at:
 *   - /66-sk/prehlad-pripadov/ (concentrations)
 *   - /73-sk/prehlad-pripadov/ (mixed: cartels, abuse, concentrations)
 * These pages share the same table structure as /prehlad-rozhodnuti/.
 *
 * Prerequisites:
 *   npm install cheerio   (if not already installed)
 *
 * Usage:
 *   npx tsx scripts/ingest-pmu.ts
 *   npx tsx scripts/ingest-pmu.ts --dry-run
 *   npx tsx scripts/ingest-pmu.ts --resume
 *   npx tsx scripts/ingest-pmu.ts --force
 *   npx tsx scripts/ingest-pmu.ts --max-pages 5
 */

import Database from "better-sqlite3";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import * as cheerio from "cheerio";
import { SCHEMA_SQL } from "../src/db.js";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DB_PATH = process.env["PMU_SK_DB_PATH"] ?? "data/pmu-sk.db";
const STATE_FILE = join(dirname(DB_PATH), "ingest-state.json");
const BASE_URL = "https://www.antimon.gov.sk";
const LISTING_PATH = "/prehlad-rozhodnuti/";
const SUPPLEMENTARY_LISTING_PATHS = [
  "/66-sk/prehlad-pripadov/",  // concentrations listing
  "/73-sk/prehlad-pripadov/",  // mixed enforcement listing
];
const MAX_LISTING_PAGES = 30; // ~23 pages observed on main listing, add headroom
const RATE_LIMIT_MS = 1500;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 5000;
const USER_AGENT =
  "AnsvarPMUCrawler/1.0 (+https://github.com/Ansvar-Systems/slovak-competition-mcp)";

// CLI flags
const dryRun = process.argv.includes("--dry-run");
const resume = process.argv.includes("--resume");
const force = process.argv.includes("--force");
const maxPagesArg = process.argv.find((_, i, a) => a[i - 1] === "--max-pages");
const maxPages = maxPagesArg ? parseInt(maxPagesArg, 10) : MAX_LISTING_PAGES;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface IngestState {
  processedUrls: string[];
  lastRun: string;
  decisionsIngested: number;
  mergersIngested: number;
  errors: string[];
}

interface ListingEntry {
  title: string;
  url: string;
  date: string | null;
  type: string | null;
}

interface ParsedDecision {
  case_number: string;
  title: string;
  date: string | null;
  type: string | null;
  sector: string | null;
  parties: string | null;
  summary: string | null;
  full_text: string;
  outcome: string | null;
  fine_amount: number | null;
  zohs_articles: string | null;
  status: string;
}

interface ParsedMerger {
  case_number: string;
  title: string;
  date: string | null;
  sector: string | null;
  acquiring_party: string | null;
  target: string | null;
  summary: string | null;
  full_text: string;
  outcome: string | null;
  turnover: number | null;
}

interface SectorAccumulator {
  [id: string]: {
    name: string;
    name_en: string | null;
    description: string | null;
    decisionCount: number;
    mergerCount: number;
  };
}

// ---------------------------------------------------------------------------
// HTTP fetching with rate limiting and retries
// ---------------------------------------------------------------------------

let lastRequestTime = 0;

async function rateLimitedFetch(url: string): Promise<string | null> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < RATE_LIMIT_MS) {
    await sleep(RATE_LIMIT_MS - elapsed);
  }

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      lastRequestTime = Date.now();
      const response = await fetch(url, {
        headers: {
          "User-Agent": USER_AGENT,
          Accept:
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "sk-SK,sk;q=0.9,cs;q=0.8,en;q=0.5",
        },
        redirect: "follow",
        signal: AbortSignal.timeout(30_000),
      });

      if (response.status === 403 || response.status === 429) {
        console.warn(
          `  [WARN] HTTP ${response.status} for ${url} (attempt ${attempt}/${MAX_RETRIES})`,
        );
        if (attempt < MAX_RETRIES) {
          await sleep(RETRY_DELAY_MS * attempt);
          continue;
        }
        return null;
      }

      if (!response.ok) {
        console.warn(`  [WARN] HTTP ${response.status} for ${url}`);
        if (attempt < MAX_RETRIES) {
          await sleep(RETRY_DELAY_MS * attempt);
          continue;
        }
        return null;
      }

      return await response.text();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.warn(
        `  [WARN] Fetch error for ${url} (attempt ${attempt}/${MAX_RETRIES}): ${message}`,
      );
      if (attempt < MAX_RETRIES) {
        await sleep(RETRY_DELAY_MS * attempt);
      }
    }
  }

  return null;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// State management (for --resume)
// ---------------------------------------------------------------------------

function loadState(): IngestState {
  if (resume && existsSync(STATE_FILE)) {
    try {
      const raw = readFileSync(STATE_FILE, "utf-8");
      return JSON.parse(raw) as IngestState;
    } catch {
      console.warn("[WARN] Could not read state file, starting fresh.");
    }
  }
  return {
    processedUrls: [],
    lastRun: new Date().toISOString(),
    decisionsIngested: 0,
    mergersIngested: 0,
    errors: [],
  };
}

function saveState(state: IngestState): void {
  state.lastRun = new Date().toISOString();
  const dir = dirname(STATE_FILE);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), "utf-8");
}

// ---------------------------------------------------------------------------
// Listing page parsing -- discover decision URLs
// ---------------------------------------------------------------------------

/**
 * Parse a single listing page from /prehlad-rozhodnuti/ and extract entries.
 *
 * The PMU listing at /prehlad-rozhodnuti/ renders ~15-20 decisions per page
 * in a table. Each row contains:
 *   - Participant name (linked to detail page)
 *   - Case type (Koncentracie, Kartely, Zneuzivanie dominantneho postavenia, etc.)
 *   - Decision date (dd.mm.yyyy)
 *   - Instance level (I. stupen / II. stupen)
 *
 * Pagination: ?page=0 is the first page, ?page=N for subsequent pages.
 */
function parseListingPage(html: string): ListingEntry[] {
  const $ = cheerio.load(html);
  const entries: ListingEntry[] = [];
  const seen = new Set<string>();

  // The listing is a table with rows linking to detail pages.
  // Each <a> links to a slug-based detail page like /kaumy-group-as-praha-ceska-republika/
  // We look for table rows containing links and metadata columns.
  $("table tr, .views-row, .item-list li").each((_i, row) => {
    const $row = $(row);
    const link = $row.find("a[href]").first();
    if (!link.length) return;

    const href = link.attr("href") ?? "";
    if (!href || href === "#" || href === "/") return;

    // Skip links to external sites, pagination, and site-wide navigation
    if (href.startsWith("http") && !href.includes("antimon.gov.sk")) return;
    if (href.includes("?page=")) return;
    if (href === LISTING_PATH) return;

    // Skip non-decision links (site structure pages)
    const skipPrefixes = [
      "/mapa-stranky",
      "/site-map",
      "/en/",
      "/kontakt",
      "/organizacia",
      "/predstavitelia",
      "/financ",
      "/kariera",
      "/spristupnovanie",
      "/kniznica",
      "/podatelna",
      "/hospodarska-sutaz",
      "/kartely/",
      "/vertikalne-dohody/",
      "/zneuzivanie-dominantneho-postavenia/",
      "/obmedzovanie-sutaze",
      "/koncentracie/",
      "/prehlad-rozhodnuti/",
      "/legislativ",
      "/o-statnej-pomoci",
      "/metodicke",
      "/rocne-spravy",
      "/schemy-statnej",
      "/rozhodnutia-europskej",
      "/centralny-register",
      "/aktuality",
      "/compact",
      "/rozcestnik",
      "/data/",
    ];
    if (skipPrefixes.some((p) => href.startsWith(p) || href.includes(p)))
      return;

    const fullUrl = href.startsWith("http")
      ? href
      : `${BASE_URL}${href.startsWith("/") ? "" : "/"}${href}`;

    if (seen.has(fullUrl)) return;
    seen.add(fullUrl);

    const linkText = link.text().trim();
    if (!linkText || linkText.length < 3) return;

    // Extract the row text to find type, date, and instance
    const rowText = $row.text();

    // Extract type from the row
    const type = extractTypeFromListing(rowText);

    // Extract date from the row (dd.mm.yyyy format)
    const date = extractDateFromContext(rowText);

    entries.push({
      title: linkText,
      url: fullUrl,
      date,
      type,
    });
  });

  // Fallback: if the table selector did not work, try all anchor tags in the
  // main content area that look like decision links.
  if (entries.length === 0) {
    $("a[href]").each((_i, el) => {
      const href = $(el).attr("href") ?? "";
      if (!href || href === "#" || href === "/") return;
      if (href.startsWith("http") && !href.includes("antimon.gov.sk")) return;
      if (href.includes("?page=")) return;

      // Decision detail pages are slug-based: /company-name-slug/
      // Some also use numeric prefix: /8895-sk/slovenska-posta-as-banska-bystrica/
      const slugMatch = href.match(
        /^\/(?:\d+-sk\/)?([a-z0-9][\w-]*[a-z0-9])\/?$/i,
      );
      if (!slugMatch) return;

      const slug = slugMatch[1]!;
      // Skip known structural pages
      const structuralSlugs = [
        "prehlad-rozhodnuti",
        "hospodarska-sutaz",
        "kartely",
        "vertikalne-dohody",
        "koncentracie",
        "mapa-stranky",
        "site-map",
        "aktuality",
        "compact",
        "kontakty-a-informacie",
        "rozcestniky",
        "rozcestniky-en",
        "organization",
      ];
      if (structuralSlugs.includes(slug.toLowerCase())) return;

      const fullUrl = `${BASE_URL}${href.startsWith("/") ? "" : "/"}${href}`;
      if (seen.has(fullUrl)) return;
      seen.add(fullUrl);

      const linkText = $(el).text().trim();
      if (!linkText || linkText.length < 3) return;

      const parentText = $(el).closest("tr, div, li, p").text();
      const type = extractTypeFromListing(parentText);
      const date = extractDateFromContext(parentText);

      entries.push({
        title: linkText,
        url: fullUrl,
        date,
        type,
      });
    });
  }

  return entries;
}

/**
 * Extract the proceeding type from listing row text.
 *
 * The PMU listing labels proceedings as:
 *   - Koncentracie
 *   - Kartely
 *   - Vertikalne dohody
 *   - Zneuzivanie dominantneho postavenia
 *   - Obmedzovanie sutaze organmi statnej spravy a samospravy
 *   - Statna pomoc
 *   - Ine (other)
 */
function extractTypeFromListing(text: string): string | null {
  const lower = text.toLowerCase();

  if (lower.includes("koncentr")) return "concentration";
  if (lower.includes("kartel")) return "cartel";
  if (
    lower.includes("zneužívanie") ||
    lower.includes("zneuzivanie") ||
    lower.includes("dominantn")
  )
    return "abuse_of_dominance";
  if (
    lower.includes("vertikáln") ||
    lower.includes("vertikaln")
  )
    return "vertical_agreement";
  if (
    lower.includes("obmedzovanie") ||
    lower.includes("obmedzovan")
  )
    return "state_restriction";
  if (
    lower.includes("štátna pomoc") ||
    lower.includes("statna pomoc")
  )
    return "state_aid";
  if (lower.includes("iné") || lower.includes("ine"))
    return "other";

  return null;
}

// ---------------------------------------------------------------------------
// Date parsing -- handles Slovak date formats
// ---------------------------------------------------------------------------

const SK_MONTH_MAP: Record<string, string> = {
  // Genitive forms (used in "1. januara 2024")
  "januára": "01",
  "januara": "01",
  "februára": "02",
  "februara": "02",
  "marca": "03",
  "apríla": "04",
  "aprila": "04",
  "mája": "05",
  "maja": "05",
  "júna": "06",
  "juna": "06",
  "júla": "07",
  "jula": "07",
  "augusta": "08",
  "septembra": "09",
  "októbra": "10",
  "oktobra": "10",
  "novembra": "11",
  "decembra": "12",
  // Nominative forms (used in headings)
  "január": "01",
  "januar": "01",
  "február": "02",
  "februar": "02",
  "marec": "03",
  "apríl": "04",
  "april": "04",
  "máj": "05",
  "maj": "05",
  "jún": "06",
  "jun": "06",
  "júl": "07",
  "jul": "07",
  "august": "08",
  "september": "09",
  "október": "10",
  "oktober": "10",
  "november": "11",
  "december": "12",
};

/**
 * Parse a Slovak date string to ISO format (YYYY-MM-DD).
 *
 * Handles:
 *   - "06.02.2026"       (dd.mm.yyyy -- most common on PMU)
 *   - "06. 02. 2026"     (dd. mm. yyyy)
 *   - "6. februara 2026" (dd. month-name yyyy)
 *   - "2024-03-15"       (ISO)
 */
function parseDate(raw: string): string | null {
  if (!raw) return null;

  const trimmed = raw.trim();

  // ISO format already
  const isoMatch = trimmed.match(/(\d{4})-(\d{2})-(\d{2})/);
  if (isoMatch) return isoMatch[0];

  // Slovak numeric: "06.02.2026" or "06. 02. 2026" or "6.2.2026"
  const skNumeric = trimmed.match(/(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})/);
  if (skNumeric) {
    const [, day, month, year] = skNumeric;
    return `${year}-${month!.padStart(2, "0")}-${day!.padStart(2, "0")}`;
  }

  // Slovak text: "6. februara 2026", "15. marca 2023"
  const skText = trimmed.match(
    /(\d{1,2})\.\s*([a-zA-Z\u00c0-\u017e]+)\s+(\d{4})/,
  );
  if (skText) {
    const [, day, monthName, year] = skText;
    const monthNum = SK_MONTH_MAP[monthName!.toLowerCase()];
    if (monthNum) {
      return `${year}-${monthNum}-${day!.padStart(2, "0")}`;
    }
  }

  return null;
}

/** Try to find a date in surrounding context text. */
function extractDateFromContext(text: string): string | null {
  // Slovak numeric date: "06.02.2026"
  const datePattern = /(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})/;
  const match = text.match(datePattern);
  if (match) return parseDate(match[0]);

  // Slovak month name format
  const monthNames = Object.keys(SK_MONTH_MAP).join("|");
  const textDateRe = new RegExp(
    `(\\d{1,2})\\.\\s*(${monthNames})\\s+(\\d{4})`,
    "i",
  );
  const textMatch = text.match(textDateRe);
  if (textMatch) return parseDate(textMatch[0]);

  return null;
}

// ---------------------------------------------------------------------------
// Detail page parsing -- extract structured data
// ---------------------------------------------------------------------------

/**
 * Extract metadata fields from a PMU decision detail page.
 *
 * PMU detail pages present metadata with field labels:
 *   - Publikovane: (publication date)
 *   - Zaciatok konania: (proceeding start date)
 *   - Ucastnici konania: (parties)
 *   - Typ konania: (proceeding type)
 *   - Praktika: (legal basis / violated provision)
 *   - Sektor/relevantny trh: (sector/relevant market)
 *
 * Below the metadata is a table with decision details:
 *   - Vydanie rozhodnutia (decision date)
 *   - Rozhodnutie (decision number, e.g. 2026/KOH/SKO/3/5)
 *   - Instancia (instance: I. stupen / II. stupen)
 *   - Verdikt (verdict: suhlas, pokuta, zastavenie, etc.)
 *   - Pravoplatnost (legal effect date)
 *   - Subor (PDF file link)
 */
function extractMetadata(
  $: cheerio.CheerioAPI,
): Record<string, string> {
  const meta: Record<string, string> = {};
  const pageText = $("body").text();

  // Extract key metadata fields using regex on the full page text
  const fieldPatterns: Array<{ key: string; pattern: RegExp }> = [
    {
      key: "publikovane",
      pattern:
        /Publikovan[ée]\s*:\s*(.+?)(?=\n|Za[čc]iatok|$)/i,
    },
    {
      key: "zaciatok_konania",
      pattern:
        /Za[čc]iatok\s+konania\s*:\s*(.+?)(?=\n|[ÚU][čc]astn|$)/i,
    },
    {
      key: "ucastnici",
      pattern:
        /[ÚU][čc]astn[ií][ck][ií]\s+konania\s*:\s*([\s\S]+?)(?=Typ\s+konania|$)/i,
    },
    {
      key: "typ_konania",
      pattern:
        /Typ\s+konania\s*:\s*(.+?)(?=\n|Praktika|$)/i,
    },
    {
      key: "praktika",
      pattern:
        /Praktika\s*:\s*(.+?)(?=\n|Sektor|$)/i,
    },
    {
      key: "sektor",
      pattern:
        /Sektor\s*\/?\s*relevantn[ýy]\s+trh\s*:\s*(.+?)(?=\n|Vydanie|$)/i,
    },
  ];

  for (const { key, pattern } of fieldPatterns) {
    const match = pageText.match(pattern);
    if (match?.[1]) {
      meta[key] = match[1].trim();
    }
  }

  // Extract from the decision details table
  // Columns: Vydanie rozhodnutia | Rozhodnutie | Instancia | Verdikt | Pravoplatnost | Subor
  $("table").each((_ti, table) => {
    const $table = $(table);
    const headerText = $table.find("th, thead").text().toLowerCase();

    // Identify the decision details table by its column headers
    if (
      headerText.includes("rozhodnutie") ||
      headerText.includes("verdikt") ||
      headerText.includes("právoplatnosť") ||
      headerText.includes("pravoplatnost")
    ) {
      // Get the first data row (most recent decision)
      const $firstRow = $table.find("tbody tr, tr").filter((_ri, row) => {
        return $(row).find("td").length > 0;
      }).first();

      if ($firstRow.length) {
        const cells = $firstRow.find("td");
        if (cells.length >= 4) {
          const vydanie = $(cells[0]).text().trim();
          const rozhodnutie = $(cells[1]).text().trim();
          const instancia = $(cells[2]).text().trim();
          const verdikt = $(cells[3]).text().trim();
          const pravoplatnost =
            cells.length >= 5 ? $(cells[4]).text().trim() : "";

          if (vydanie) meta["vydanie_rozhodnutia"] = vydanie;
          if (rozhodnutie) meta["rozhodnutie"] = rozhodnutie;
          if (instancia) meta["instancia"] = instancia;
          if (verdikt) meta["verdikt"] = verdikt;
          if (pravoplatnost) meta["pravoplatnost"] = pravoplatnost;
        }
      }
    }
  });

  return meta;
}

/**
 * Extract the main body text from the decision detail page.
 *
 * PMU pages have decision metadata followed by the decision details table
 * and sometimes additional narrative text. The full decision is typically
 * available only in the PDF attachment, so we combine all available text.
 */
function extractBodyText($: cheerio.CheerioAPI): string {
  // Remove navigation, header, footer, scripts
  const clone = $.root().clone();
  const $c = cheerio.load(clone.html() ?? "");
  $c(
    "nav, header, footer, script, style, .menu, .breadcrumb, .pager, .social-sharing",
  ).remove();

  // Try known content selectors
  const selectors = [
    ".node__content",
    ".content .field--name-body",
    ".content article",
    "#content",
    "main .content",
    ".content-area",
    "main article",
    "main",
  ];

  for (const sel of selectors) {
    const el = $c(sel);
    if (el.length > 0) {
      const text = el.text().trim();
      if (text.length > 100) return cleanText(text);
    }
  }

  // Fallback: collect all paragraph text
  const paragraphs: string[] = [];
  $c("p").each((_i, el) => {
    const text = $c(el).text().trim();
    if (text.length > 30) paragraphs.push(text);
  });
  if (paragraphs.length > 0) return cleanText(paragraphs.join("\n\n"));

  // Last resort
  return cleanText($c("body").text().trim());
}

/** Collapse excessive whitespace while preserving paragraph breaks. */
function cleanText(text: string): string {
  return text
    .replace(/[ \t]+/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

/**
 * Extract PDF download link from the detail page.
 *
 * PMU PDFs follow the pattern: /data/att/{hex}/{id}.{hash}.pdf
 * Example: /data/att/116/4564.6b4049.pdf
 */
function extractPdfLink($: cheerio.CheerioAPI): string | null {
  let pdfUrl: string | null = null;

  $('a[href*=".pdf"]').each((_i, el) => {
    const href = $(el).attr("href") ?? "";
    if (href.includes("/data/att/")) {
      pdfUrl = href.startsWith("http") ? href : `${BASE_URL}${href}`;
      return false; // break
    }
  });

  // Broader PDF search if the specific pattern did not match
  if (!pdfUrl) {
    $('a[href$=".pdf"]').each((_i, el) => {
      const href = $(el).attr("href") ?? "";
      if (href) {
        pdfUrl = href.startsWith("http") ? href : `${BASE_URL}${href}`;
        return false;
      }
    });
  }

  return pdfUrl;
}

/**
 * Extract parties from the Ucastnici konania field.
 * Can be a single entity or multiple entities separated by commas or newlines.
 */
function extractParties(raw: string | undefined): string | null {
  if (!raw) return null;

  const parties = raw
    .split(/[,\n]/)
    .map((line) => line.replace(/^\d+\.\s*/, "").trim())
    .filter((line) => line.length > 1)
    .join("; ");

  return parties.length > 0 ? parties : null;
}

// ---------------------------------------------------------------------------
// Classification -- case type, outcome, sector
// ---------------------------------------------------------------------------

/**
 * Determine whether a case is a merger or enforcement decision.
 * Also classify the decision type and outcome.
 *
 * PMU proceeding types (Typ konania):
 *   - "Koncentracie" = merger control
 *   - "Kartely" = cartel
 *   - "Vertikalne dohody" = vertical agreement
 *   - "Zneuzivanie dominantneho postavenia" = abuse of dominance
 *   - "Obmedzovanie sutaze organmi statnej spravy" = state restriction
 *   - "Statna pomoc" = state aid
 *
 * PMU case number format:
 *   - YYYY/KOH/SKO/N/N (concentrations -- KOH = koncentracie)
 *   - YYYY/DOH/POK/N/N (agreements -- DOH = dohody)
 *   - YYYY/DOZ/POK/N/N (abuse of dominance -- DOZ = dominantne postavenie, zneuzivanie)
 *   - YYYY/DZ/R/N/N    (abuse of dominance, older format)
 *   - YYYY/DOH/R/N/N   (agreements -- second instance)
 *   - YYYY/DOH/ZPR/R/N (agreements -- second instance reversal)
 */
function classifyCase(
  meta: Record<string, string>,
  title: string,
  bodyText: string,
  listingType: string | null,
): {
  isMerger: boolean;
  isDecision: boolean;
  type: string | null;
  outcome: string | null;
} {
  const typKonania = (meta["typ_konania"] ?? "").toLowerCase();
  const verdikt = (meta["verdikt"] ?? "").toLowerCase();
  const rozhodnutie = (meta["rozhodnutie"] ?? "").toUpperCase();
  const praktika = (meta["praktika"] ?? "").toLowerCase();
  const allText =
    `${typKonania} ${verdikt} ${title.toLowerCase()} ${bodyText.toLowerCase().slice(0, 5000)}`;

  // --- Merger detection ---
  const isMerger =
    listingType === "concentration" ||
    typKonania.includes("koncentr") ||
    rozhodnutie.includes("/KOH/") ||
    rozhodnutie.includes("/SKO/") ||
    allText.includes("koncentráci") ||
    allText.includes("koncentraci") ||
    allText.includes("prevzat") ||
    allText.includes("prevzatie") ||
    (allText.includes("zlúčeni") || allText.includes("zluceni")) ||
    allText.includes("fúzi") ||
    allText.includes("fuzi");

  // --- Decision type ---
  let type: string | null = null;

  if (isMerger) {
    type = "merger_control";
  } else if (
    listingType === "cartel" ||
    typKonania.includes("kartel") ||
    rozhodnutie.includes("/DOH/") ||
    allText.includes("kartel") ||
    allText.includes("dohoda obmedzujúca") ||
    allText.includes("dohoda obmedzujuca") ||
    allText.includes("horizontálna dohoda") ||
    allText.includes("horizontalna dohoda") ||
    allText.includes("bid rigging") ||
    allText.includes("krycia ponuka") ||
    allText.includes("krycí ponuk")
  ) {
    type = "cartel";
  } else if (
    listingType === "abuse_of_dominance" ||
    typKonania.includes("zneužívanie") ||
    typKonania.includes("zneuzivanie") ||
    typKonania.includes("dominantn") ||
    rozhodnutie.includes("/DZ/") ||
    rozhodnutie.includes("/DOZ/") ||
    allText.includes("zneužívanie dominantného") ||
    allText.includes("zneuzivanie dominantneho") ||
    praktika.includes("§ 8")
  ) {
    type = "abuse_of_dominance";
  } else if (
    listingType === "vertical_agreement" ||
    typKonania.includes("vertikáln") ||
    typKonania.includes("vertikaln") ||
    allText.includes("vertikálna dohoda") ||
    allText.includes("vertikalna dohoda")
  ) {
    type = "vertical_agreement";
  } else if (
    listingType === "state_restriction" ||
    typKonania.includes("obmedzovanie") ||
    allText.includes("obmedzovanie súťaže orgánmi") ||
    allText.includes("obmedzovanie sutaze organmi")
  ) {
    type = "state_restriction";
  } else if (
    listingType === "state_aid" ||
    typKonania.includes("štátna pomoc") ||
    typKonania.includes("statna pomoc")
  ) {
    type = "state_aid";
  } else if (
    allText.includes("sektorové šetrenie") ||
    allText.includes("sektorove setrenie") ||
    allText.includes("prieskum trhu")
  ) {
    type = "sector_inquiry";
  }

  // --- Outcome ---
  let outcome: string | null = null;

  if (
    verdikt.includes("pokut") ||
    allText.includes("uložil pokutu") ||
    allText.includes("ulozil pokutu") ||
    allText.includes("sankci")
  ) {
    outcome = "fine";
  } else if (
    verdikt.includes("súhlas") ||
    verdikt.includes("suhlas") ||
    verdikt.includes("schválen") ||
    verdikt.includes("schvalen")
  ) {
    if (isMerger) {
      // Phase 2 detection
      outcome =
        allText.includes("ii. fáz") ||
        allText.includes("ii. faz") ||
        allText.includes("druhá fáza") ||
        allText.includes("druha faza")
          ? "cleared_phase2"
          : "cleared_phase1";
    } else {
      outcome = "cleared";
    }
  } else if (
    verdikt.includes("záväzk") ||
    verdikt.includes("zavazk") ||
    allText.includes("prijatie záväzkov") ||
    allText.includes("prijatie zavazkov") ||
    allText.includes("záväzkové rozhodnutie") ||
    allText.includes("zavazkove rozhodnutie")
  ) {
    outcome = isMerger ? "cleared_with_conditions" : "cleared_with_conditions";
  } else if (
    verdikt.includes("zastaveni") ||
    verdikt.includes("zastavení") ||
    allText.includes("konanie zastavené") ||
    allText.includes("konanie zastavene")
  ) {
    outcome = "closed";
  } else if (
    verdikt.includes("zamietnut") ||
    verdikt.includes("zakázan") ||
    verdikt.includes("zakazan") ||
    allText.includes("koncentrácia zakázaná") ||
    allText.includes("koncentracia zakazana")
  ) {
    outcome = isMerger ? "blocked" : "prohibited";
  } else if (
    verdikt.includes("prebieha") ||
    verdikt.includes("začat") ||
    verdikt.includes("zacat") ||
    allText.includes("konanie prebieha") ||
    allText.includes("začalo správne konanie") ||
    allText.includes("zacalo spravne konanie")
  ) {
    outcome = "ongoing";
  } else if (isMerger) {
    outcome = "cleared_phase1";
  }

  const isDecision = !isMerger && type !== null;

  return { isMerger, isDecision, type, outcome };
}

/**
 * Map metadata and text content to a sector identifier.
 * Uses Slovak terms since all PMU content is in Slovak.
 *
 * The PMU sektor field uses NACE classification codes like:
 *   "C.10.86 Vyroba a priprava homogenizovanych potravin"
 *   "G 47.30 (retail fuel); G 46.71 (wholesale fuels)"
 */
function classifySector(
  meta: Record<string, string>,
  title: string,
  bodyText: string,
): string | null {
  const sektorField = meta["sektor"] ?? "";
  const text =
    `${title} ${sektorField} ${bodyText.slice(0, 3000)}`.toLowerCase();

  const mapping: Array<{ id: string; patterns: string[] }> = [
    {
      id: "energy",
      patterns: [
        "energetik",
        "energi",
        "elektrická",
        "elektrick",
        "elektrin",
        "plyn",
        "plynár",
        "plynar",
        "teplár",
        "teplar",
        "obnoviteľn",
        "obnoviteln",
        "distribúci",
        "distribuci",
        "spp ",
        "zse ",
        "sse-",
        "vse ",
        "úrso",
        "urso",
      ],
    },
    {
      id: "telecommunications",
      patterns: [
        "telekomunikáci",
        "telekomunikaci",
        "telecom",
        "mobiln",
        "širokopásm",
        "sirokopasm",
        "internet",
        "televízn",
        "televizn",
        "slovak telekom",
        "orange slovensko",
        "o2 slovakia",
        "swan",
        "roaming",
      ],
    },
    {
      id: "retail",
      patterns: [
        "maloobchod",
        "supermarket",
        "obchodn",
        "potravin",
        "hypermarket",
        "kaufland",
        "lidl",
        "tesco",
        "billa",
        "coop jednota",
        "penny",
        "potraviny",
      ],
    },
    {
      id: "pharmaceuticals",
      patterns: [
        "farmaceutick",
        "farmaceut",
        "liečiv",
        "lieciv",
        "lekáreň",
        "lekaren",
        "zdravotn",
        "nemocni",
        "distribúcia liekov",
        "distribucia liekov",
      ],
    },
    {
      id: "banking",
      patterns: [
        "bank",
        "finančn",
        "financn",
        "poisťov",
        "poistov",
        "hypotečn",
        "hypotecn",
        "úverov",
        "uverov",
        "platobné služb",
        "platobne sluzb",
        "slovenská sporiteľňa",
        "slovenska sporitelna",
        "vúb ",
        "vub ",
        "čsob",
        "csob",
        "psd2",
        "bancassurance",
        "fintech",
      ],
    },
    {
      id: "construction",
      patterns: [
        "stavebníctv",
        "stavebnictv",
        "stavebn",
        "stavba",
        "stavby",
        "betón",
        "beton",
        "cement",
        "asfalt",
        "diaľnic",
        "dialnic",
        "infraštruktúr",
        "infrastruktur",
        "strabag",
        "eurovia",
        "doprastav",
        "verejné obstarávanie",
        "verejne obstaravanie",
      ],
    },
    {
      id: "transport",
      patterns: [
        "doprav",
        "preprav",
        "leteck",
        "železnič",
        "zeleznic",
        "autobus",
        "logistik",
        "spedič",
        "spedic",
        "nákladn",
        "nakladn",
        "zssk",
        "regiojet",
        "flixbus",
      ],
    },
    {
      id: "automotive",
      patterns: [
        "automob",
        "vozidl",
        "autoserv",
        "autorizovan",
        "predaj vozidiel",
        "volkswagen",
        "kia ",
        "peugeot",
        "jaguar",
      ],
    },
    {
      id: "agriculture",
      patterns: [
        "poľnohospodárst",
        "polnohospodarst",
        "agrárn",
        "agrarn",
        "mlieko",
        "mliekar",
        "obilnin",
        "potravinársk",
        "potravinarsk",
        "cukrovar",
        "pekár",
        "pekar",
      ],
    },
    {
      id: "media",
      patterns: [
        "médi",
        "medi",
        "vysielani",
        "vysielan",
        "televíz",
        "televiz",
        "tlač",
        "tlac",
        "vydavateľ",
        "vydavatel",
        "reklam",
        "inzerci",
      ],
    },
    {
      id: "digital",
      patterns: [
        "online",
        "e-commerce",
        "digitáln",
        "digitaln",
        "platforma",
        "trhovisko",
        "softvér",
        "softver",
        "it služb",
        "it sluzb",
      ],
    },
    {
      id: "chemicals",
      patterns: [
        "chemick",
        "chémi",
        "chemi",
        "plastov",
        "plasty",
        "hnojiv",
        "petrochemi",
      ],
    },
    {
      id: "waste_management",
      patterns: [
        "odpad",
        "recykláci",
        "recyklaci",
        "skládkov",
        "skladkov",
        "zber",
      ],
    },
    {
      id: "real_estate",
      patterns: [
        "nehnuteľnost",
        "nehnutelnost",
        "reality",
        "realitn",
        "bytov",
        "developersk",
      ],
    },
    {
      id: "insurance",
      patterns: [
        "poisťovníctv",
        "poistovnictv",
        "poisťovň",
        "poistovn",
        "poisteni",
        "zaisteni",
        "generali",
        "allianz",
      ],
    },
  ];

  for (const { id, patterns } of mapping) {
    for (const p of patterns) {
      if (text.includes(p)) return id;
    }
  }

  // Also try NACE code prefixes from the sektor field
  const naceLower = sektorField.toLowerCase();
  if (naceLower.match(/^[cd]\./)) {
    // Manufacturing / industry
    if (naceLower.includes("c.10") || naceLower.includes("c.11"))
      return "agriculture";
    if (naceLower.includes("c.20") || naceLower.includes("c.21"))
      return "chemicals";
    if (naceLower.includes("c.29") || naceLower.includes("c.30"))
      return "automotive";
  }
  if (naceLower.match(/^g\s/)) return "retail";
  if (naceLower.match(/^j\s/)) return "telecommunications";
  if (naceLower.match(/^k\s/)) return "banking";
  if (naceLower.match(/^d\s/) || naceLower.match(/^d\./)) return "energy";
  if (naceLower.match(/^f\s/) || naceLower.match(/^f\./))
    return "construction";
  if (naceLower.match(/^h\s/) || naceLower.match(/^h\./)) return "transport";

  return null;
}

// ---------------------------------------------------------------------------
// Fine and legal article extraction
// ---------------------------------------------------------------------------

/**
 * Extract a fine amount from Slovak text.
 *
 * Slovak fines are denominated in EUR. Common patterns:
 *   - "pokutu vo vyske 3 200 000 eur"
 *   - "pokuta 8 500 000 EUR"
 *   - "sankcia: 1 200 000 eur"
 *   - "22 mil. eur"
 *   - "1,5 miliona eur"
 *   - "pokuta ... presahovala 17 milionov eur"
 */
function extractFineAmount(text: string): number | null {
  const patterns: Array<{ re: RegExp; multiplier?: number }> = [
    // "pokut(a/u/y) vo vyske 3 200 000 eur" or "uložil pokutu 8 500 000 EUR"
    {
      re: /(?:pokut[auy]|sankci[aeu]|vo\s+v[ýy][šs]ke)\s+(?:celkom\s+)?(\d[\d\s.]*\d)\s*(?:,-)?\s*(?:eur|€)/gi,
    },
    // Inline amount with EUR
    { re: /(\d[\d\s.]*\d)\s*(?:,-)?\s*(?:eur|€)/gi },
    // "N mil. eur" or "N milionov eur"
    {
      re: /(\d+(?:[,.]?\d+)?)\s*(?:mil\.|mili[oó]n(?:ov)?)\s*(?:eur|€)/gi,
      multiplier: 1_000_000,
    },
    // "N mld. eur" or "N miliard eur"
    {
      re: /(\d+(?:[,.]?\d+)?)\s*(?:mld\.|miliárd?(?:y)?)\s*(?:eur|€)/gi,
      multiplier: 1_000_000_000,
    },
  ];

  for (const { re, multiplier } of patterns) {
    const match = re.exec(text);
    if (match?.[1]) {
      let numStr = match[1].replace(/\s/g, "").replace(/\./g, "");

      if (multiplier) {
        numStr = numStr.replace(",", ".");
        const val = parseFloat(numStr);
        if (!isNaN(val) && val > 0) return val * multiplier;
      } else {
        numStr = numStr.replace(/,$/, "");
        const val = parseInt(numStr, 10);
        if (!isNaN(val) && val > 0) return val;
      }
    }
  }

  return null;
}

/**
 * Extract cited legal provisions from Slovak text.
 *
 * Common Slovak competition law references:
 *   - ZOHS (Zakon o ochrane hospodarskej sutaze) - Act No. 187/2021 Z. z.
 *     (and its predecessor Act No. 136/2001 Z. z.)
 *     e.g. "§ 4 ZOHS" (prohibited agreements), "§ 8 ZOHS" (abuse of dominance)
 *     "§ 7 ZOHS" (concentrations), "§ 22 ZOHS" (penalties)
 *   - ZFEU / SFEU (Zmluva o fungovani EU / Treaty on the Functioning of the EU)
 *     e.g. "cl. 101 ZFEU", "cl. 102 ZFEU"
 *   - Direct references: "§ 4 zakona c. 187/2021 Z. z."
 */
function extractLegalArticles(text: string): string[] {
  const articles: Set<string> = new Set();

  // "§ N ZOHS" or "§ N ods. M ZOHS" or "§ N zakona o ochrane"
  const zohsPattern =
    /§\s*(\d+(?:\s*ods\.\s*\d+)?(?:\s*písm\.\s*[a-z]\))?)\s*(?:zákon[a]?\s+(?:[oč]\.\s*)?(?:187\/2021|136\/2001)|ZOHS|zákona\s+o\s+ochrane)/gi;
  let m: RegExpExecArray | null;
  while ((m = zohsPattern.exec(text)) !== null) {
    articles.add(`ZOHS § ${m[1]!.replace(/\s+/g, " ").trim()}`);
  }

  // Standalone "§ N" followed by act reference
  const standaloneZohs =
    /§\s*(\d+(?:\s+ods\.\s*\d+)?)\s+z[áa]kona\s+[čc]\.\s*(?:187\/2021|136\/2001)/gi;
  while ((m = standaloneZohs.exec(text)) !== null) {
    articles.add(`ZOHS § ${m[1]!.trim()}`);
  }

  // Practical reference patterns from the "Praktika" field
  // "§ 7 ods. 1 pism. b) zakona c. 187/2021 Z. z."
  const praktikaPattern =
    /§\s*(\d+)\s+ods\.\s*(\d+)\s+p[ií]sm\.\s*([a-z])\)/gi;
  while ((m = praktikaPattern.exec(text)) !== null) {
    articles.add(`ZOHS § ${m[1]} ods. ${m[2]} písm. ${m[3]})`);
  }

  // "cl. 101 ZFEU" / "clanku 102 ZFEU" / "cl. 101 SFEU"
  const euPattern =
    /[čc]l(?:[áa]nk[uú]|\.)\s*(101|102)\s*(?:ZFEU|SFEU|TFEU|Zmluvy)/gi;
  while ((m = euPattern.exec(text)) !== null) {
    articles.add(`ZFEU čl. ${m[1]}`);
  }

  // "Art. 101 TFEU" (English references in some pages)
  const artPattern = /Art(?:icle)?\.?\s*(101|102)\s*(?:TFEU|SFEU|ZFEU)/gi;
  while ((m = artPattern.exec(text)) !== null) {
    articles.add(`ZFEU čl. ${m[1]}`);
  }

  return [...articles];
}

/** Extract acquiring and target parties from a merger title/body. */
function extractMergerParties(
  title: string,
  bodyText: string,
  meta: Record<string, string>,
): { acquiring: string | null; target: string | null } {
  const ucastnici = meta["ucastnici"] ?? "";
  const allText = `${title}\n${ucastnici}\n${bodyText.slice(0, 3000)}`;

  // Common PMU merger title pattern: "Company / Company B"
  // or "Prevzatie / akvizicia spolocnosti X spolocnostou Y"
  const slashMatch = allText.match(
    /(?:prevzatie|akvizícia|akvizicia|získanie|ziskanie|zlúčenie|zlucenie)\s+(?:kontroly\s+nad\s+)?(.+?)\s+(?:spoločnosťou|spolocnostou|skupinou|podnikateľom|podnikatelom)\s+(.+?)(?:\.|,|\n|$)/i,
  );
  if (slashMatch) {
    return {
      acquiring: slashMatch[2]!.trim() || null,
      target: slashMatch[1]!.trim() || null,
    };
  }

  // "X získava (exclusive/joint) kontrolu nad Y"
  const kontrolaMatch = allText.match(
    /(.+?)\s+(?:získava|ziskava|nadobúda|nadobuda)\s+(?:\w+\s+)?kontrolu?\s+nad\s+(.+?)(?:\.|,|\n|$)/i,
  );
  if (kontrolaMatch) {
    return {
      acquiring: kontrolaMatch[1]!.trim() || null,
      target: kontrolaMatch[2]!.trim() || null,
    };
  }

  // Title with "/" or "—" separator
  const simpleSplit = title.match(/^(.+?)\s*[/–—]\s*(.+)$/);
  if (simpleSplit) {
    return {
      acquiring: simpleSplit[1]!.trim() || null,
      target: simpleSplit[2]!.trim() || null,
    };
  }

  // Look for "nadobúdateľ" (acquirer) and "cieľový podnik" (target)
  const bodyAcquirer = allText.match(
    /(?:nadobúdateľ|nadobudatel|navrhovateľ|navrhovatel)\s*:?\s*([^.\n]+)/i,
  );
  const bodyTarget = allText.match(
    /(?:cieľov[ýa]|cielov[ya]|nadobúdan[ýa]|nadobudan[ya])\s+(?:podnik|spoločnosť|spolocnost)\s*:?\s*([^.\n]+)/i,
  );

  return {
    acquiring: bodyAcquirer?.[1]?.trim() ?? null,
    target: bodyTarget?.[1]?.trim() ?? null,
  };
}

/**
 * Build a case number for the database.
 *
 * Prefers the decision number (Rozhodnutie) from the detail page table
 * (e.g. 2026/KOH/SKO/3/5), prefixed with "PMU-".
 * Falls back to a URL-derived identifier.
 */
function buildCaseNumber(
  meta: Record<string, string>,
  url: string,
): string {
  // Decision number from detail table (e.g. "2026/KOH/SKO/3/5")
  const rozhodnutie = meta["rozhodnutie"];
  if (rozhodnutie) {
    const cleaned = rozhodnutie.trim().replace(/\s+/g, "");
    return cleaned.startsWith("PMU-") ? cleaned : `PMU-${cleaned}`;
  }

  // Extract from legal basis / praktika field if it contains a case reference
  // Known type codes: KOH (concentrations), DOH (agreements/cartels),
  // DOZ (abuse of dominance), DZ (dominance), ZKP, OSS, ZPR (second instance)
  const praktika = meta["praktika"] ?? "";
  const caseRefMatch = praktika.match(
    /(\d{4}\/(?:KOH|DOH|DOZ|DZ|ZKP|OSS|ZPR)\/\w+\/\d+\/\d+)/,
  );
  if (caseRefMatch) {
    return `PMU-${caseRefMatch[1]}`;
  }

  // Derive from URL slug
  const slugMatch = url.match(/antimon\.gov\.sk\/([^/?]+)\/?$/);
  if (slugMatch) {
    return `PMU-WEB-${slugMatch[1]}`;
  }

  return `PMU-UNKNOWN-${Date.now()}`;
}

// ---------------------------------------------------------------------------
// Page parsing -- combine everything into a decision or merger record
// ---------------------------------------------------------------------------

function parseDetailPage(
  html: string,
  url: string,
  listingEntry: ListingEntry,
): { decision: ParsedDecision | null; merger: ParsedMerger | null } {
  const $ = cheerio.load(html);

  // Title: prefer h1, fall back to listing entry
  const title =
    $("h1").first().text().trim() ||
    $('meta[property="og:title"]').attr("content")?.trim() ||
    $("title")
      .text()
      .trim()
      .replace(/\s*\|\s*Protimonopoln[ýy].*$/, "") ||
    listingEntry.title;

  if (!title || title.length < 3) {
    return { decision: null, merger: null };
  }

  const meta = extractMetadata($);
  const bodyText = extractBodyText($);
  const pdfLink = extractPdfLink($);

  // Build full_text: combine all available text
  const fullTextParts: string[] = [];
  fullTextParts.push(title);

  // Add metadata as structured text
  const metaEntries = Object.entries(meta)
    .filter(([, v]) => v.length > 0)
    .map(([k, v]) => `${k}: ${v}`);
  if (metaEntries.length > 0) {
    fullTextParts.push(metaEntries.join("\n"));
  }

  if (bodyText && bodyText.length > 50) {
    fullTextParts.push(bodyText);
  }

  if (pdfLink) {
    fullTextParts.push(`Dokument PDF: ${pdfLink}`);
  }

  const fullText = fullTextParts.join("\n\n");

  // Minimum content threshold
  if (fullText.length < 30) {
    return { decision: null, merger: null };
  }

  // Date: prefer the decision date from the table, then publication date
  const rawDate =
    meta["vydanie_rozhodnutia"] ??
    meta["pravoplatnost"] ??
    meta["publikovane"] ??
    "";
  let date = parseDate(rawDate);
  if (!date) {
    date = listingEntry.date;
  }

  // Case number
  const caseNumber = buildCaseNumber(meta, url);

  // Classification
  const { isMerger, isDecision, type, outcome } = classifyCase(
    meta,
    title,
    fullText,
    listingEntry.type,
  );
  const sector = classifySector(meta, title, fullText);

  // Summary: first 500 chars of body text
  const summary =
    bodyText.length > 50
      ? bodyText.slice(0, 500).replace(/\s+/g, " ").trim()
      : null;

  if (isMerger) {
    const { acquiring, target } = extractMergerParties(
      title,
      bodyText,
      meta,
    );

    return {
      decision: null,
      merger: {
        case_number: caseNumber,
        title,
        date,
        sector,
        acquiring_party: acquiring,
        target,
        summary,
        full_text: fullText,
        outcome: outcome ?? "cleared_phase1",
        turnover: null, // Not reliably extractable from HTML
      },
    };
  }

  if (isDecision || type !== null) {
    const parties = extractParties(meta["ucastnici"]);
    const fineAmount = extractFineAmount(fullText);
    const articles = extractLegalArticles(fullText);

    return {
      decision: {
        case_number: caseNumber,
        title,
        date,
        type,
        sector,
        parties,
        summary,
        full_text: fullText,
        outcome: outcome ?? (fineAmount ? "fine" : "pending"),
        fine_amount: fineAmount,
        zohs_articles: articles.length > 0 ? articles.join(", ") : null,
        status: "final",
      },
      merger: null,
    };
  }

  // Cannot clearly classify -- treat as a generic decision
  const parties = extractParties(meta["ucastnici"]);
  const fineAmount = extractFineAmount(fullText);
  const articles = extractLegalArticles(fullText);

  return {
    decision: {
      case_number: caseNumber,
      title,
      date,
      type: type ?? "decision",
      sector,
      parties,
      summary,
      full_text: fullText,
      outcome: outcome ?? (fineAmount ? "fine" : "pending"),
      fine_amount: fineAmount,
      zohs_articles: articles.length > 0 ? articles.join(", ") : null,
      status: "final",
    },
    merger: null,
  };
}

// ---------------------------------------------------------------------------
// Database operations
// ---------------------------------------------------------------------------

function initDb(): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
    console.log(`Created data directory: ${dir}`);
  }

  if (force && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    console.log(`Deleted existing database (--force)`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);

  return db;
}

function prepareStatements(db: Database.Database) {
  const insertDecision = db.prepare(`
    INSERT OR IGNORE INTO decisions
      (case_number, title, date, type, sector, parties, summary, full_text,
       outcome, fine_amount, gwb_articles, status)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const upsertDecision = db.prepare(`
    INSERT INTO decisions
      (case_number, title, date, type, sector, parties, summary, full_text,
       outcome, fine_amount, gwb_articles, status)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(case_number) DO UPDATE SET
      title = excluded.title,
      date = excluded.date,
      type = excluded.type,
      sector = excluded.sector,
      parties = excluded.parties,
      summary = excluded.summary,
      full_text = excluded.full_text,
      outcome = excluded.outcome,
      fine_amount = excluded.fine_amount,
      gwb_articles = excluded.gwb_articles,
      status = excluded.status
  `);

  const insertMerger = db.prepare(`
    INSERT OR IGNORE INTO mergers
      (case_number, title, date, sector, acquiring_party, target, summary,
       full_text, outcome, turnover)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const upsertMerger = db.prepare(`
    INSERT INTO mergers
      (case_number, title, date, sector, acquiring_party, target, summary,
       full_text, outcome, turnover)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(case_number) DO UPDATE SET
      title = excluded.title,
      date = excluded.date,
      sector = excluded.sector,
      acquiring_party = excluded.acquiring_party,
      target = excluded.target,
      summary = excluded.summary,
      full_text = excluded.full_text,
      outcome = excluded.outcome,
      turnover = excluded.turnover
  `);

  const upsertSector = db.prepare(`
    INSERT INTO sectors (id, name, name_en, description, decision_count, merger_count)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      decision_count = excluded.decision_count,
      merger_count = excluded.merger_count
  `);

  return {
    insertDecision,
    upsertDecision,
    insertMerger,
    upsertMerger,
    upsertSector,
  };
}

// ---------------------------------------------------------------------------
// Main ingestion pipeline
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log("=== PMU (Protimonopolny urad SR) Competition Decisions Crawler ===");
  console.log(`  Database:   ${DB_PATH}`);
  console.log(`  Dry run:    ${dryRun}`);
  console.log(`  Resume:     ${resume}`);
  console.log(`  Force:      ${force}`);
  console.log(`  Max pages:  ${maxPages}`);
  console.log("");

  // Load resume state
  const state = loadState();
  const processedSet = new Set(state.processedUrls);

  // ---- Step 1: Discover decision URLs from paginated listing ----
  console.log("Step 1: Discovering decision URLs from listing pages...\n");
  const allEntries: ListingEntry[] = [];
  let emptyPageStreak = 0;

  for (let page = 0; page < maxPages; page++) {
    // PMU pagination: ?page=0 is the first page, ?page=N for subsequent pages
    const listingUrl =
      page === 0
        ? `${BASE_URL}${LISTING_PATH}`
        : `${BASE_URL}${LISTING_PATH}?page=${page}`;

    console.log(`  Fetching listing page ${page + 1}/${maxPages}...`);

    const html = await rateLimitedFetch(listingUrl);
    if (!html) {
      console.warn(`  [WARN] Could not fetch listing page ${page + 1}`);
      emptyPageStreak++;
      if (emptyPageStreak >= 3) {
        console.log("  Three consecutive empty pages -- stopping discovery.");
        break;
      }
      continue;
    }

    const entries = parseListingPage(html);
    if (entries.length === 0) {
      emptyPageStreak++;
      console.log(`  No entries found on page ${page + 1}`);
      if (emptyPageStreak >= 3) {
        console.log("  Three consecutive empty pages -- stopping discovery.");
        break;
      }
      continue;
    }

    emptyPageStreak = 0;
    allEntries.push(...entries);
    console.log(
      `    Found ${entries.length} entries (total: ${allEntries.length})`,
    );
  }

  // ---- Step 1b: Crawl supplementary case listing pages ----
  console.log("\nStep 1b: Crawling supplementary case listing pages...\n");

  for (const suppPath of SUPPLEMENTARY_LISTING_PATHS) {
    let suppEmptyStreak = 0;

    for (let page = 0; page < maxPages; page++) {
      const suppUrl =
        page === 0
          ? `${BASE_URL}${suppPath}`
          : `${BASE_URL}${suppPath}?page=${page}`;

      console.log(`  Fetching ${suppPath} page ${page + 1}...`);
      const html = await rateLimitedFetch(suppUrl);
      if (!html) {
        suppEmptyStreak++;
        if (suppEmptyStreak >= 3) break;
        continue;
      }

      const entries = parseListingPage(html);
      if (entries.length === 0) {
        suppEmptyStreak++;
        if (suppEmptyStreak >= 3) break;
        continue;
      }

      suppEmptyStreak = 0;
      allEntries.push(...entries);
      console.log(
        `    Found ${entries.length} entries (total: ${allEntries.length})`,
      );
    }
  }

  // Deduplicate by URL
  const seenUrls = new Set<string>();
  const uniqueEntries = allEntries.filter((e) => {
    if (seenUrls.has(e.url)) return false;
    seenUrls.add(e.url);
    return true;
  });

  console.log(
    `\nDiscovered ${uniqueEntries.length} unique decision URLs (from ${allEntries.length} total)`,
  );

  // Filter already-processed URLs (for --resume)
  const entriesToProcess = resume
    ? uniqueEntries.filter((e) => !processedSet.has(e.url))
    : uniqueEntries;

  console.log(`URLs to process: ${entriesToProcess.length}`);
  if (resume && uniqueEntries.length !== entriesToProcess.length) {
    console.log(
      `  Skipping ${uniqueEntries.length - entriesToProcess.length} already-processed URLs`,
    );
  }

  if (entriesToProcess.length === 0) {
    console.log("Nothing to process. Exiting.");
    return;
  }

  // ---- Step 2: Initialise database (unless dry run) ----
  let db: Database.Database | null = null;
  let stmts: ReturnType<typeof prepareStatements> | null = null;

  if (!dryRun) {
    db = initDb();
    stmts = prepareStatements(db);
  }

  // ---- Step 3: Fetch and parse each decision detail page ----
  console.log("\nStep 2: Processing individual decision pages...\n");

  let decisionsIngested = state.decisionsIngested;
  let mergersIngested = state.mergersIngested;
  let errors = 0;
  let skipped = 0;
  const sectorCounts: SectorAccumulator = {};

  for (let i = 0; i < entriesToProcess.length; i++) {
    const entry = entriesToProcess[i]!;
    const progress = `[${i + 1}/${entriesToProcess.length}]`;

    console.log(`${progress} Fetching: ${entry.url}`);

    const html = await rateLimitedFetch(entry.url);
    if (!html) {
      console.log(`  SKIP -- could not fetch`);
      state.errors.push(`fetch_failed: ${entry.url}`);
      errors++;
      continue;
    }

    try {
      const { decision, merger } = parseDetailPage(html, entry.url, entry);

      if (decision) {
        if (dryRun) {
          console.log(
            `  DECISION: ${decision.case_number} -- ${decision.title.slice(0, 80)}`,
          );
          console.log(
            `    type=${decision.type}, sector=${decision.sector}, outcome=${decision.outcome}, fine=${decision.fine_amount}`,
          );
        } else {
          const stmt = force
            ? stmts!.upsertDecision
            : stmts!.insertDecision;
          stmt.run(
            decision.case_number,
            decision.title,
            decision.date,
            decision.type,
            decision.sector,
            decision.parties,
            decision.summary,
            decision.full_text,
            decision.outcome,
            decision.fine_amount,
            decision.zohs_articles,
            decision.status,
          );
          console.log(`  INSERTED decision: ${decision.case_number}`);
        }

        decisionsIngested++;

        if (decision.sector) {
          if (!sectorCounts[decision.sector]) {
            sectorCounts[decision.sector] = {
              name: decision.sector,
              name_en: null,
              description: null,
              decisionCount: 0,
              mergerCount: 0,
            };
          }
          sectorCounts[decision.sector]!.decisionCount++;
        }
      } else if (merger) {
        if (dryRun) {
          console.log(
            `  MERGER: ${merger.case_number} -- ${merger.title.slice(0, 80)}`,
          );
          console.log(
            `    sector=${merger.sector}, outcome=${merger.outcome}, acquiring=${merger.acquiring_party?.slice(0, 40)}`,
          );
        } else {
          const stmt = force ? stmts!.upsertMerger : stmts!.insertMerger;
          stmt.run(
            merger.case_number,
            merger.title,
            merger.date,
            merger.sector,
            merger.acquiring_party,
            merger.target,
            merger.summary,
            merger.full_text,
            merger.outcome,
            merger.turnover,
          );
          console.log(`  INSERTED merger: ${merger.case_number}`);
        }

        mergersIngested++;

        if (merger.sector) {
          if (!sectorCounts[merger.sector]) {
            sectorCounts[merger.sector] = {
              name: merger.sector,
              name_en: null,
              description: null,
              decisionCount: 0,
              mergerCount: 0,
            };
          }
          sectorCounts[merger.sector]!.mergerCount++;
        }
      } else {
        console.log(`  SKIP -- could not extract structured data`);
        skipped++;
      }

      // Mark URL as processed
      processedSet.add(entry.url);
      state.processedUrls.push(entry.url);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error(`  ERROR: ${message}`);
      state.errors.push(`parse_error: ${entry.url}: ${message}`);
      errors++;
    }

    // Save state periodically (every 25 URLs)
    if ((i + 1) % 25 === 0) {
      state.decisionsIngested = decisionsIngested;
      state.mergersIngested = mergersIngested;
      saveState(state);
      console.log(`  [checkpoint] State saved after ${i + 1} URLs`);
    }
  }

  // ---- Step 4: Update sector counts ----
  if (!dryRun && db && stmts) {
    const sectorMeta: Record<string, { name: string; name_en: string }> = {
      energy: {
        name: "Energetika",
        name_en: "Energy",
      },
      telecommunications: {
        name: "Telekomunikácie",
        name_en: "Telecommunications",
      },
      retail: {
        name: "Maloobchod",
        name_en: "Retail",
      },
      pharmaceuticals: {
        name: "Farmaceutický priemysel",
        name_en: "Pharmaceuticals and Healthcare",
      },
      banking: {
        name: "Bankovníctvo a finančné služby",
        name_en: "Banking and Financial Services",
      },
      construction: {
        name: "Stavebníctvo",
        name_en: "Construction",
      },
      transport: {
        name: "Doprava a logistika",
        name_en: "Transport and Logistics",
      },
      automotive: {
        name: "Automobilový priemysel",
        name_en: "Automotive",
      },
      agriculture: {
        name: "Poľnohospodárstvo a potravinárstvo",
        name_en: "Agriculture and Food",
      },
      media: {
        name: "Médiá a reklama",
        name_en: "Media and Advertising",
      },
      digital: {
        name: "Digitálna ekonomika",
        name_en: "Digital Economy",
      },
      chemicals: {
        name: "Chemický priemysel",
        name_en: "Chemicals",
      },
      waste_management: {
        name: "Odpadové hospodárstvo",
        name_en: "Waste Management",
      },
      real_estate: {
        name: "Nehnuteľnosti",
        name_en: "Real Estate",
      },
      insurance: {
        name: "Poisťovníctvo",
        name_en: "Insurance",
      },
    };

    // Count decisions and mergers per sector from the database
    const decisionSectorCounts = db
      .prepare(
        "SELECT sector, COUNT(*) as cnt FROM decisions WHERE sector IS NOT NULL GROUP BY sector",
      )
      .all() as Array<{ sector: string; cnt: number }>;
    const mergerSectorCounts = db
      .prepare(
        "SELECT sector, COUNT(*) as cnt FROM mergers WHERE sector IS NOT NULL GROUP BY sector",
      )
      .all() as Array<{ sector: string; cnt: number }>;

    const finalSectorCounts: Record<
      string,
      { decisions: number; mergers: number }
    > = {};
    for (const row of decisionSectorCounts) {
      if (!finalSectorCounts[row.sector])
        finalSectorCounts[row.sector] = { decisions: 0, mergers: 0 };
      finalSectorCounts[row.sector]!.decisions = row.cnt;
    }
    for (const row of mergerSectorCounts) {
      if (!finalSectorCounts[row.sector])
        finalSectorCounts[row.sector] = { decisions: 0, mergers: 0 };
      finalSectorCounts[row.sector]!.mergers = row.cnt;
    }

    const updateSectors = db.transaction(() => {
      for (const [id, counts] of Object.entries(finalSectorCounts)) {
        const info = sectorMeta[id];
        stmts!.upsertSector.run(
          id,
          info?.name ?? id,
          info?.name_en ?? null,
          null,
          counts.decisions,
          counts.mergers,
        );
      }
    });
    updateSectors();

    console.log(
      `\nUpdated ${Object.keys(finalSectorCounts).length} sector records`,
    );
  }

  // ---- Step 5: Final state save ----
  state.decisionsIngested = decisionsIngested;
  state.mergersIngested = mergersIngested;
  saveState(state);

  // ---- Step 6: Summary ----
  if (!dryRun && db) {
    const decisionCount = (
      db.prepare("SELECT count(*) as cnt FROM decisions").get() as {
        cnt: number;
      }
    ).cnt;
    const mergerCount = (
      db.prepare("SELECT count(*) as cnt FROM mergers").get() as {
        cnt: number;
      }
    ).cnt;
    const sectorCount = (
      db.prepare("SELECT count(*) as cnt FROM sectors").get() as {
        cnt: number;
      }
    ).cnt;

    console.log("\n=== Ingestion Complete ===");
    console.log(`  Decisions in DB:  ${decisionCount}`);
    console.log(`  Mergers in DB:    ${mergerCount}`);
    console.log(`  Sectors in DB:    ${sectorCount}`);
    console.log(`  New decisions:    ${decisionsIngested}`);
    console.log(`  New mergers:      ${mergersIngested}`);
    console.log(`  Errors:           ${errors}`);
    console.log(`  Skipped:          ${skipped}`);
    console.log(`  State saved to:   ${STATE_FILE}`);

    db.close();
  } else {
    console.log("\n=== Dry Run Complete ===");
    console.log(`  Decisions found:  ${decisionsIngested}`);
    console.log(`  Mergers found:    ${mergersIngested}`);
    console.log(`  Errors:           ${errors}`);
    console.log(`  Skipped:          ${skipped}`);
  }

  console.log("\nHotovo.");
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
