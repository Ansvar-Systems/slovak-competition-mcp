#!/usr/bin/env node

/**
 * Slovak Competition MCP — stdio entry point.
 *
 * Provides MCP tools for querying PMU (Protimonopolný úrad — Slovak Antimonopoly
 * Office) decisions, merger control cases, and sector enforcement activity under
 * Slovak competition law (ZOHS — Zákon o ochrane hospodárskej súťaže).
 *
 * Tool prefix: sk_comp_
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { CallToolRequestSchema, ListToolsRequestSchema } from "@modelcontextprotocol/sdk/types.js";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { z } from "zod";
import { searchDecisions, getDecision, searchMergers, getMerger, listSectors } from "./db.js";
import { buildCitation } from "./utils/citation.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

let pkgVersion = "0.1.0";
try {
  const pkg = JSON.parse(readFileSync(join(__dirname, "..", "package.json"), "utf8")) as { version: string };
  pkgVersion = pkg.version;
} catch { /* fallback */ }

const SERVER_NAME = "slovak-competition-mcp";

const TOOLS = [
  {
    name: "sk_comp_search_decisions",
    description: "Full-text search across PMU enforcement decisions (abuse of dominance, cartels, sector inquiries) under Slovak competition law (ZOHS). Returns matching decisions with case number, parties, outcome, fine amount, and ZOHS articles cited.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: { type: "string", description: "Search query (e.g., 'zneužívanie dominantného postavenia', 'kartel', 'koncentrácia')" },
        type: { type: "string", enum: ["abuse_of_dominance", "cartel", "merger", "sector_inquiry"], description: "Filter by decision type. Optional." },
        sector: { type: "string", description: "Filter by sector ID (e.g., 'telecommunications', 'energy', 'retail'). Optional." },
        outcome: { type: "string", enum: ["prohibited", "cleared", "cleared_with_conditions", "fine"], description: "Filter by outcome. Optional." },
        limit: { type: "number", description: "Maximum number of results to return. Defaults to 20." },
      },
      required: ["query"],
    },
  },
  {
    name: "sk_comp_get_decision",
    description: "Get a specific PMU decision by case number (e.g., '2023/PM/1/1/001').",
    inputSchema: {
      type: "object" as const,
      properties: { case_number: { type: "string", description: "PMU case number" } },
      required: ["case_number"],
    },
  },
  {
    name: "sk_comp_search_mergers",
    description: "Search PMU merger control decisions (concentrations). Returns merger cases with acquiring party, target, sector, and outcome.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: { type: "string", description: "Search query (e.g., 'koncentrácia', 'prevzatie', 'telekomunikácie')" },
        sector: { type: "string", description: "Filter by sector ID. Optional." },
        outcome: { type: "string", enum: ["cleared", "cleared_phase1", "cleared_with_conditions", "prohibited"], description: "Filter by merger outcome. Optional." },
        limit: { type: "number", description: "Maximum number of results to return. Defaults to 20." },
      },
      required: ["query"],
    },
  },
  {
    name: "sk_comp_get_merger",
    description: "Get a specific PMU merger control decision by case number.",
    inputSchema: {
      type: "object" as const,
      properties: { case_number: { type: "string", description: "PMU merger case number" } },
      required: ["case_number"],
    },
  },
  {
    name: "sk_comp_list_sectors",
    description: "List all sectors with PMU enforcement activity, including decision counts and merger counts per sector.",
    inputSchema: { type: "object" as const, properties: {}, required: [] },
  },
  {
    name: "sk_comp_about",
    description: "Return metadata about this MCP server: version, data source, coverage, and tool list.",
    inputSchema: { type: "object" as const, properties: {}, required: [] },
  },
];

const SearchDecisionsArgs = z.object({
  query: z.string().min(1),
  type: z.enum(["abuse_of_dominance", "cartel", "merger", "sector_inquiry"]).optional(),
  sector: z.string().optional(),
  outcome: z.enum(["prohibited", "cleared", "cleared_with_conditions", "fine"]).optional(),
  limit: z.number().int().positive().max(100).optional(),
});
const GetDecisionArgs = z.object({ case_number: z.string().min(1) });
const SearchMergersArgs = z.object({
  query: z.string().min(1),
  sector: z.string().optional(),
  outcome: z.enum(["cleared", "cleared_phase1", "cleared_with_conditions", "prohibited"]).optional(),
  limit: z.number().int().positive().max(100).optional(),
});
const GetMergerArgs = z.object({ case_number: z.string().min(1) });

function textContent(data: unknown) { return { content: [{ type: "text" as const, text: JSON.stringify(data, null, 2) }] }; }
function errorContent(message: string) { return { content: [{ type: "text" as const, text: message }], isError: true as const }; }

const server = new Server({ name: SERVER_NAME, version: pkgVersion }, { capabilities: { tools: {} } });
server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: TOOLS }));
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args = {} } = request.params;
  try {
    switch (name) {
      case "sk_comp_search_decisions": { const p = SearchDecisionsArgs.parse(args); const r = searchDecisions({ query: p.query, type: p.type, sector: p.sector, outcome: p.outcome, limit: p.limit }); return textContent({ results: r, count: r.length }); }
      case "sk_comp_get_decision": {
        const p = GetDecisionArgs.parse(args);
        const d = getDecision(p.case_number);
        if (!d) return errorContent(`Decision not found: ${p.case_number}`);
        const _citation = buildCitation(
          p.case_number,
          (d as Record<string, unknown>).title as string || p.case_number,
          "sk_comp_get_decision",
          { case_number: p.case_number },
        );
        return textContent({ ...d as Record<string, unknown>, _citation });
      }
      case "sk_comp_search_mergers": { const p = SearchMergersArgs.parse(args); const r = searchMergers({ query: p.query, sector: p.sector, outcome: p.outcome, limit: p.limit }); return textContent({ results: r, count: r.length }); }
      case "sk_comp_get_merger": {
        const p = GetMergerArgs.parse(args);
        const m = getMerger(p.case_number);
        if (!m) return errorContent(`Merger case not found: ${p.case_number}`);
        const _citation = buildCitation(
          p.case_number,
          (m as Record<string, unknown>).title as string || p.case_number,
          "sk_comp_get_merger",
          { case_number: p.case_number },
        );
        return textContent({ ...m as Record<string, unknown>, _citation });
      }
      case "sk_comp_list_sectors": { const s = listSectors(); return textContent({ sectors: s, count: s.length }); }
      case "sk_comp_about": return textContent({ name: SERVER_NAME, version: pkgVersion, description: "PMU (Protimonopolný úrad Slovenskej republiky — Slovak Antimonopoly Office) MCP server. Provides access to Slovak competition law enforcement decisions, merger control cases, and sector enforcement data under the ZOHS (Zákon o ochrane hospodárskej súťaže).", data_source: "PMU Slovakia (https://www.antimon.gov.sk/)", coverage: { decisions: "Abuse of dominance, cartel enforcement, and sector inquiries under ZOHS", mergers: "Merger control decisions (concentrations) — Phase I and Phase II", sectors: "Telecommunications, energy, retail, financial services, digital economy, food and agriculture" }, tools: TOOLS.map(t => ({ name: t.name, description: t.description })) });
      default: return errorContent(`Unknown tool: ${name}`);
    }
  } catch (err) { return errorContent(`Error executing ${name}: ${err instanceof Error ? err.message : String(err)}`); }
});

async function main(): Promise<void> {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  process.stderr.write(`${SERVER_NAME} v${pkgVersion} running on stdio\n`);
}
main().catch(err => { process.stderr.write(`Fatal error: ${err instanceof Error ? err.message : String(err)}\n`); process.exit(1); });
