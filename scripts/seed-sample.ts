/**
 * Seed the PMU (Slovak Antimonopoly Office) database with sample decisions,
 * mergers, and sectors for testing.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["PMU_SK_DB_PATH"] ?? "data/pmu-sk.db";
const force = process.argv.includes("--force");

const dir = dirname(DB_PATH);
if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
if (force && existsSync(DB_PATH)) { unlinkSync(DB_PATH); console.log(`Deleted existing database at ${DB_PATH}`); }

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);
console.log(`Database initialised at ${DB_PATH}`);

const sectors = [
  { id: "energy", name: "Energetika", name_en: "Energy", description: "Výroba, distribúcia a dodávka elektriny a plynu, obnoviteľné zdroje a sieťová infraštruktúra.", decision_count: 3, merger_count: 2 },
  { id: "retail", name: "Maloobchod", name_en: "Retail", description: "Potravinový maloobchod, obchodné reťazce a elektronický obchod.", decision_count: 2, merger_count: 1 },
  { id: "telecommunications", name: "Telekomunikácie", name_en: "Telecommunications", description: "Mobilné siete, širokopásmový internet a televízne distribučné služby.", decision_count: 2, merger_count: 1 },
  { id: "banking", name: "Bankovníctvo", name_en: "Banking", description: "Komerčné banky, poisťovníctvo a platobné služby.", decision_count: 1, merger_count: 2 },
  { id: "construction", name: "Stavebníctvo", name_en: "Construction", description: "Stavebné práce, verejné obstarávanie a infraštruktúrne projekty.", decision_count: 2, merger_count: 0 },
];

const is = db.prepare("INSERT OR IGNORE INTO sectors (id, name, name_en, description, decision_count, merger_count) VALUES (?, ?, ?, ?, ?, ?)");
for (const s of sectors) is.run(s.id, s.name, s.name_en, s.description, s.decision_count, s.merger_count);
console.log(`Inserted ${sectors.length} sectors`);

const decisions = [
  { case_number: "2023/PM/1/1/021", title: "Slovak Telekom — zneužívanie dominantného postavenia na trhu pevného širokopásmového pripojenia", date: "2023-05-23", type: "abuse_of_dominance", sector: "telecommunications", parties: JSON.stringify(["Slovak Telekom, a.s."]), summary: "PMÚ zistil, že Slovak Telekom zneužíval dominantné postavenie na trhu prístupu k pevnej telekomunikačnej infraštruktúre odmietaním prístupu konkurentom za primeraných podmienok.", full_text: "PMÚ začal konanie voči Slovak Telekom, a.s. pre podozrenie z porušenia § 8 ZOHS. Slovak Telekom má dominantné postavenie na trhu veľkoobchodného prístupu k miestnym slučkám v SR. PMÚ zistil, že Slovak Telekom odmietol alternatívnym operátorom prístup k fyzickej infraštruktúre za nediskriminačných podmienok, čo im sťažilo poskytovanie maloobchodných širokopásmových služieb. PMÚ uložil pokutu a nariadil zosúladenie podmienok prístupu s požiadavkami regulátora RÚKT.", outcome: "fine", fine_amount: 3_200_000, gwb_articles: JSON.stringify(["8", "22"]), status: "final" },
  { case_number: "2022/PM/3/1/008", title: "Stavebné spoločnosti — kartélová dohoda pri verejnom obstarávaní diaľnic", date: "2022-09-14", type: "cartel", sector: "construction", parties: JSON.stringify(["Strabag SE Slovakia", "Eurovia SK, a.s.", "Doprastav, a.s."]), summary: "PMÚ odhalil a pokutoval kartelovú dohodu troch veľkých stavebných spoločností koordinujúcich ponuky vo verejnom obstarávaní diaľničnej infraštruktúry.", full_text: "PMÚ odhalil kartelovú dohodu medzi troma vedúcimi stavebnými spoločnosťami pri podávaní ponúk na zákazky verejného obstarávania pre výstavbu a rekonštrukciu diaľnic. Dohoda zahŕňala koordináciu cien, rozdelenie zákaziek a podávanie krycích ponúk. PMÚ vykonal dôkazy z inšpekcií, e-mailovej komunikácie a svedeckých výpovedí. Všetkým trom spoločnostiam boli uložené pokuty. Prípad bol postúpený Úradu na ochranu hospodárskej súťaže pre ďalšie posúdenie trestnoprávnych aspektov.", outcome: "fine", fine_amount: 8_500_000, gwb_articles: JSON.stringify(["4", "38"]), status: "final" },
  { case_number: "2023/PM/2/1/005", title: "Retail — cenové podmienky dodávateľov potravín", date: "2023-11-07", type: "abuse_of_dominance", sector: "retail", parties: JSON.stringify(["Lidl Slovenská republika v.o.s.", "Kaufland Slovensko v.o.s."]), summary: "PMÚ prešetril zmluvné podmienky uplatňované veľkými obchodnými reťazcami voči dodávateľom potravín z hľadiska súladu s pravidlami hospodárskej súťaže.", full_text: "PMÚ začal predbežné šetrenie praktík veľkých obchodných reťazcov voči slovenským dodávateľom potravín. Zisťovanie sa zameriavalo na: (1) jednostranné zmeny zmluvných podmienok bez primeranej lehoty; (2) vyžadovanie príspevkov na zaradenie do sortimentu bez hospodárskej opodstatnenosti; (3) odmietanie platieb v zmluvných lehotách. PMÚ nepreukázal kartelovú dohodu, ale vydal odporúčania na zlepšenie transparentnosti zmluvných podmienok v súlade s Obchodnou praktikou v potravinovom reťazci (OMNIBUS smernica).", outcome: "cleared_with_conditions", fine_amount: null, gwb_articles: JSON.stringify(["8", "10"]), status: "final" },
  { case_number: "2022/PM/4/1/003", title: "Energetický trh — Vyšetrovanie koordinovaného správania distribútorov elektriny", date: "2022-06-30", type: "sector_inquiry", sector: "energy", parties: JSON.stringify(["ZSE Distribúcia, a.s.", "SSE-D, a.s.", "VSE Distribúcia, a.s."]), summary: "PMÚ uskutočnil sektorové šetrenie distribútorov elektriny v troch regionálnych oblastiach a zistil štruktúrne bariéry efektívnej hospodárskej súťaže.", full_text: "PMÚ uskutočnil komplexné sektorové šetrenie trhu distribúcie elektriny. Distribučný trh má regionálnu štruktúru s prirodzenými monopolmi v každom regióne. PMÚ zistil: (1) nedostatočnú transparentnosť v metodike stanovovania distribučných sadzieb; (2) bariéry pre pripájanie malých výrobcov obnoviteľnej energie; (3) diskriminačné podmienky pre agregátorov flexibility. PMÚ vydal odporúčania pre Úrad pre reguláciu sieťových odvetví (ÚRSO) na posilnenie regulačného dohľadu a zlepšenie hospodárskej súťaže pri obnoviteľných zdrojoch.", outcome: "cleared", fine_amount: null, gwb_articles: JSON.stringify(["22a"]), status: "final" },
  { case_number: "2024/PM/1/1/002", title: "Komerčná banka — odmietnutie prístupu k platobnej infraštruktúre", date: "2024-01-18", type: "abuse_of_dominance", sector: "banking", parties: JSON.stringify(["Slovenská sporiteľňa, a.s."]), summary: "PMÚ prešetril obvinenia z odmietnutia prístupu fintech spoločnosti k bankovej platobnej infraštruktúre potrebnej pre poskytovanie platobných služieb.", full_text: "PMÚ začal konanie na základe podnetu fintechovej spoločnosti, ktorej Slovenská sporiteľňa odmietla poskytnúť prístup k bankovým účtom potrebným pre PSD2 služby. PMÚ preskúmal, či odmietnutie prístupu predstavuje zneužitie dominantného postavenia podľa § 8 ZOHS. Konanie skončilo prijatím záväzkov banky poskytnúť prístup za transparentných a nediskriminačných podmienok v súlade s PSD2 smernicou. Prípad naznačil potrebu aktualizácie sektorovej regulácie pre otvorené bankovníctvo.", outcome: "cleared_with_conditions", fine_amount: null, gwb_articles: JSON.stringify(["8", "PSD2"]), status: "final" },
];

const id = db.prepare("INSERT OR IGNORE INTO decisions (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
db.transaction(() => { for (const d of decisions) id.run(d.case_number, d.title, d.date, d.type, d.sector, d.parties, d.summary, d.full_text, d.outcome, d.fine_amount, d.gwb_articles, d.status); })();
console.log(`Inserted ${decisions.length} decisions`);

const mergers = [
  { case_number: "2023/PM/5/1/011", title: "Energetická skupina / Prevzatie distribútora zemného plynu", date: "2023-08-22", sector: "energy", acquiring_party: "SPP Infrastructure, a.s.", target: "Regionálna plynárenská spoločnosť, s.r.o.", summary: "PMÚ schválil koncentráciu v Fáze 1 bez podmienok po zistení, že prevzatie neprináša významné prekrytie na relevantnom trhu distribúcie zemného plynu.", full_text: "PMÚ posúdil concentráciu, pri ktorej SPP Infrastructure získava 100% podiel v Regionálna plynárenská spoločnosť. SPP Infrastructure je dominantný prevádzkovateľ distribučnej siete zemného plynu na Slovensku. Cieľová spoločnosť prevádzkuje lokálnu distribučnú sieť v dvoch obciach. Trh distribúcie zemného plynu má regionálny charakter s prirodzeným monopolom. PMÚ zistil, že strany neprekrývajú svoje distribučné územia a schválil concentráciu bez podmienok.", outcome: "cleared_phase1", turnover: 980_000_000 },
  { case_number: "2023/PM/5/1/007", title: "Bankové prevzatie / Fúzia poisťovní", date: "2023-05-10", sector: "banking", acquiring_party: "VÚB Banka, a.s.", target: "Generali Poisťovňa, a.s.", summary: "PMÚ schválil s podmienkami prevzatie poisťovne bankovým sektorom, požadujúc zabezpečenie prístupu konkurentov ku klientom cez bankové kanály.", full_text: "PMÚ posúdil concentráciu, pri ktorej VÚB Banka získava poisťovňu Generali. VÚB má rozsiahlu pobočkovú sieť po celom Slovensku. PMÚ identifikoval problémy v bancassurance segmente — kombinácia bankovej distribúcie s vlastnou poisťovňou môže vylúčiť konkurentov z distribúcie. PMÚ podmienil schválenie záväzkom VÚB nezaradovať do povinných balíkov pre úvery iba vlastné poisťovacie produkty a zabezpečiť nediskriminačný prístup pre alternatívnych poisťovateľov.", outcome: "cleared_with_conditions", turnover: 3_500_000_000 },
  { case_number: "2022/PM/5/1/019", title: "Maloobchodná skupina / Prevzatie regionálnej potravinovej siete", date: "2022-10-05", sector: "retail", acquiring_party: "COOP Jednota Slovensko", target: "Maloobchodná sieť TEMPO", summary: "PMÚ schválil v Fáze 1 prevzatie menšej regionálnej potravinovej siete, zisťujúc iba obmedzené regionálne prekrytie bez celoštátnych problémov.", full_text: "PMÚ posúdil concentráciu medzi COOP Jednota Slovensko a TEMPO maloobchodnou sieťou. COOP Jednota prevádzkuje viac ako 1 000 predajní po celom Slovensku, predovšetkým v menších obciach. TEMPO sieť má 47 predajní v Záhorí a okolí Trnavy. Analýza ukázala, že v relevantných lokálnych trhoch strany neprekrývajú svoje predajne nad prahom predpokladaných problémov. PMÚ schválil concentráciu bez podmienok v Fáze 1.", outcome: "cleared_phase1", turnover: 420_000_000 },
];

const im = db.prepare("INSERT OR IGNORE INTO mergers (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
db.transaction(() => { for (const m of mergers) im.run(m.case_number, m.title, m.date, m.sector, m.acquiring_party, m.target, m.summary, m.full_text, m.outcome, m.turnover); })();
console.log(`Inserted ${mergers.length} mergers`);

const dCount = (db.prepare("SELECT count(*) as cnt FROM decisions").get() as { cnt: number }).cnt;
const mCount = (db.prepare("SELECT count(*) as cnt FROM mergers").get() as { cnt: number }).cnt;
const sCount = (db.prepare("SELECT count(*) as cnt FROM sectors").get() as { cnt: number }).cnt;
console.log(`\nDatabase summary:\n  Sectors:   ${sCount}\n  Decisions: ${dCount}\n  Mergers:   ${mCount}\n\nDone. Database ready at ${DB_PATH}`);
db.close();
