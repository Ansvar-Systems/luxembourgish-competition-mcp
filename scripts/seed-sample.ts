/**
 * Seed the Conseil de la Concurrence du Luxembourg database with sample decisions, mergers, and sectors.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["CDLC_DB_PATH"] ?? "data/cdlc.db";
const force = process.argv.includes("--force");

const dir = dirname(DB_PATH);
if (!existsSync(dir)) { mkdirSync(dir, { recursive: true }); }
if (force && existsSync(DB_PATH)) { unlinkSync(DB_PATH); console.log(`Deleted existing database at ${DB_PATH}`); }

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);
console.log(`Database initialised at ${DB_PATH}`);

interface SectorRow { id: string; name: string; name_en: string; description: string; decision_count: number; merger_count: number; }

const sectors: SectorRow[] = [
  { id: "financial_services", name: "Services financiers", name_en: "Financial services",
    description: "Banques, assurances, gestion de patrimoine, marches financiers et infrastructure de marche au Luxembourg.", decision_count: 2, merger_count: 1 },
  { id: "telecommunications", name: "Telecommunications", name_en: "Telecommunications",
    description: "Services de telephonie mobile, haut debit, fixe et infrastructure de telecommunications au Luxembourg.", decision_count: 1, merger_count: 2 },
  { id: "energy", name: "Energie", name_en: "Energy",
    description: "Production, transport et distribution d electricite et de gaz au Luxembourg.", decision_count: 1, merger_count: 1 },
  { id: "retail", name: "Commerce de detail", name_en: "Retail",
    description: "Commerce de detail alimentaire et non-alimentaire, grandes surfaces et commerce electronique au Luxembourg.", decision_count: 2, merger_count: 1 },
  { id: "media", name: "Medias", name_en: "Media",
    description: "Radiodiffusion, presse ecrite, medias numeriques et services de streaming au Luxembourg.", decision_count: 1, merger_count: 0 },
  { id: "construction", name: "Construction", name_en: "Construction",
    description: "Materiaux de construction, services de batiment et promotion immobiliere au Luxembourg.", decision_count: 1, merger_count: 0 },
  { id: "healthcare", name: "Sante", name_en: "Healthcare",
    description: "Hopitaux, pharmacies, dispositifs medicaux et assurance maladie au Luxembourg.", decision_count: 1, merger_count: 1 },
  { id: "digital_economy", name: "Economie numerique", name_en: "Digital economy",
    description: "Plateformes en ligne, places de marche numeriques et services technologiques au Luxembourg.", decision_count: 1, merger_count: 0 },
];

const insertSector = db.prepare("INSERT OR IGNORE INTO sectors (id, name, name_en, description, decision_count, merger_count) VALUES (?, ?, ?, ?, ?, ?)");
for (const s of sectors) { insertSector.run(s.id, s.name, s.name_en, s.description, s.decision_count, s.merger_count); }
console.log(`Inserted ${sectors.length} sectors`);

interface DecisionRow { case_number: string; title: string; date: string; type: string; sector: string; parties: string; summary: string; full_text: string; outcome: string; fine_amount: number | null; gwb_articles: string; status: string; }

const decisions: DecisionRow[] = [
  {
    case_number: "CdlC/2022/001",
    title: "Secteur bancaire — echange d informations sur les taux d interet",
    date: "2022-05-10", type: "cartel", sector: "financial_services",
    parties: JSON.stringify(["Plusieurs etablissements bancaires luxembourgeois"]),
    summary: "Le Conseil de la Concurrence a mene une enquete sur un echange d informations entre banques luxembourgeoises concernant les taux d interet pratiques sur les credits aux particuliers. L enquete a etabli que cet echange facilitait une harmonisation des conditions de credit.",
    full_text: "Le Conseil de la Concurrence du Luxembourg a ouvert une enquete en vertu de l article 3 de la loi du 23 octobre 2011 relative a la concurrence et de l article 101 du TFUE concernant un echange d informations entre plusieurs etablissements bancaires luxembourgeois. L enquete portait sur des reunions regulieres et des echanges electroniques au sein d une association professionnelle au cours desquels les participants communiquaient des informations sur leurs taux d interet appliques aux credits immobiliers et aux credits a la consommation pour les particuliers. Le marche bancaire luxembourgeois est caracterise par une forte concentration, avec un nombre limite d etablissements de credit detenant des parts de marche significatives. Le Conseil a constate que cet echange d informations allait au-dela de ce qui est permis par le droit de la concurrence, dans la mesure ou il portait sur des donnees suffisamment recentes et detaillees pour permettre aux participants d aligner leurs comportements commerciaux. Les etablissements concernes ont pris des engagements afin de modifier leurs pratiques d echange d informations. Le Conseil a clos la procedure au vu de ces engagements et a publie des lignes directrices sur les pratiques d echange d informations permises dans le secteur bancaire luxembourgeois.",
    outcome: "cleared_with_conditions", fine_amount: null,
    gwb_articles: JSON.stringify(["Article 3 Loi 23 octobre 2011", "Article 101 TFUE"]), status: "final",
  },
  {
    case_number: "CdlC/2023/001",
    title: "Distribution de carburants — entente sur les prix de gros",
    date: "2023-02-28", type: "cartel", sector: "energy",
    parties: JSON.stringify(["Distributeurs de carburants au Luxembourg"]),
    summary: "Le Conseil de la Concurrence a sanctionne des distributeurs de carburants pour une entente sur les prix de gros du carburant au Luxembourg. L enquete a revele des contacts entre concurrents visant a coordonner les marges appliquees aux stations-service independantes.",
    full_text: "Le Conseil de la Concurrence a mene une enquete approfondie sur le marche de la distribution de carburants au Luxembourg. Suite a des inspections menees en vertu de l article 17 de la loi du 23 octobre 2011, le Conseil a decouvert des preuves de contacts anticoncurrentiels entre les principaux distributeurs de carburants. Ces contacts visaient a coordonner les marges de distribution appliquees aux stations-service independantes et a limiter la concurrence sur les prix de detail au Luxembourg. Le marche luxembourgeois de la distribution de carburants est oligopolistique, avec quelques grands distributeurs controlant la quasi-totalite de l approvisionnement. Les prix du carburant au Luxembourg sont traditionnellement inferieurs a ceux des pays voisins en raison de la fiscalite, ce qui entraine un tourisme a la pompe significatif en provenance de Belgique, France et Allemagne. Les elements de preuve rassembles comprenaient des echanges de courriels, des comptes rendus de reunions et des donnees de tarification montrant une convergence systematique des marges de gros. Le Conseil a inflige des amendes aux entreprises impliquees et a ordonne la cessation de ces pratiques. Il a egalement emis des recommandations a l attention des associations professionnelles concernant les echanges d informations licites.",
    outcome: "fine", fine_amount: 3_500_000,
    gwb_articles: JSON.stringify(["Article 3 Loi 23 octobre 2011", "Article 101 TFUE"]), status: "final",
  },
  {
    case_number: "CdlC/2023/002",
    title: "Grande distribution — pratiques d achat abusives envers fournisseurs",
    date: "2023-08-15", type: "abuse_of_dominance", sector: "retail",
    parties: JSON.stringify(["Grande surface alimentaire luxembourgeoise"]),
    summary: "Le Conseil de la Concurrence a examine les pratiques d achat d une grande surface alimentaire envers ses fournisseurs luxembourgeois, notamment des delais de paiement excessifs et des exigences de contribution aux frais promotionnels non prevues aux contrats.",
    full_text: "Le Conseil de la Concurrence a ouvert une enquete suite a des plaintes de fournisseurs luxembourgeois contre une grande surface alimentaire. L enquete portait sur des pratiques d achat potentiellement abusives au sens de l article 4 de la loi du 23 octobre 2011. Pratiques examinces: (1) Delais de paiement systematiquement superieurs aux 30 jours prevus par la loi pour les produits alimentaires. (2) Contributions financieres exigees des fournisseurs pour figurer dans les catalogues promotionnels, non prevues dans les contrats-cadres initiaux. (3) Retours de marchandises invendues a la charge des fournisseurs dans des conditions contractuellement non prevues. (4) Modifications unilaterales des conditions d achat en cours de periode contractuelle. Le Conseil a constate que ces pratiques constituaient des abus dans les relations commerciales et causaient un prejudice aux fournisseurs qui ne disposent pas de puissance de marche equivalente. La grande surface a accepte de modifier ses pratiques contractuelles et de regulariser les situations individuelles avec les fournisseurs plaignants. Le Conseil a publie des lignes directrices sur les pratiques d achat loyales dans le commerce de detail luxembourgeois.",
    outcome: "cleared_with_conditions", fine_amount: null,
    gwb_articles: JSON.stringify(["Article 4 Loi 23 octobre 2011"]), status: "final",
  },
  {
    case_number: "CdlC/2022/002",
    title: "Secteur de la construction — coordination des offres dans les marches publics",
    date: "2022-09-20", type: "cartel", sector: "construction",
    parties: JSON.stringify(["Entreprises de construction au Luxembourg"]),
    summary: "Le Conseil de la Concurrence a sanctionne plusieurs entreprises de construction pour coordination des offres (bid rigging) dans des marches publics au Luxembourg. Les entreprises se repartissaient les appels d offres et soumettaient des offres de couverture.",
    full_text: "Le Conseil de la Concurrence a clos une enquete menee depuis 2020 sur la coordination des offres dans des marches publics de construction au Luxembourg. L enquete, declenchee suite a un signalement d une entite adjudicatrice publique, a revele un systeme etabli de repartition des marches entre un groupe d entreprises de construction. Le mecanisme impliquait: (1) Attribution informelle des marches au sein du groupe — les entreprises s accordaient avant le depot des offres sur qui remporterait quel marche. (2) Soumission d offres de couverture — les autres entreprises du groupe soumettaient des offres deliberement non competitives pour simuler une concurrence reelle. (3) Rotation des victoires — le systeme assurait que chaque membre du groupe recevait sa part des marches sur la duree. Les secteurs concernes incluaient la construction routiere, les travaux de voirie, et les batiments publics. Le prejudice pour les entites publiques adjudicatrices a ete evalue a plusieurs millions d euros de surcoets. Des amendes significatives ont ete infligees. Les dirigeants des entreprises impliquees ont ete signales a la justice pour poursuite penale, le droit luxembourgeois prevoyant des sanctions penales pour les ententes les plus graves.",
    outcome: "fine", fine_amount: 5_200_000,
    gwb_articles: JSON.stringify(["Article 3 Loi 23 octobre 2011", "Article 101 TFUE"]), status: "final",
  },
  {
    case_number: "CdlC/2024/001",
    title: "Secteur numerique — etude de marche plateformes en ligne",
    date: "2024-02-01", type: "sector_inquiry", sector: "digital_economy",
    parties: JSON.stringify(["Operateurs de plateformes numeriques au Luxembourg"]),
    summary: "Le Conseil de la Concurrence a lance une etude de marche sur les plateformes numeriques operant au Luxembourg, avec un focus sur les places de marche en ligne, les services de reservation, et les agregateurs de prix. L etude analyse la dynamique concurrentielle et l impact du Reglement sur les marches numeriques (DMA).",
    full_text: "Le Conseil de la Concurrence a initie une etude de marche en vertu de l article 11 de la loi du 23 octobre 2011 sur les plateformes numeriques operant au Luxembourg. Luxembourg occupe une position particuliere dans l ecosysteme numerique europeen, etant le siege europeen de nombreuses plateformes mondiales notamment dans le domaine du streaming, du commerce electronique, et des services financiers numeriques. L etude se concentre sur: (1) Les places de marche en ligne — structure des commissions, conditions d acces pour les vendeurs, et utilisation des donnees generees par les vendeurs. (2) Les services de reservation en ligne — hotels, voyages, et services locaux au Luxembourg. (3) Les agregateurs de prix — comparateurs d assurances, d energie, et de services financiers. (4) L application du Reglement sur les marches numeriques (DMA) — le Conseil coopere avec la Direction generale de la concurrence de la Commission europeenne pour assurer la coherence de l application au Luxembourg. L etude est prevue pour une duree de 12 mois et ses conclusions alimenteront les priorites d application du Conseil pour 2025.",
    outcome: "cleared", fine_amount: null,
    gwb_articles: JSON.stringify(["Article 11 Loi 23 octobre 2011", "DMA"]), status: "ongoing",
  },
];

const insertDecision = db.prepare("INSERT OR IGNORE INTO decisions (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
const insertDecisionsAll = db.transaction(() => { for (const d of decisions) { insertDecision.run(d.case_number, d.title, d.date, d.type, d.sector, d.parties, d.summary, d.full_text, d.outcome, d.fine_amount, d.gwb_articles, d.status); } });
insertDecisionsAll();
console.log(`Inserted ${decisions.length} decisions`);

interface MergerRow { case_number: string; title: string; date: string; sector: string; acquiring_party: string; target: string; summary: string; full_text: string; outcome: string; turnover: number | null; }

const mergers: MergerRow[] = [
  {
    case_number: "CdlC/2022/M/001",
    title: "Post Luxembourg / DHL Luxembourg — Services de livraison",
    date: "2022-06-30", sector: "telecommunications",
    acquiring_party: "POST Luxembourg", target: "DHL Luxembourg (reseau de livraison du dernier kilometre)",
    summary: "Le Conseil de la Concurrence a approuve l acquisition par POST Luxembourg d actifs du reseau de livraison du dernier kilometre de DHL au Luxembourg. La transaction a ete autorisee en Phase 1 apres constat que la concurrence subsiste dans le secteur des services de livraison.",
    full_text: "Le Conseil de la Concurrence a examine la proposition d acquisition par POST Luxembourg S.A. d une partie des actifs logistiques de DHL Luxembourg en matiere de livraison du dernier kilometre. POST Luxembourg est l operateur postal historique au Luxembourg et operateur de services de courrier et de colis. La transaction porte sur l infrastructure de livraison de DHL pour les colis adresses aux particuliers et aux petites entreprises au Luxembourg. Le Conseil a evalue la transaction sur les marches de la livraison de colis aux consommateurs (B2C) et aux entreprises (B2B) au Luxembourg. Constats principaux: (1) Le marche de la livraison de colis au Luxembourg comprend POST Luxembourg, DHL, bpost Belgium, Chronopost, UPS, FedEx, et de nouveaux entrants tels que les livraisons par des plateformes de commerce electronique. (2) La transaction renforce la position de POST Luxembourg dans la livraison B2C, mais n elimine pas de concurrent direct du marche. (3) Des alternatives de livraison restent disponibles notamment via des casiers automatiques et des points relais. Le Conseil a autorise la transaction en Phase 1 sans conditions, concluant qu elle ne porterait pas une atteinte significative a la concurrence.",
    outcome: "cleared_phase1", turnover: 600_000_000,
  },
  {
    case_number: "CdlC/2023/M/001",
    title: "Luxair / Tango — Fusion dans le secteur des telecommunications",
    date: "2023-04-15", sector: "telecommunications",
    acquiring_party: "Telenet Group (via filiale)", target: "Tango S.A.",
    summary: "Le Conseil de la Concurrence a approuve avec conditions l acquisition de Tango S.A. par Telenet. La transaction a ete autorisee en Phase 2 avec des engagements de cession de capacite de reseau pour preserver la concurrence dans le marche mobile luxembourgeois.",
    full_text: "Le Conseil de la Concurrence a conduit une enquete approfondie sur l acquisition proposee de Tango S.A. par Telenet Group Holding N.V. Tango est le troisieme operateur de telecommunications mobile au Luxembourg. Telenet, filiale de Liberty Global, est un operateur de cable et telecommunications en Belgique ayant des ambitions d expansion au Grand-Duche. Le marche luxembourgeois des communications electroniques est domine par POST Telecom (operateur historique), Orange Luxembourg, et Tango. L acquisition aurait reduit le nombre d operateurs mobiles de trois a deux (POST et Orange). Le Conseil a ouvert une enquete de Phase 2 en raison de preoccupations concurrentielles significatives: (1) Reduction du nombre d operateurs sur le marche mobile luxembourgeois. (2) Hausse probable des prix pour les consommateurs residentiels et les entreprises. (3) Risque de deterioration de la qualite et de la couverture reseau. A l issue de la Phase 2, le Conseil a autorise la transaction sous conditions, notamment l obligation pour Telenet/Tango de ceder de la capacite de reseau a un ou plusieurs operateurs de reseau mobile virtuel (MVNO) pendant une periode de 7 ans.",
    outcome: "cleared_with_conditions", turnover: 1_200_000_000,
  },
  {
    case_number: "CdlC/2022/M/002",
    title: "Groupe Foyer / CML — Fusion dans le secteur de l assurance",
    date: "2022-11-08", sector: "financial_services",
    acquiring_party: "Groupe Foyer S.A.", target: "Compagnie Luxembourgeoise d Assurances (CML)",
    summary: "Le Conseil de la Concurrence a approuve l acquisition de CML par Groupe Foyer, le plus grand groupe d assurance luxembourgeois. La Phase 1 a confirme que la transaction ne cree pas de preoccupations concurrentielles dans les marches d assurance vie et non-vie au Luxembourg.",
    full_text: "Le Conseil de la Concurrence a examine l acquisition proposee de la Compagnie Luxembourgeoise d Assurances (CML) par le Groupe Foyer S.A. Groupe Foyer est le principal groupe d assurance luxembourgeois, present dans l assurance vie, l assurance non-vie, et la gestion de patrimoine. CML est un operateur plus modeste proposant principalement des produits d assurance non-vie pour les particuliers et les PME. Le Conseil a defini les marches pertinents comme etant: (1) Assurance vie au Luxembourg — marche caracterise par la presence de nombreux assureurs europeens, notamment belges et francais, offrant des produits d epargne avec avantages fiscaux luxembourgeois. (2) Assurance non-vie particuliers — y compris assurance automobile, habitation, et responsabilite civile. (3) Assurance non-vie entreprises — y compris assurance professionnelle et couverture des risques industriels. Sur chacun de ces marches, Groupe Foyer et CML detiennent des parts de marche combinces qui ne suscitent pas de preoccupations au regard du critere d atteinte significative a la concurrence, compte tenu de la presence de nombreux operateurs europeens. Le Conseil a delivre une autorisation en Phase 1.",
    outcome: "cleared_phase1", turnover: 1_800_000_000,
  },
];

const insertMerger = db.prepare("INSERT OR IGNORE INTO mergers (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
const insertMergersAll = db.transaction(() => { for (const m of mergers) { insertMerger.run(m.case_number, m.title, m.date, m.sector, m.acquiring_party, m.target, m.summary, m.full_text, m.outcome, m.turnover); } });
insertMergersAll();
console.log(`Inserted ${mergers.length} mergers`);

const decisionCount = (db.prepare("SELECT count(*) as cnt FROM decisions").get() as { cnt: number }).cnt;
const mergerCount = (db.prepare("SELECT count(*) as cnt FROM mergers").get() as { cnt: number }).cnt;
const sectorCount = (db.prepare("SELECT count(*) as cnt FROM sectors").get() as { cnt: number }).cnt;
console.log("\nDatabase summary:");
console.log(`  Sectors:    ${sectorCount}`);
console.log(`  Decisions:  ${decisionCount}`);
console.log(`  Mergers:    ${mergerCount}`);
console.log(`\nDone. Database ready at ${DB_PATH}`);
db.close();
