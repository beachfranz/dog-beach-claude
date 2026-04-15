// scripts/research-beaches.mjs
// Uses Claude to research practical info for each beach.
// Run with: node scripts/research-beaches.mjs
//
// Set env var before running:
//   $env:ANTHROPIC_API_KEY="your_key"

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages";
const MODEL             = "claude-opus-4-5";

if (!ANTHROPIC_API_KEY) {
  console.error("Missing ANTHROPIC_API_KEY environment variable");
  process.exit(1);
}

const BEACHES = [
  { location_id: "huntington-dog-beach",  display_name: "Huntington Dog Beach",  address: "Pacific Coast Highway, Huntington Beach, CA" },
  { location_id: "coronado-dog-beach",    display_name: "Coronado Dog Beach",    address: "301 Ocean Blvd, Coronado, CA 92118" },
  { location_id: "del-mar-dog-beach",     display_name: "Del Mar Dog Beach",     address: "3902 29th St, Del Mar, CA 92014" },
  { location_id: "ocean-beach-dog-beach", display_name: "Ocean Beach Dog Beach", address: "5156 W Point Loma Blvd, San Diego, CA 92107" },
  { location_id: "rosies-dog-beach",      display_name: "Rosie's Dog Beach",     address: "4800 E Ocean Blvd, Long Beach, CA 90803" },
];

async function researchBeach(beach) {
  const prompt = `Research the following dog beach and return practical information for visitors.

Beach: ${beach.display_name}
Address: ${beach.address}

Based on your knowledge, provide accurate, concise information for each field. Write in plain text, 1-3 sentences each. Be specific and practical — this is what a local would tell a first-time visitor.

Return ONLY a JSON object with these exact keys:
{
  "parking_text": "Where to park, cost if any, tips for busy days",
  "leash_policy": "On-leash vs off-leash rules, any time/zone restrictions",
  "dog_rules": "Rules specific to dogs — tags, vaccinations, breed restrictions, ranger enforcement",
  "amenities": "Dog water stations, rinse stations, waste bag dispensers, shade, seating",
  "restrooms": "Restroom availability, location, seasonal notes"
}`;

  const res = await fetch(ANTHROPIC_API_URL, {
    method: "POST",
    headers: {
      "Content-Type":      "application/json",
      "x-api-key":         ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({
      model:      MODEL,
      max_tokens: 1024,
      messages:   [{ role: "user", content: prompt }],
    }),
  });

  if (!res.ok) throw new Error(`Anthropic error ${res.status}: ${await res.text()}`);

  const data = await res.json();
  const text = data.content?.[0]?.text ?? "";

  const match = text.match(/\{[\s\S]*\}/);
  if (!match) throw new Error(`No JSON found in response for ${beach.display_name}`);

  const info = JSON.parse(match[0]);
  return { location_id: beach.location_id, ...info };
}

console.log("Researching beaches...\n");

const results = [];

for (const beach of BEACHES) {
  process.stdout.write(`Researching ${beach.display_name}...`);
  try {
    const info = await researchBeach(beach);
    results.push(info);
    console.log(" done");
  } catch (err) {
    console.log(` FAILED: ${String(err)}`);
  }
}

console.log("\n" + "=".repeat(60));
console.log("REVIEW OUTPUT");
console.log("=".repeat(60));
console.log(JSON.stringify(results, null, 2));

console.log("\n" + "=".repeat(60));
console.log("SQL (apply after review)");
console.log("=".repeat(60));

for (const r of results) {
  const esc = (s) => s.replace(/'/g, "''");
  console.log(`
UPDATE public.beaches SET
  parking_text = '${esc(r.parking_text)}',
  leash_policy = '${esc(r.leash_policy)}',
  dog_rules    = '${esc(r.dog_rules)}',
  amenities    = '${esc(r.amenities)}',
  restrooms    = '${esc(r.restrooms)}'
WHERE location_id = '${r.location_id}';`);
}
