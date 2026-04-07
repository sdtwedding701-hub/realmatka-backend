export const pannaChartBySingle = {
  "1": ["128", "137", "236", "678", "245", "290", "470", "579"],
  "2": ["129", "147", "246", "679", "345", "390", "480", "589"],
  "3": ["120", "157", "256", "670", "139", "148", "346", "689"],
  "4": ["130", "158", "356", "680", "239", "248", "347", "789"],
  "5": ["140", "159", "456", "690", "230", "258", "357", "780"],
  "6": ["123", "178", "268", "367", "240", "259", "457", "790"],
  "7": ["124", "179", "269", "467", "340", "359", "458", "890"],
  "8": ["125", "170", "260", "567", "134", "189", "369", "468"],
  "9": ["135", "180", "360", "568", "234", "289", "379", "478"],
  "0": ["145", "190", "460", "569", "235", "280", "370", "578"]
} as const;

export const sattaCardBySingle = {
  "1": ["128", "137", "146", "236", "245", "290", "380", "470", "489", "560", "678", "579", "119", "155", "227", "335", "344", "399", "588", "669", "777"],
  "2": ["129", "138", "147", "156", "237", "246", "345", "390", "480", "570", "679", "589", "110", "228", "255", "336", "499", "660", "688", "778", "444"],
  "3": ["120", "139", "148", "157", "238", "247", "256", "346", "490", "580", "670", "689", "166", "229", "337", "355", "445", "599", "779", "788", "111"],
  "4": ["130", "149", "158", "167", "239", "248", "257", "347", "356", "590", "680", "789", "112", "220", "266", "338", "446", "455", "699", "770", "888"],
  "5": ["140", "159", "168", "230", "249", "258", "267", "348", "357", "456", "690", "780", "113", "122", "177", "339", "366", "447", "799", "889", "555"],
  "6": ["123", "150", "169", "178", "240", "259", "268", "349", "358", "457", "367", "790", "114", "277", "330", "448", "466", "556", "880", "899", "222"],
  "7": ["124", "160", "179", "250", "269", "278", "340", "359", "368", "458", "467", "890", "115", "133", "188", "223", "377", "449", "557", "566", "999"],
  "8": ["125", "134", "170", "189", "260", "279", "350", "369", "378", "459", "567", "468", "116", "224", "233", "288", "440", "477", "558", "990", "666"],
  "9": ["126", "135", "180", "234", "270", "289", "360", "379", "450", "469", "117", "478", "568", "144", "199", "225", "388", "559", "577", "667", "333"],
  "0": ["127", "136", "145", "190", "235", "280", "370", "389", "460", "479", "569", "578", "118", "226", "244", "299", "334", "488", "668", "677", "000"]
} as const;

const pannaSingleEntries = Object.entries(pannaChartBySingle) as Array<[string, readonly string[]]>;
const sattaCardEntries = Object.entries(sattaCardBySingle) as Array<[string, readonly string[]]>;

export const allTriplePannas = ["000", "111", "222", "333", "444", "555", "666", "777", "888", "999"] as const;
export const allSinglePannas = [...new Set(pannaSingleEntries.flatMap(([, pannas]) => pannas))].sort();
export const allCardPannas = [...new Set(sattaCardEntries.flatMap(([, pannas]) => pannas))].sort();
export const allDoublePannas = allCardPannas.filter((panna) => !allSinglePannas.includes(panna) && !allTriplePannas.includes(panna as never));
export const allValidPannas = [...new Set([...allSinglePannas, ...allDoublePannas, ...allTriplePannas])].sort();

const pannaSingleLookup = new Map<string, string>();
for (const [single, pannas] of pannaSingleEntries) {
  for (const panna of pannas) {
    pannaSingleLookup.set(panna, single);
  }
}

const sattaCardLookup = new Map<string, string>();
for (const [single, pannas] of sattaCardEntries) {
  for (const panna of pannas) {
    sattaCardLookup.set(panna, single);
  }
}

export function getPannaSingleDigit(panna: string) {
  if (allTriplePannas.includes(panna as never)) {
    return panna[0] ?? null;
  }
  return pannaSingleLookup.get(panna) ?? null;
}

export function getSattaCardDigit(panna: string) {
  return sattaCardLookup.get(panna) ?? null;
}

export function getPannaType(panna: string) {
  if (allSinglePannas.includes(panna)) {
    return "single";
  }
  if (allDoublePannas.includes(panna)) {
    return "double";
  }
  if (allTriplePannas.includes(panna as never)) {
    return "triple";
  }
  return "unknown";
}

export function isValidPanna(panna: string) {
  return allValidPannas.includes(panna);
}

export const panelGroupDigits = Object.keys(sattaCardBySingle);
