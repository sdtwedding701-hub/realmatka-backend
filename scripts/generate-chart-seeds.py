from __future__ import annotations

import json
import re
import sys
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from xml.etree import ElementTree as ET


ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
MOCK_PATH = ROOT / "user apk" / "data" / "mock.ts"
FILE_SLUG_OVERRIDES = {
    "ANDHRA DAY MATKA PANEL RECORD 2021 - 2026.xlsx": "andhra-day",
    "ANDHRA MORNING MATKA PANEL RECORD.xlsx": "andhra-morning",
    "ANDHRA NIGHT MATKA PANEL RECORD.xlsx": "andhra-night",
    "KALYAN MATKA PANEL RECORD 2012 - 2026.xlsx": "kalyan",
    "KALYAN NIGHT MATKA JODI RECORD.xlsx": "kalyan-night",
    "karnataka day chart pannel.xlsx": "karnataka-day",
    "MADHUR DAY MATKA PANEL RECORD 2018 - 2026.xlsx": "madhur-day",
    "Madhur Night Matka Panel Chart.xlsx": "madhur-night",
    "MAHADEVI MORNING Panel Chart.xlsx": "mahadevi-morning",
    "MAHADEVI NIGHT Panel Chart.xlsx": "mahadevi-night",
    "MAHADEVI PANEL CHART.xlsx": "mahadevi",
    "MAIN BAZAR MATKA PANEL RECORD 2015 - 2026.xlsx": "main-bazar",
    "MANGAL BAZAR PANEL CHART.xlsx": "mangal-bazar",
    "Maya Bazar Panel Chart.xlsx": "maya-bazar",
    "MILAN DAY MATKA PANEL RECORD 2018 - 2026.xlsx": "milan-day",
    "MILAN MORNING MATKA PANEL RECORD 2019 - 2026.xlsx": "milan-morning",
    "MILAN NIGHT MATKA PANEL RECORD.xlsx": "milan-night",
    "NTR DAY PANEL CHART.xlsx": "ntr-day",
    "Ntr morning.xlsx": "ntr-morning",
    "NTR NIGHT PANEL CHART.xlsx": "ntr-night",
    "RAJDHANI DAY MATKA PANEL RECORD 2013 - 2026.xlsx": "rajdhani-day",
    "RAJDHANI NIGHT MATKA PANEL RECORD.xlsx": "rajdhani-night",
    "SITA DAY PANEL CHART.xlsx": "sita-day",
    "sita morning chart pannel.xlsx": "sita-morning",
    "SITA NIGHT PANEL CHART.xlsx": "sita-night",
    "SRIDEVI MATKA PANEL RECORD 2018 - 2026.xlsx": "sridevi",
    "SRIDEVI NIGHT MATKA PANEL RECORD.xlsx": "sridevi-night",
    "STAR TARA DAY PANEL CHART.xlsx": "star-tara-day",
    "STAR TARA MORNING PANEL CHART.xlsx": "star-tara-morning",
    "STAR TARA NIGHT PANEL CHART.xlsx": "star-tara-night",
    "Supreme Night Panel Chart.xlsx": "supreme-night",
    "TIME BAZAR MATKA PANEL RECORD 2017 - 2026.xlsx": "time-bazar",
}

NS_MAIN = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
NS_REL = {"rel": "http://schemas.openxmlformats.org/package/2006/relationships"}


def normalize_text(value: str) -> str:
    value = value.lower()
    value = re.sub(r"\.xlsx$", "", value)
    value = re.sub(r"\b(matka|panel|pannel|record|chart|charts|jodi)\b", " ", value)
    value = re.sub(r"\b(19|20)\d{2}\b", " ", value)
    value = re.sub(r"[^a-z0-9]+", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def load_markets() -> list[dict[str, str]]:
    raw = MOCK_PATH.read_text(encoding="utf-8")
    matches = re.findall(r'\{\s*slug:\s*"([^"]+)",\s*name:\s*"([^"]+)"', raw)
    return [{"slug": slug, "name": name, "normalized": normalize_text(name)} for slug, name in matches]


def resolve_slug(file_name: str, markets: list[dict[str, str]]) -> dict[str, str] | None:
    override_slug = FILE_SLUG_OVERRIDES.get(file_name)
    if override_slug:
        for market in markets:
            if market["slug"] == override_slug:
                return market
    needle = normalize_text(file_name)
    best: dict[str, str] | None = None
    best_len = -1
    for market in markets:
      normalized = market["normalized"]
      if normalized and (normalized in needle or needle in normalized):
          if len(normalized) > best_len:
              best = market
              best_len = len(normalized)
    return best


def col_to_index(ref: str) -> tuple[int, int]:
    match = re.match(r"([A-Z]+)(\d+)", ref)
    if not match:
        raise ValueError(f"Invalid cell reference: {ref}")
    col_letters, row_digits = match.groups()
    col = 0
    for char in col_letters:
        col = col * 26 + (ord(char) - 64)
    return int(row_digits), col


def load_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    try:
        root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    except KeyError:
        return []

    values: list[str] = []
    for item in root.findall("main:si", NS_MAIN):
        text = "".join(node.text or "" for node in item.findall(".//main:t", NS_MAIN))
        values.append(text)
    return values


def first_sheet_path(zf: zipfile.ZipFile) -> str:
    workbook = ET.fromstring(zf.read("xl/workbook.xml"))
    workbook_rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    rel_map = {
        rel.attrib["Id"]: rel.attrib["Target"]
        for rel in workbook_rels.findall("rel:Relationship", NS_REL)
    }
    first_sheet = workbook.find("main:sheets/main:sheet", NS_MAIN)
    if first_sheet is None:
        raise ValueError("Workbook has no sheets")
    rel_id = first_sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
    if not rel_id or rel_id not in rel_map:
        raise ValueError("Unable to resolve first worksheet")
    target = rel_map[rel_id].replace("\\", "/")
    if not target.startswith("xl/"):
        target = f"xl/{target}"
    return target


def load_cells(xlsx_path: Path) -> dict[int, dict[int, str]]:
    rows: dict[int, dict[int, str]] = {}
    with zipfile.ZipFile(xlsx_path) as zf:
        shared = load_shared_strings(zf)
        sheet_path = first_sheet_path(zf)
        sheet = ET.fromstring(zf.read(sheet_path))

        for row in sheet.findall("main:sheetData/main:row", NS_MAIN):
            row_index = int(row.attrib.get("r", "0"))
            cell_map: dict[int, str] = {}
            for cell in row.findall("main:c", NS_MAIN):
                ref = cell.attrib.get("r")
                if not ref:
                    continue
                _, col_index = col_to_index(ref)
                cell_type = cell.attrib.get("t")
                value = ""
                if cell_type == "inlineStr":
                    value = "".join(node.text or "" for node in cell.findall(".//main:t", NS_MAIN))
                else:
                    value_node = cell.find("main:v", NS_MAIN)
                    if value_node is not None and value_node.text is not None:
                        raw_value = value_node.text
                        if cell_type == "s":
                            try:
                                value = shared[int(raw_value)]
                            except Exception:
                                value = raw_value
                        else:
                            value = raw_value
                value = re.sub(r"\s+", " ", value).strip()
                if value:
                    cell_map[col_index] = value
            if cell_map:
                rows[row_index] = cell_map
    return rows


def format_jodi(value: str) -> str:
    cleaned = re.sub(r"\s+", "", value or "")
    if re.fullmatch(r"\d+", cleaned):
        return f"{int(cleaned):02d}"
    if cleaned in {"", "-", "--", "---"}:
        return "--"
    return cleaned[-2:].rjust(2, "0")


def format_panna_digit(value: str) -> str | None:
    cleaned = re.sub(r"\s+", "", value or "")
    return cleaned if re.fullmatch(r"\d", cleaned) else None


def format_panna(parts: list[str | None]) -> str:
    return "".join(parts) if all(part is not None for part in parts) else "---"


def excel_serial_to_date(value: str) -> datetime | None:
    cleaned = re.sub(r"\s+", "", value or "")
    if not re.fullmatch(r"\d+", cleaned):
        return None
    serial = int(cleaned)
    if serial < 20000:
        return None
    return datetime(1899, 12, 30) + timedelta(days=serial)


def build_week_label(start_value: str, end_value: str) -> str:
    start_date = excel_serial_to_date(start_value)
    end_date = excel_serial_to_date(end_value)
    if start_date and end_date:
        return f"{start_date:%Y} {start_date:%b %d} to {end_date:%b %d}"
    return re.sub(r"\s+", " ", f"{start_value} to {end_value}").strip()


def parse_chart_rows(cells: dict[int, dict[int, str]]) -> tuple[list[list[str]], list[list[str]]]:
    max_row = max(cells) if cells else 0
    jodi_rows: list[list[str]] = []
    panna_rows: list[list[str]] = []
    row = 1
    while row <= max_row - 2:
        start_value = re.sub(r"\s+", " ", cells.get(row, {}).get(1, "")).strip()
        bridge_value = re.sub(r"\s+", " ", cells.get(row + 1, {}).get(1, "")).strip().lower()
        end_value = re.sub(r"\s+", " ", cells.get(row + 2, {}).get(1, "")).strip()

        if not excel_serial_to_date(start_value) or bridge_value != "to" or not excel_serial_to_date(end_value):
            row += 1
            continue

        label = build_week_label(start_value, end_value)
        jodi_row = [label]
        panna_row = [label]
        for day_index in range(7):
            base_col = 2 + (day_index * 3)
            open_panna = format_panna([
                format_panna_digit(cells.get(row + 0, {}).get(base_col, "")),
                format_panna_digit(cells.get(row + 1, {}).get(base_col, "")),
                format_panna_digit(cells.get(row + 2, {}).get(base_col, "")),
            ])
            jodi = format_jodi(cells.get(row, {}).get(base_col + 1, ""))
            close_panna = format_panna([
                format_panna_digit(cells.get(row + 0, {}).get(base_col + 2, "")),
                format_panna_digit(cells.get(row + 1, {}).get(base_col + 2, "")),
                format_panna_digit(cells.get(row + 2, {}).get(base_col + 2, "")),
            ])
            jodi_row.append(jodi)
            panna_row.extend([open_panna, close_panna])

        jodi_rows.append(jodi_row)
        panna_rows.append(panna_row)
        row += 3

    return jodi_rows, panna_rows


def main() -> int:
    markets = load_markets()
    generated = []
    skipped = []

    for xlsx_path in sorted(DATA_DIR.glob("*.xlsx")):
        market = resolve_slug(xlsx_path.name, markets)
        if not market:
            skipped.append({"file": xlsx_path.name, "reason": "No market slug match"})
            continue

        try:
            cells = load_cells(xlsx_path)
            jodi_rows, panna_rows = parse_chart_rows(cells)
            if not jodi_rows or not panna_rows:
                skipped.append({"file": xlsx_path.name, "reason": "No chart rows parsed"})
                continue

            payload = {
                "slug": market["slug"],
                "source": xlsx_path.name,
                "jodi": jodi_rows,
                "panna": panna_rows,
            }
            output_path = DATA_DIR / f"{market['slug']}.chart.json"
            output_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
            generated.append({
                "slug": market["slug"],
                "source": xlsx_path.name,
                "output": output_path.name,
                "jodiRows": len(jodi_rows),
                "pannaRows": len(panna_rows),
            })
        except Exception as exc:
            skipped.append({"file": xlsx_path.name, "reason": str(exc)})

    print(json.dumps({"generated": generated, "skipped": skipped}, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
