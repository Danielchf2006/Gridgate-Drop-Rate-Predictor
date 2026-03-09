import re
import csv
import json
import zipfile
from pathlib import Path
from decimal import Decimal

import pdfplumber

MISO_DIR = Path("miso_docs")
EXPANDED_DIR = Path("expanded_docs_auto")

DOC_OUT = "gi_master_extracted_data.csv"
PROJECT_OUT = "gi_projects_extracted_data.csv"

JSON_OUT_DIR = Path("extracted_json")

def safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def expand_all_zips(zip_roots: list[Path], out_dir: Path) -> None:
    """Expand all .zip found under zip_roots into out_dir/<zipname>/..."""
    safe_mkdir(out_dir)
    zips = []
    for root in zip_roots:
        if root.exists():
            zips.extend(list(root.rglob("*.zip")))
            zips.extend(list(root.rglob("*.ZIP")))

    for z in zips:
        dest = out_dir / z.stem
        if dest.exists():
            continue
        try:
            safe_mkdir(dest)
            with zipfile.ZipFile(z, "r") as zf:
                zf.extractall(dest)
        except Exception:
            continue


def iter_pdfs(roots: list[Path]) -> list[Path]:
    pdfs = []
    for root in roots:
        if not root.exists():
            continue
        pdfs.extend(list(root.rglob("*.pdf")))
        pdfs.extend(list(root.rglob("*.PDF")))

    seen = set()
    out = []
    for p in pdfs:
        s = str(p.resolve())
        if s not in seen:
            seen.add(s)
            out.append(p)
    return out


def extract_first_pages_text(pdf_path: Path, max_pages: int = 30) -> str:
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages[:max_pages]:
            text += (page.extract_text() or "") + "\n"
    return text


def classify_document(text: str) -> str:
    if re.search(r"SYSTEM\s+IMPACT\s+STUDY", text, re.I) or re.search(r"\bSIS\b", text, re.I):
        return "SIS"
    if re.search(r"FEASIBILITY\s+STUDY", text, re.I):
        return "Feasibility"
    if re.search(r"FACILITIES\s+STUDY", text, re.I):
        return "Facilities"
    return "Other"


def standardize_cycle(text: str) -> str:
    m = re.search(r"DPP[-\s]?(\d{4})", text, re.I)
    return f"DPP {m.group(1)}" if m else ""


def extract_phase(text: str) -> str:
    m = re.search(r"\bPhase\s*([1-3])\b", text, re.I)
    return f"Phase {m.group(1)}" if m else ""


def extract_issue_date(text: str) -> str:
    m = re.search(
        r"(Issue Date|Issued|Report Date)[:\s]*([0-9]{1,2}[\/\-][0-9]{1,2}[\/\-][0-9]{2,4})",
        text,
        re.I,
    )
    return m.group(2) if m else ""


def extract_in_service_timeline(text: str) -> str:
    m = re.search(r"\b\d{1,2}\s*[–\-]\s*\d{1,2}\s*months\b", text, re.I)
    return m.group(0) if m else ""


PROJECT_RE = re.compile(r"\bJ\d{4,5}\b")
MONEY_RE = re.compile(r"\$\s*[\d,]+(?:\.\d{2})?")


def normalize_project_id(raw: str) -> str:
    raw = raw.strip()
    if len(raw) == 6 and raw.startswith("J") and raw[1:].isdigit():
        return raw[:5]
    return raw


def extract_project_ids(text: str) -> str:
    ids = sorted(set(re.findall(r"\bJ\d{4,5}\b", text)))
    ids = [normalize_project_id(x) for x in ids]
    ids = sorted(set(ids))
    return ",".join(ids)


def money_to_int_dollars(m: str) -> int | None:
    s = m.replace("$", "").replace(",", "").strip()
    try:
        return int(Decimal(s).quantize(Decimal("1")))
    except Exception:
        return None


def extract_project_cost_table(pdf_path: Path, max_pages: int = 40) -> dict[str, int]:
    """
    Per-project upgrade cost extraction (text-based).
    """
    KEYWORDS = [
        "Executive Project and Upgrade Cost Summary",
        "Project and Upgrade Cost Summary",
        "Total Network Upgrade Cost",
        "Network Upgrade Cost",
        "Upgrade Cost Summary",
        "Total Network Upgrades",
        "Interconnection Requests Under Study",
        "Table 1",
        "Table 3",
    ]

    project_cost_map: dict[str, int] = {}

    try:
        with pdfplumber.open(pdf_path) as pdf:
            pages = pdf.pages[:max_pages]
            collecting = False

            for page in pages:
                text = page.extract_text() or ""
                if not text.strip():
                    continue

                if any(k.lower() in text.lower() for k in KEYWORDS):
                    collecting = True

                if not collecting:
                    continue

                for line in text.splitlines():
                    pid_m = PROJECT_RE.search(line)
                    money_m = MONEY_RE.search(line)
                    if not pid_m or not money_m:
                        continue

                    pid = normalize_project_id(pid_m.group(0))
                    cost = money_to_int_dollars(money_m.group(0))
                    if cost is None:
                        continue

                    project_cost_map[pid] = cost

        return project_cost_map
    except Exception:
        return {}


def sum_cost_map(cost_map: dict[str, int]) -> str:
    if not cost_map:
        return ""
    return f"${sum(cost_map.values()):,}"


TOC_LINE_RE = re.compile(
    r"^(?P<title>[A-Za-z][A-Za-z0-9 &/\-(),.]+?)\s+\.{2,}\s+(?P<page>\d{1,3})\s*$"
)

DEFAULT_SECTION_ORDER_PHASE3 = [
    "Executive Summary",
    "Project List",
    "Total Network Upgrades",
    "FERC Order Compliance",
    "Model Development and Study Assumptions",
    "Thermal Analysis",
    "Voltage Analysis",
    "Stability Analysis",
    "Short Circuit Analysis",
    "Affected System Impact Study",
    "Deliverability Analysis",
    "Shared Network Upgrades Analysis",
    "Cost Allocation",
]

DEFAULT_SECTION_ORDER_PHASE12 = [
    "Executive Summary",
    "Executive Project and Upgrade Cost Summary",
    "Steady State Thermal Analyses",
    "Appendices",
]


def find_toc_entries(pdf: pdfplumber.PDF, max_pages: int = 8) -> list[dict]:
    """
    Attempt to parse Table of Contents from early pages.
    Returns list of {title, toc_page}.
    """
    entries = []
    for i in range(min(max_pages, len(pdf.pages))):
        t = pdf.pages[i].extract_text() or ""
        if not t.strip():
            continue

        if "table of contents" not in t.lower() and "contents" not in t.lower():
            continue

        for line in t.splitlines():
            line = line.strip()
            m = TOC_LINE_RE.match(line)
            if m:
                title = m.group("title").strip()
                toc_page = int(m.group("page"))
                entries.append({"title": title, "toc_page": toc_page})

        if len(entries) >= 6:
            break

    seen = set()
    out = []
    for e in entries:
        key = e["title"].lower()
        if key not in seen:
            seen.add(key)
            out.append(e)
    return out


def find_pdf_page_containing_heading(pdf: pdfplumber.PDF, heading: str, search_pages: int = 25) -> int | None:
    """
    Find first PDF page index where heading appears (case-insensitive).
    Returns 0-based index or None.
    """
    target = heading.lower()
    for i in range(min(search_pages, len(pdf.pages))):
        t = (pdf.pages[i].extract_text() or "").lower()
        if target in t:
            return i
    return None


def build_sections_from_toc(pdf: pdfplumber.PDF) -> list[dict] | None:
    """
    Build sections [{title, start_idx, end_idx}] using ToC + offset calibration.
    Returns None if ToC isn't usable.
    """
    toc = find_toc_entries(pdf)
    if len(toc) < 3:
        return None

    offsets = []
    for e in toc[:6]:
        idx = find_pdf_page_containing_heading(pdf, e["title"], search_pages=35)
        if idx is not None:
            offsets.append(idx - (e["toc_page"] - 1))

    if not offsets:
        return None

    offsets.sort()
    offset = offsets[len(offsets) // 2]

    sections = []
    for i, e in enumerate(toc):
        start_idx = (e["toc_page"] - 1) + offset
        if start_idx < 0:
            continue

        end_idx = len(pdf.pages) - 1
        if i + 1 < len(toc):
            nxt = toc[i + 1]
            end_idx = (nxt["toc_page"] - 1) + offset - 1

        start_idx = max(0, min(start_idx, len(pdf.pages) - 1))
        end_idx = max(0, min(end_idx, len(pdf.pages) - 1))
        if end_idx < start_idx:
            continue

        sections.append({"title": e["title"], "start_idx": start_idx, "end_idx": end_idx})

    if len(sections) < 3:
        return None
    return sections


def fallback_sections_by_keywords(pdf: pdfplumber.PDF, phase: str) -> list[dict]:
    """
    If ToC parsing fails, fall back to scanning for known section headings in order.
    """
    order = DEFAULT_SECTION_ORDER_PHASE3 if phase == "Phase 3" else DEFAULT_SECTION_ORDER_PHASE12

    hits = []
    for title in order:
        idx = find_pdf_page_containing_heading(pdf, title, search_pages=60)
        if idx is not None:
            hits.append({"title": title, "start_idx": idx})

    hits.sort(key=lambda x: x["start_idx"])

    sections = []
    for i, h in enumerate(hits):
        start_idx = h["start_idx"]
        end_idx = len(pdf.pages) - 1
        if i + 1 < len(hits):
            end_idx = hits[i + 1]["start_idx"] - 1
        sections.append({"title": h["title"], "start_idx": start_idx, "end_idx": max(start_idx, end_idx)})

    return sections


def extract_section_text_and_tables(pdf: pdfplumber.PDF, start_idx: int, end_idx: int, max_pages: int = 25) -> tuple[str, list]:
    """
    Extract text + tables from a section page range.
    """
    text_parts = []
    tables_out = []
    count = 0

    for i in range(start_idx, min(end_idx + 1, len(pdf.pages))):
        if count >= max_pages:
            break
        page = pdf.pages[i]
        t = page.extract_text() or ""
        if t.strip():
            text_parts.append(t)

        try:
            tables = page.extract_tables() or []
            for tbl in tables:
                if tbl and len(tbl) >= 2:
                    tables_out.append({
                        "page_index": i,
                        "rows": tbl
                    })
        except Exception:
            pass

        count += 1

    return "\n".join(text_parts).strip(), tables_out


def write_sis_json(pdf_path: Path, meta: dict, phase: str) -> None:
    """
    Create a structured JSON dump for a SIS PDF: metadata + sections (text + tables).
    """
    safe_mkdir(JSON_OUT_DIR)
    out_path = JSON_OUT_DIR / (pdf_path.stem[:180] + ".json")

    try:
        with pdfplumber.open(pdf_path) as pdf:
            sections = build_sections_from_toc(pdf)
            if sections is None:
                sections = fallback_sections_by_keywords(pdf, phase)

            out_sections = []
            for s in sections:
                sec_text, sec_tables = extract_section_text_and_tables(pdf, s["start_idx"], s["end_idx"])
                out_sections.append({
                    "title": s["title"],
                    "pdf_page_start": s["start_idx"] + 1, 
                    "pdf_page_end": s["end_idx"] + 1,
                    "text": sec_text,
                    "tables": sec_tables,
                })

            payload = {
                "file": str(pdf_path),
                "metadata": meta,
                "sections": out_sections,
            }

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    except Exception:
        pass


def explode_documents_to_projects(doc_csv_path: str, out_csv_path: str) -> None:
    with open(doc_csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    fieldnames = [
        "project_id",
        "file",
        "document_type",
        "study_cycle",
        "phase",
        "issue_date",
        "project_upgrade_cost",
        "in_service_timeline",
    ]

    with open(out_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for r in rows:
            pids_raw = (r.get("project_ids") or "").strip()
            if not pids_raw:
                continue
            if r.get("document_type") != "SIS":
                continue

            try:
                cost_map = json.loads(r.get("project_cost_map_json") or "{}")
            except Exception:
                cost_map = {}

            pids = [p.strip() for p in pids_raw.split(",") if p.strip()]
            for pid in pids:
                cost_val = cost_map.get(pid)
                cost_str = f"${cost_val:,}" if isinstance(cost_val, int) else ""

                writer.writerow({
                    "project_id": pid,
                    "file": r.get("file", ""),
                    "document_type": r.get("document_type", ""),
                    "study_cycle": r.get("study_cycle", ""),
                    "phase": r.get("phase", ""),
                    "issue_date": r.get("issue_date", ""),
                    "project_upgrade_cost": cost_str,
                    "in_service_timeline": r.get("in_service_timeline", ""),
                })

    print(f"Wrote project-level CSV: {out_csv_path}")


def main():
    expand_all_zips([MISO_DIR], EXPANDED_DIR)

    pdf_files = iter_pdfs([MISO_DIR, EXPANDED_DIR])
    print(f"Found {len(pdf_files)} PDFs total (miso_docs + expanded_docs_auto)")

    rows = []
    for pdf_path in pdf_files:
        try:
            text = extract_first_pages_text(pdf_path, max_pages=30)
            if not text.strip():
                continue

            doc_type = classify_document(text)
            study_cycle = standardize_cycle(text)
            phase = extract_phase(text)
            issue_date = extract_issue_date(text)
            project_ids = extract_project_ids(text)
            timeline = extract_in_service_timeline(text)

            cost_map = extract_project_cost_table(pdf_path) if doc_type == "SIS" else {}

            if doc_type == "SIS":
                meta = {
                    "document_type": doc_type,
                    "study_cycle": study_cycle,
                    "phase": phase,
                    "issue_date": issue_date,
                }
                write_sis_json(pdf_path, meta, phase)

            rows.append({
                "file": str(pdf_path),
                "document_type": doc_type,
                "project_ids": project_ids,
                "study_cycle": study_cycle,
                "phase": phase,
                "issue_date": issue_date,
                "in_service_timeline": timeline,
                "project_cost_map_json": json.dumps(cost_map, ensure_ascii=False),
                "total_upgrade_cost": sum_cost_map(cost_map),
            })

        except Exception:
            continue

    with open(DOC_OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "file",
                "document_type",
                "project_ids",
                "study_cycle",
                "phase",
                "issue_date",
                "total_upgrade_cost",
                "in_service_timeline",
                "project_cost_map_json",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {DOC_OUT}")
    explode_documents_to_projects(DOC_OUT, PROJECT_OUT)
    print(f"Wrote JSON files to: {JSON_OUT_DIR.resolve()}")


if __name__ == "__main__":
    main()