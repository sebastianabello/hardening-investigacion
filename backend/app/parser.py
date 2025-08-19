import csv, io, os, re
from typing import Optional, Dict, List, Tuple
from .progress import bus

# === Marcadores flexibles ===
MARK_T1 = re.compile(r"Control\s+Statistics", re.I)          # permite sufijos "(Percentage ...)"
MARK_T2 = re.compile(r'^\s*"?RESULTS"?\s*$', re.I)           # tolera comillas/espacios
MARK_ADJ = re.compile(r"\b(AJUSTADA|AJU)\b", re.I)
MARK_DC  = re.compile(r"DOMAIN\s+CONTROL+ER", re.I)           # CONTROLER/CONTROLLER (tolerante)

# Cortes de sección (líneas que indican fin de la tabla en curso)
SECTION_STOP_RE = re.compile(
    r'^\s*('
    r'"?SUMMARY"?|"?ASSET\s+TAGS"?|ASSETS\b|POLICY\s+ID\b|'
    r'CIS\s+Benchmark|roles\b|ubica\b|'
    r'HOST\s+STATISTICS(?:\s*\(.*\))?'   # Host Statistics (...) → fuera de T1/T2
    r')',
    re.I
)

# Ruido típico de logs/errores que a veces aparecen tras RESULTS
STOP_LOG_RE = re.compile(
    r'^\s*(ERROR|ERR|WARN|WARNING|INFO|DEBUG|TRACE|EXCEPTION|TRACEBACK|Caused by:|at\s+\S+\.|java\.|org\.)',
    re.I
)

def _detect_metadata(head_lines: List[str]) -> Dict[str, Optional[str]]:
    head = "\n".join(head_lines[:200])
    adjusted = bool(MARK_ADJ.search(head))
    has_dc   = bool(MARK_DC.search(head))
    cliente = None
    subcliente = None
    m_cli = re.search(r"(?:Cliente|Client|Customer)\s*[:\-]\s*(.+)", head, re.I)
    if m_cli: cliente = m_cli.group(1).strip()
    m_sub = re.search(r"(?:Subcliente|Subclient)\s*[:\-]\s*(.+)", head, re.I)
    if m_sub: subcliente = m_sub.group(1).strip()
    return {"adjusted": adjusted, "has_dc": has_dc, "cliente": cliente, "subcliente": subcliente}

def _norm_os_value(val: str, has_dc_flag: bool) -> str:
    if not has_dc_flag: return val
    vlow = (val or "").lower()
    if "domain controller" in vlow:
        return val
    return (val or "") + " domain controller"

def _strip_cell(s: str) -> str:
    return (s or "").strip().strip('"').strip()

def _norm_header(cols: List[str]) -> List[str]:
    return [_strip_cell(c) for c in cols]

def _ensure_cliente_last(header: List[str]) -> List[str]:
    out = [h for h in header if h.strip().lower() != "cliente"]
    out.append("Cliente")
    return out

def _read_existing_header(path: str) -> Optional[List[str]]:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return None
    with open(path, "r", encoding="utf-8", newline="") as f:
        rdr = csv.reader(f)
        try:
            row = next(rdr)
        except StopIteration:
            return None
        return _norm_header(row) if row else None

class TableIterator:
    """
    Iterador que lee líneas desde fh y se detiene ANTES de:
      - línea vacía,
      - SECTION_STOP_RE,
      - STOP_LOG_RE,
      - aparición de otro marcador de tabla (T1/T2).
    Reposiciona fh para que la línea de stop se procese por el bucle externo.
    """
    def __init__(self, fh, stop_predicate):
        self.fh = fh
        self.stop_predicate = stop_predicate
        self._done = False

    def __iter__(self):
        return self

    def __next__(self):
        if self._done:
            raise StopIteration
        pos = self.fh.tell()
        line = self.fh.readline()
        if not line:
            self._done = True
            raise StopIteration
        if self.stop_predicate(line):
            self.fh.seek(pos)
            self._done = True
            raise StopIteration
        return line

def _t2_header_is_valid(cols: List[str]) -> bool:
    # Requisitos mínimos para considerar que "RESULTS" realmente inicia T2
    must_have_any = {"host ip", "operating system", "control id", "status"}
    norm = { _strip_cell(c).lower() for c in cols }
    return len(must_have_any & norm) > 0

def parse_report_file(
    filepath: str,
    outputs_dir: str,
    cliente_por_defecto: str,
    session_id: str,
) -> Dict[str, int]:

    counts = {"t1_normal":0, "t1_ajustada":0, "t2_normal":0, "t2_ajustada":0}
    out_paths = {
        "t1_normal":   os.path.join(outputs_dir, "t1_normal.csv"),
        "t1_ajustada": os.path.join(outputs_dir, "t1_ajustada.csv"),
        "t2_normal":   os.path.join(outputs_dir, "t2_normal.csv"),
        "t2_ajustada": os.path.join(outputs_dir, "t2_ajustada.csv"),
    }

    # Abrimos salidas y precargamos header canónico si ya existe
    out_handles: Dict[str, Tuple[io.TextIOBase, csv.writer, Optional[List[str]]]] = {}
    for k, p in out_paths.items():
        existing = _read_existing_header(p)
        f = open(p, "a+", encoding="utf-8", newline="")
        w = csv.writer(f)
        out_handles[k] = (f, w, existing)  # cache del header canónico (o None)

    def _bucket(is_t1: bool, adjusted: bool) -> str:
        return ("t1_" if is_t1 else "t2_") + ("ajustada" if adjusted else "normal")

    def ensure_header(key: str, incoming_header: List[str]) -> List[str]:
        f, w, cached = out_handles[key]
        if cached is not None:
            return cached
        canon = _ensure_cliente_last(_norm_header(incoming_header))
        w.writerow(canon)          # solo una vez por archivo
        out_handles[key] = (f, w, canon)
        return canon

    def make_row_mapper(in_header: List[str], canon_header: List[str], cliente_val: str):
        src_names = [_strip_cell(c).lower() for c in in_header]
        src_map = {name: idx for idx, name in enumerate(src_names)}
        dest_cols = [c for c in canon_header if c.strip().lower() != "cliente"]

        def map_row(row: List[str]) -> List[str]:
            out = []
            for col in dest_cols:
                idx = src_map.get(col.strip().lower())
                val = row[idx] if (idx is not None and idx < len(row)) else ""
                out.append(val)
            out.append(cliente_val)
            return out
        return map_row

    with open(filepath, "r", encoding="utf-8-sig", errors="replace") as fh:
        # Leer primeras N líneas para metadata
        head_lines: List[str] = []
        for _ in range(200):
            pos = fh.tell()
            line = fh.readline()
            if not line: break
            head_lines.append(line)
        fh.seek(0)
        md = _detect_metadata(head_lines)
        cliente_val = md["subcliente"] or md["cliente"] or cliente_por_defecto

        # Bucle principal con control explícito del file pointer
        while True:
            pos = fh.tell()
            line = fh.readline()
            if not line:
                break
            stripped = line.rstrip("\r\n")

            # Inicio de T1
            if MARK_T1.search(stripped):
                # Header en la siguiente línea
                header_line = fh.readline()
                if not header_line:
                    continue
                try:
                    in_header = next(csv.reader([header_line]))
                except Exception:
                    in_header = [c.strip() for c in header_line.split(",")]
                bucket = _bucket(True, md["adjusted"])
                canon = ensure_header(bucket, in_header)
                map_row = make_row_mapper(in_header, canon, cliente_val)

                def stop_pred(l: str) -> bool:
                    if not l.strip(): return True
                    if SECTION_STOP_RE.search(l): return True
                    if STOP_LOG_RE.search(l): return True
                    if MARK_T1.search(l) or MARK_T2.match(l): return True
                    return False

                rdr = csv.reader(TableIterator(fh, stop_pred))
                f, w, _ = out_handles[bucket]
                bad = 0
                for row in rdr:
                    if in_header and len(row) != len(in_header):
                        bad += 1
                        if bad >= 3:
                            break
                        continue
                    bad = 0
                    w.writerow(map_row(row))
                    counts["t1_ajustada" if md["adjusted"] else "t1_normal"] += 1
                continue

            # Inicio de T2
            if MARK_T2.match(stripped):
                # Header en la siguiente línea
                header_line = fh.readline()
                if not header_line:
                    continue
                try:
                    in_header = next(csv.reader([header_line]))
                except Exception:
                    in_header = [c.strip() for c in header_line.split(",")]

                # Validación mínima del header de T2
                if not _t2_header_is_valid(in_header):
                    bus.push(session_id, "warning", "Encabezado T2 inválido tras RESULTS; bloque ignorado")
                    # saltamos este bloque: seguimos el while principal
                    continue

                # Índice de Operating System (case-ins)
                os_idx = None
                for idx, col in enumerate(in_header):
                    if _strip_cell(col).lower() == "operating system":
                        os_idx = idx
                        break

                bucket = _bucket(False, md["adjusted"])
                canon = ensure_header(bucket, in_header)
                map_row = make_row_mapper(in_header, canon, cliente_val)

                def stop_pred(l: str) -> bool:
                    if not l.strip(): return True
                    if SECTION_STOP_RE.search(l): return True
                    if STOP_LOG_RE.search(l): return True
                    if MARK_T1.search(l) or MARK_T2.match(l): return True
                    return False

                rdr = csv.reader(TableIterator(fh, stop_pred))
                f, w, _ = out_handles[bucket]
                bad = 0
                for row in rdr:
                    if in_header and len(row) != len(in_header):
                        bad += 1
                        if bad >= 3:
                            break
                        continue
                    bad = 0
                    if os_idx is not None and os_idx < len(row):
                        row[os_idx] = _norm_os_value(row[os_idx], md["has_dc"])
                    w.writerow(map_row(row))
                    counts["t2_ajustada" if md["adjusted"] else "t2_normal"] += 1
                continue

            # Si no es inicio de ninguna tabla, seguimos
            continue

    # Cerrar archivos
    for f, _, _ in out_handles.values():
        f.close()

    bus.push(session_id, "info",
             f"Procesado {os.path.basename(filepath)} "
             f"(T1N={counts['t1_normal']}, T1A={counts['t1_ajustada']}, "
             f"T2N={counts['t2_normal']}, T2A={counts['t2_ajustada']})")
    return counts
