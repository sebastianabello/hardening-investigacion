import csv, io, os, re
from typing import Optional, Dict, List, Tuple
from .progress import bus

# === Marcadores flexibles ===
MARK_T1 = re.compile(r"Control\s+Statistics", re.I)          # permite sufijos "(Percentage ...)"
MARK_T2 = re.compile(r'^\s*"?RESULTS"?\s*$', re.I)           # tolera comillas/espacios
MARK_ADJ = re.compile(r"\b(AJUSTADA|AJU)\b", re.I)
MARK_DC  = re.compile(r"DOMAIN\s+CONTROL+ER", re.I)           # CONTROLER/CONTROLLER

# Cortes de sección para no “arrastrar” líneas fuera de la tabla
SECTION_STOP_RE = re.compile(
    r'^\s*("?SUMMARY"?|"?ASSET\s+TAGS"?|ASSETS\b|POLICY\s+ID\b|CIS\s+Benchmark|roles\b|ubica\b)',
    re.I
)

# === Utils de metadata / normalización ===
def _detect_metadata(head_lines: List[str]) -> Dict[str, Optional[str]]:
    head = "\n".join(head_lines[:80])
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
    # Limpia comillas y espacios comunes en encabezados de Qualys
    return (s or "").strip().strip('"').strip()

def _norm_header(cols: List[str]) -> List[str]:
    return [_strip_cell(c) for c in cols]

def _ensure_cliente_last(header: List[str]) -> List[str]:
    # Garantiza exactamente una columna "Cliente" al final
    out = [h for h in header if h.strip().lower() != "cliente"]
    out.append("Cliente")
    return out

def _read_existing_header(path: str) -> Optional[List[str]]:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return None
    with open(path, "r", encoding="utf-8", newline="") as f:
        try:
            rdr = csv.reader(f)
            row = next(rdr)
            return _norm_header(row) if row else None
        except StopIteration:
            return None

# === Núcleo ===
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

    # Abrir writers y fijar header canónico si ya existe
    out_handles: Dict[str, Tuple[io.TextIOBase, csv.writer, Optional[List[str]]]] = {}
    for k, p in out_paths.items():
        first = not os.path.exists(p) or os.path.getsize(p) == 0
        # Si el archivo ya tiene contenido, leemos el header existente y lo reusamos
        existing_header = _read_existing_header(p)
        f = open(p, "a+", encoding="utf-8", newline="")
        w = csv.writer(f)
        out_handles[k] = (f, w, existing_header)  # cache del header canónico
        # No escribimos header aquí; lo haremos al primer ensure_header() si no existía

    def _bucket(is_t1: bool, adjusted: bool) -> str:
        if is_t1:
            return "t1_ajustada" if adjusted else "t1_normal"
        return "t2_ajustada" if adjusted else "t2_normal"

    def ensure_header(key: str, incoming_header: List[str]) -> List[str]:
        """
        Retorna el header canónico para el bucket.
        - Si ya existe: lo reutiliza.
        - Si no existe: toma el del primer bloque, lo normaliza y añade "Cliente" al final, lo escribe.
        """
        f, w, cached = out_handles[key]
        if cached is not None:
            return cached

        # No existía: normalizamos y fijamos header inicial
        norm_in = _norm_header(incoming_header)
        canon = _ensure_cliente_last(norm_in)
        w.writerow(canon)               # se escribe una única vez por archivo de salida
        out_handles[key] = (f, w, canon)
        return canon

    # Lee cabecera del archivo (para metadata) sin consumir todo el stream
    head_lines: List[str] = []
    with open(filepath, "r", encoding="utf-8-sig", errors="replace") as fh:
        for _ in range(80):
            pos = fh.tell()
            line = fh.readline()
            if not line:
                break
            head_lines.append(line)
            if len(head_lines) >= 80:
                break
        fh.seek(0)
        md = _detect_metadata(head_lines)

        # Estados
        IN_NONE, IN_T1_WAIT_HEADER, IN_T1_DATA, IN_T2_WAIT_HEADER, IN_T2_DATA = range(5)
        state = IN_NONE

        # Variables del bloque actual
        current_in_header: Optional[List[str]] = None   # header detectado en el bloque
        current_dest_header: Optional[List[str]] = None # header canónico del bucket
        current_bucket: Optional[str] = None
        t2_os_idx: Optional[int] = None

        # Función para mapear filas entrantes → header canónico (sin “Cliente”)
        def map_row_to_canon(row: List[str]) -> List[str]:
            assert current_in_header is not None and current_dest_header is not None
            # Mapa source
            src_names = [_strip_cell(c).lower() for c in current_in_header]
            src_map = {name: idx for idx, name in enumerate(src_names)}
            # Dest sin “Cliente”
            dest_cols = [c for c in current_dest_header if c.strip().lower() != "cliente"]
            out = []
            for col in dest_cols:
                idx = src_map.get(col.strip().lower())
                val = row[idx] if (idx is not None and idx < len(row)) else ""
                out.append(val)
            # Añade Cliente
            cliente = md["subcliente"] or md["cliente"] or cliente_por_defecto
            out.append(cliente)
            return out

        for raw in fh:
            line = raw.rstrip("\r\n")

            # Transiciones por marcadores (T1 por "search"; T2 por "match")
            if state in (IN_NONE, IN_T1_DATA, IN_T2_DATA):
                if MARK_T1.search(line):
                    state = IN_T1_WAIT_HEADER
                    current_in_header = None
                    current_dest_header = None
                    current_bucket = None
                    t2_os_idx = None
                    continue
                if MARK_T2.match(line):
                    state = IN_T2_WAIT_HEADER
                    current_in_header = None
                    current_dest_header = None
                    current_bucket = None
                    t2_os_idx = None
                    continue

            # Header de T1
            if state == IN_T1_WAIT_HEADER:
                try:
                    current_in_header = next(csv.reader([line]))
                except Exception:
                    current_in_header = [c.strip() for c in line.split(",")]
                current_bucket = _bucket(True, md["adjusted"])
                current_dest_header = ensure_header(current_bucket, current_in_header)
                state = IN_T1_DATA
                continue

            # Header de T2
            if state == IN_T2_WAIT_HEADER:
                try:
                    current_in_header = next(csv.reader([line]))
                except Exception:
                    current_in_header = [c.strip() for c in line.split(",")]
                # índice de operating system (case-insensitive) en header entrante
                t2_os_idx = None
                for idx, col in enumerate(current_in_header):
                    if _strip_cell(col).lower() == "operating system":
                        t2_os_idx = idx
                        break
                current_bucket = _bucket(False, md["adjusted"])
                current_dest_header = ensure_header(current_bucket, current_in_header)
                state = IN_T2_DATA
                continue

            # Filas de T1
            if state == IN_T1_DATA:
                # Corte si comienza otra sección o línea vacía
                if not line.strip() or SECTION_STOP_RE.search(line):
                    state = IN_NONE
                    continue
                row = next(csv.reader([line]))
                # Rellenar si viene más corta que su propio header entrante
                if current_in_header and len(row) < len(current_in_header):
                    row = row + [""] * (len(current_in_header) - len(row))
                # Alinear a header canónico y escribir
                f, w, _ = out_handles[current_bucket]  # type: ignore[arg-type]
                w.writerow(map_row_to_canon(row))
                counts["t1_ajustada" if md["adjusted"] else "t1_normal"] += 1
                continue

            # Filas de T2
            if state == IN_T2_DATA:
                if not line.strip() or SECTION_STOP_RE.search(line):
                    state = IN_NONE
                    continue
                row = next(csv.reader([line]))
                if current_in_header and len(row) < len(current_in_header):
                    row = row + [""] * (len(current_in_header) - len(row))
                # Ajuste de OS si aplica
                if t2_os_idx is not None and t2_os_idx < len(row):
                    row[t2_os_idx] = _norm_os_value(row[t2_os_idx], md["has_dc"])
                f, w, _ = out_handles[current_bucket]  # type: ignore[arg-type]
                w.writerow(map_row_to_canon(row))
                counts["t2_ajustada" if md["adjusted"] else "t2_normal"] += 1
                continue

    # Cierre
    for f, _, _ in out_handles.values():
        f.close()

    bus.push(session_id, "info",
             f"Procesado {os.path.basename(filepath)} "
             f"(T1N={counts['t1_normal']}, T1A={counts['t1_ajustada']}, "
             f"T2N={counts['t2_normal']}, T2A={counts['t2_ajustada']})")
    return counts
