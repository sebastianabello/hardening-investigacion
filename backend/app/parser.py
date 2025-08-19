import csv, io, os, re
from typing import Optional, Dict, List, Tuple
from .progress import bus

MARK_T1 = re.compile(r"^\s*Control\s+Statistics\s*$", re.I)
MARK_T2 = re.compile(r"^\s*RESULTS\s*$", re.I)
MARK_ADJ = re.compile(r"\b(AJUSTADA|AJU)\b", re.I)
MARK_DC  = re.compile(r"DOMAIN\s+CONTROL+ER", re.I)  # matches CONTROLER / CONTROLLER

def _detect_metadata(head_lines: List[str]) -> Dict[str, Optional[str]]:
    head = "\n".join(head_lines[:50])
    adjusted = bool(MARK_ADJ.search(head))
    has_dc   = bool(MARK_DC.search(head))
    # cliente/subcliente heurísticos
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

def parse_report_file(
    filepath: str,
    outputs_dir: str,
    cliente_por_defecto: str,
    session_id: str,
) -> Dict[str, int]:
    """
    Lee un CSV gigante de Qualys en streaming y escribe filas en 4 archivos:
    t1_normal.csv, t1_ajustada.csv, t2_normal.csv, t2_ajustada.csv
    Devuelve contadores por salida.
    """
    counts = {"t1_normal":0, "t1_ajustada":0, "t2_normal":0, "t2_ajustada":0}
    # Abrir writers en modo append; escribimos encabezado en la primera vez que no exista
    out_paths = {
        "t1_normal":   os.path.join(outputs_dir, "t1_normal.csv"),
        "t1_ajustada": os.path.join(outputs_dir, "t1_ajustada.csv"),
        "t2_normal":   os.path.join(outputs_dir, "t2_normal.csv"),
        "t2_ajustada": os.path.join(outputs_dir, "t2_ajustada.csv"),
    }
    out_handles: Dict[str, Tuple[io.TextIOBase, csv.writer, Optional[List[str]]]] = {}
    for k, p in out_paths.items():
        first = not os.path.exists(p)
        f = open(p, "a+", encoding="utf-8", newline="")
        w = csv.writer(f)
        out_handles[k] = (f, w, None)  # header cache
        if first:
            # header aún no conocido; se escribirá cuando detectemos el de la tabla
            pass

    def ensure_header(key: str, header: List[str]):
        # Asegura columna "Cliente"
        if not any(h.strip().lower() == "cliente" for h in header):
            header = header + ["Cliente"]
        f, w, cached = out_handles[key]
        if cached is None:
            w.writerow(header)
            out_handles[key] = (f, w, header)
        return out_handles[key][2]

    # Leer primeras líneas para metadatos
    head_lines: List[str] = []
    with open(filepath, "r", encoding="utf-8-sig", errors="replace") as fh:
        # “Prime” primeras N líneas sin consumir el archivo completo
        for _ in range(80):
            pos = fh.tell()
            line = fh.readline()
            if not line:
                break
            head_lines.append(line)
            if len(head_lines) >= 80:
                break
        # Recolocar al inicio para procesar completo
        fh.seek(0)
        md = _detect_metadata(head_lines)

        # Estados
        IN_NONE, IN_T1_WAIT_HEADER, IN_T1_DATA, IN_T2_WAIT_HEADER, IN_T2_DATA = range(5)
        state = IN_NONE
        current_header: Optional[List[str]] = None
        t2_os_idx: Optional[int] = None

        def select_bucket(is_t1: bool, adjusted: bool) -> str:
            if is_t1:
                return "t1_ajustada" if adjusted else "t1_normal"
            return "t2_ajustada" if adjusted else "t2_normal"

        for raw in fh:
            line = raw.rstrip("\r\n")
            # Transiciones por marcador
            if state in (IN_NONE, IN_T1_DATA, IN_T2_DATA):
                if MARK_T1.match(line):
                    state = IN_T1_WAIT_HEADER
                    current_header = None
                    t2_os_idx = None
                    continue
                if MARK_T2.match(line):
                    state = IN_T2_WAIT_HEADER
                    current_header = None
                    t2_os_idx = None
                    continue

            if state == IN_T1_WAIT_HEADER:
                # La línea actual es el header de T1
                try:
                    current_header = next(csv.reader([line]))
                except Exception:
                    current_header = [c.strip() for c in line.split(",")]
                bucket = select_bucket(is_t1=True, adjusted=md["adjusted"])
                header = ensure_header(bucket, current_header)
                state = IN_T1_DATA
                continue

            if state == IN_T2_WAIT_HEADER:
                try:
                    current_header = next(csv.reader([line]))
                except Exception:
                    current_header = [c.strip() for c in line.split(",")]
                # detectar índice de 'operating system' (case-insensitive)
                t2_os_idx = None
                for idx, col in enumerate(current_header):
                    if col.strip().lower() == "operating system":
                        t2_os_idx = idx
                        break
                bucket = select_bucket(is_t1=False, adjusted=md["adjusted"])
                header = ensure_header(bucket, current_header)
                state = IN_T2_DATA
                continue

            if state == IN_T1_DATA:
                if not line.strip():
                    # pos fin de bloque
                    state = IN_NONE
                    continue
                row = next(csv.reader([line]))
                # añadir Cliente al final si no venía
                if len(row) < len(current_header):
                    # filas cortadas — rellenar
                    row = row + [""] * (len(current_header) - len(row))
                cliente = md["subcliente"] or md["cliente"] or cliente_por_defecto
                row_out = row + [cliente]
                f, w, _ = out_handles[select_bucket(True, md["adjusted"])]
                w.writerow(row_out)
                counts["t1_ajustada" if md["adjusted"] else "t1_normal"] += 1
                continue

            if state == IN_T2_DATA:
                if not line.strip():
                    state = IN_NONE
                    continue
                row = next(csv.reader([line]))
                if len(row) < len(current_header):
                    row = row + [""] * (len(current_header) - len(row))
                if t2_os_idx is not None:
                    row[t2_os_idx] = _norm_os_value(row[t2_os_idx], md["has_dc"])
                cliente = md["subcliente"] or md["cliente"] or cliente_por_defecto
                row_out = row + [cliente]
                f, w, _ = out_handles[select_bucket(False, md["adjusted"])]
                w.writerow(row_out)
                counts["t2_ajustada" if md["adjusted"] else "t2_normal"] += 1
                continue

    # Cerrar
    for f, _, _ in out_handles.values():
        f.close()

    # Log a progreso
    bus.push(session_id, "info", f"Procesado {os.path.basename(filepath)}  "
                                 f"(T1N={counts['t1_normal']}, T1A={counts['t1_ajustada']}, "
                                 f"T2N={counts['t2_normal']}, T2A={counts['t2_ajustada']})")
    return counts
