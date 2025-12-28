#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
snappy_inventory.py  (fixed)
- esa_snappy の現行環境で使えるものを可視化
  1) esa_snappy トップレベル属性
  2) GPF オペレータ & パラメータ
  3) I/Oプラグイン（Reader/Writer）
- 出力:
  - esa_snappy_inventory.md
  - esa_snappy_top_level.csv
  - esa_snappy_operators.json
  - esa_snappy_operator_params.csv
  - esa_snappy_io_readers.csv
  - esa_snappy_io_writers.csv
"""

from __future__ import annotations
import argparse, csv, json
from pathlib import Path
from typing import Any, Dict, List
from datetime import datetime

# esa_snappy import（旧名 snappy にもフォールバック）
try:
    import esa_snappy as SNAPPY
except Exception:
    import snappy as SNAPPY  # fallback

from esa_snappy import jpy, GPF

# ---------- helpers ----------
def java_list(x) -> List[Any]:
    """Java Set/List/Array/Iterator を Python list 化（Iterator対応版）"""
    if x is None:
        return []
    # try toArray()
    try:
        return list(x.toArray())
    except Exception:
        pass
    # Iterator そのもの?
    try:
        has_next = getattr(x, 'hasNext', None)
        nxt = getattr(x, 'next', None)
        if callable(has_next) and callable(nxt):
            out = []
            while x.hasNext():
                out.append(x.next())
            return out
    except Exception:
        pass
    # Iterable（Collection）?
    try:
        it = x.iterator()
        out = []
        while it.hasNext():
            out.append(it.next())
        return out
    except Exception:
        pass
    # jpy配列など
    try:
        return list(x)
    except Exception:
        return [x]

def safe_str(x) -> str:
    try:
        return str(x)
    except Exception:
        try:
            return repr(x)
        except Exception:
            return "<unprintable>"

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

# ---------- inventory: top-level of esa_snappy ----------
def inventory_top_level() -> List[Dict[str, str]]:
    rows = []
    names = sorted([n for n in dir(SNAPPY) if not n.startswith("_")])
    for n in names:
        try:
            obj = getattr(SNAPPY, n)
            tname = type(obj).__name__
            mod = getattr(obj, "__module__", "")
            rows.append({"name": n, "type": tname, "module": mod, "repr": safe_str(obj)[:200]})
        except Exception as e:
            rows.append({"name": n, "type": "<?>", "module": "", "repr": f"<error: {type(e).__name__}: {e}>"})
    return rows

# ---------- inventory: GPF operators & params ----------
def inventory_operators() -> Dict[str, Any]:
    reg = GPF.getDefaultInstance().getOperatorSpiRegistry()
    reg.loadOperatorSpis()
    spis = java_list(reg.getOperatorSpis())
    spis_sorted = sorted(spis, key=lambda s: safe_str(s.getOperatorAlias()))

    ops: List[Dict[str, Any]] = []
    params_flat_rows: List[Dict[str, str]] = []

    for spi in spis_sorted:
        try:
            alias = safe_str(spi.getOperatorAlias())
            desc = spi.getOperatorDescriptor()
            impl_class = safe_str(spi.getOperatorClass())
            try: name = safe_str(desc.getName())
            except: name = alias
            try: disp = safe_str(desc.getDisplayName())
            except: disp = name
            try: ver = safe_str(desc.getVersion())
            except: ver = ""

            pds = java_list(desc.getParameterDescriptors())
            params = []
            for pd in pds:
                try: pd_name = safe_str(pd.getName())
                except: pd_name = ""
                try: pd_alias = safe_str(pd.getAlias())
                except: pd_alias = pd_name
                try: dtype = safe_str(pd.getDataType().getName()) if pd.getDataType() else ""
                except: dtype = ""
                try:
                    default = pd.getDefaultValue()
                    default_s = "" if default is None else safe_str(default)
                except: default_s = ""
                vs = []
                try:
                    vs_raw = pd.getValueSet()
                    if vs_raw is not None:
                        vs = [safe_str(v) for v in java_list(vs_raw)]
                except: vs = []
                try: desc_text = safe_str(pd.getDescription())
                except: desc_text = ""

                params.append({
                    "name": pd_name, "alias": pd_alias, "type": dtype,
                    "default": default_s, "values": vs, "description": desc_text,
                })
                params_flat_rows.append({
                    "operator_alias": alias, "operator_name": name, "operator_version": ver,
                    "impl_class": impl_class, "param_name": pd_name, "param_alias": pd_alias,
                    "param_type": dtype, "default": default_s, "values": "|".join(vs),
                    "description": desc_text.replace("\n"," ").strip(),
                })

            ops.append({
                "alias": alias, "name": name, "display_name": disp,
                "version": ver, "impl_class": impl_class, "parameters": params,
            })
        except Exception as e:
            ops.append({"alias":"<error>","name":"<error>","display_name":"","version":"",
                        "impl_class":"","error":f"{type(e).__name__}: {e}","parameters":[]})
    return {"operators": ops, "params_flat": params_flat_rows}

# ---------- inventory: ProductIO readers/writers ----------
def inventory_io_plugins():
    PlugMgr = jpy.get_type('org.esa.snap.core.dataio.ProductIOPlugInManager')
    pm = PlugMgr.getInstance()
    # ★ 現行APIは getAllReaderPlugIns()/getAllWriterPlugIns() が「全件」
    #   （getReaderPlugIns(formatName) はフォーマット指定版）:
    #   https://step.esa.int/docs/v12.0/apidoc/engine/org/esa/snap/core/dataio/ProductIOPlugInManager.html
    readers_iter = pm.getAllReaderPlugIns()
    writers_iter = pm.getAllWriterPlugIns()
    readers = java_list(readers_iter)
    writers = java_list(writers_iter)

    def dump_reader(r):
        try: fmt = [safe_str(x) for x in java_list(r.getFormatNames())]
        except Exception: fmt = []
        try: ext = [safe_str(x) for x in java_list(r.getDefaultFileExtensions())]
        except Exception: ext = []
        return {"class": safe_str(r.getClass().getName()), "format_names": fmt, "default_extensions": ext}

    def dump_writer(w):
        try: fmt = [safe_str(x) for x in java_list(w.getFormatNames())]
        except Exception: fmt = []
        try: ext = [safe_str(x) for x in java_list(w.getDefaultFileExtensions())]
        except Exception: ext = []
        return {"class": safe_str(w.getClass().getName()), "format_names": fmt, "default_extensions": ext}

    return [dump_reader(r) for r in readers], [dump_writer(w) for w in writers]

# ---------- write helpers ----------
def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]):
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})

def main():
    ap = argparse.ArgumentParser(description="List all available esa_snappy features/operators/plugins in current env")
    ap.add_argument("--out", default=".", help="出力フォルダ（既定: カレント）")
    args = ap.parse_args()
    outdir = Path(args.out).resolve(); ensure_dir(outdir)

    # SNAP/esa_snappy バージョン（取れる範囲で）
    try:
        SystemUtils = jpy.get_type('org.esa.snap.core.util.SystemUtils')
        snap_ver = safe_str(SystemUtils.loadVersion())
    except Exception:
        snap_ver = ""
    try:
        jvm_ver = safe_str(jpy.get_type('java.lang.System').getProperty("java.runtime.version"))
    except Exception:
        jvm_ver = ""

    # 1) top-level
    top = inventory_top_level()
    write_csv(outdir / "esa_snappy_top_level.csv", top, ["name","type","module","repr"])

    # 2) operators
    ops = inventory_operators()
    (outdir / "esa_snappy_operators.json").write_text(
        json.dumps(ops["operators"], ensure_ascii=False, indent=2), encoding="utf-8")
    write_csv(outdir / "esa_snappy_operator_params.csv", ops["params_flat"],
              ["operator_alias","operator_name","operator_version","impl_class",
               "param_name","param_alias","param_type","default","values","description"])

    # 3) IO plugins
    readers, writers = inventory_io_plugins()
    write_csv(outdir / "esa_snappy_io_readers.csv",
              [{"class":r["class"],"format_names":"|".join(r["format_names"]),
                "default_extensions":"|".join(r["default_extensions"])} for r in readers],
              ["class","format_names","default_extensions"])
    write_csv(outdir / "esa_snappy_io_writers.csv",
              [{"class":w["class"],"format_names":"|".join(w["format_names"]),
                "default_extensions":"|".join(w["default_extensions"])} for w in writers],
              ["class","format_names","default_extensions"])

    # 4) Markdown サマリ
    md = []
    md.append(f"# esa-snappy Inventory ({datetime.utcnow().isoformat()}Z)")
    if snap_ver or jvm_ver:
        md += ["", f"- SNAP core version: `{snap_ver or '(unknown)'}`",
                    f"- JVM runtime      : `{jvm_ver or '(unknown)'}`", ""]
    md += [
        "## Files",
        "- `esa_snappy_top_level.csv`",
        "- `esa_snappy_operators.json`",
        "- `esa_snappy_operator_params.csv`",
        "- `esa_snappy_io_readers.csv` / `esa_snappy_io_writers.csv`",
        "",
        "## Quick Stats",
        f"- Top-level attrs : {len(top)}",
        f"- Operators       : {len(ops['operators'])}",
        f"- Operator params : {len(ops['params_flat'])}",
        f"- Readers         : {len(readers)}",
        f"- Writers         : {len(writers)}",
    ]
    (outdir / "esa_snappy_inventory.md").write_text("\n".join(md), encoding="utf-8")

    print("Done.")
    print(f"Output dir: {outdir}")
    for fn in ["esa_snappy_inventory.md","esa_snappy_top_level.csv",
               "esa_snappy_operators.json","esa_snappy_operator_params.csv",
               "esa_snappy_io_readers.csv","esa_snappy_io_writers.csv"]:
        print(" -", outdir / fn)

if __name__ == "__main__":
    main()
