#!/usr/bin/env python3
"""Simula archivos periódicos en el nodo de NiFi o en un montaje compartido."""
import argparse, shutil, time
from pathlib import Path

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--destino', required=True, type=Path)
p.add_argument('--intervalo', type=float, default=30)
a = p.parse_args()
if a.intervalo < 0:
    p.error('--intervalo debe ser >= 0')
a.destino.mkdir(parents=True, exist_ok=True)
files = sorted((Path(__file__).resolve().parent / 'datos').glob('lote_*.csv'))
if len(files) != 3:
    p.error('Se esperan los tres CSV del kit')
if any((a.destino / f.name).exists() or (a.destino / (f.name+'.part')).exists() for f in files):
    p.error('El destino contiene lotes anteriores; utiliza otra ruta y run.id')
for i, src in enumerate(files):
    partial = a.destino / (src.name+'.part')
    with partial.open('xb') as out, src.open('rb') as inp:
        shutil.copyfileobj(inp, out)
    # Renombrar en el mismo filesystem evita que ListFile vea un CSV parcial.
    partial.rename(a.destino / src.name)
    print('Disponible:', a.destino / src.name, flush=True)
    if i < len(files)-1:
        time.sleep(a.intervalo)
