#!/usr/bin/env python3
"""Verifica el kit y, opcionalmente, los JSONL descargados de HDFS."""
import argparse, csv, hashlib, json, math
from pathlib import Path

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--validos', type=Path)
p.add_argument('--rechazados', type=Path)
a = p.parse_args()
if bool(a.validos) != bool(a.rechazados):
    p.error('Indica juntos --validos y --rechazados')
base = Path(__file__).resolve().parent
manifest = json.loads((base/'manifest.json').read_text())
schema = json.loads((base/'operacion_plata.avsc').read_text())
fields = [f['name'] for f in schema['fields']]
numeric = ['ore_tonnes','ag_grade_g_t','recovery_pct']
expected = {'validos':{}, 'rechazados':{}}
def require(ok, message):
    if not ok:
        raise SystemExit('ERROR: '+message)
def good(r):
    return all(r[k] is not None and math.isfinite(r[k]) for k in numeric) and r['ore_tonnes'] > 0 and r['ag_grade_g_t'] > 0 and 0 <= r['recovery_pct'] <= 100
for b in manifest['batches']:
    path = base/b['file']
    require(hashlib.sha256(path.read_bytes()).hexdigest() == b['sha256'], 'checksum '+str(path))
    with path.open(newline='') as f:
        reader = csv.DictReader(f)
        require(reader.fieldnames == fields, 'encabezado CSV')
        rows = list(reader)
    require(len(rows) == b['records'], 'conteo del lote')
    rejected = []
    for r in rows:
        for k in numeric:
            r[k] = None if r[k]=='NULL' else float(r[k])
        key = r['event_id']
        require(not any(key in group for group in expected.values()), 'event_id repetido en CSV')
        kind = 'validos' if good(r) else 'rechazados'
        expected[kind][key] = r
        if kind == 'rechazados': rejected.append(key)
    require(rejected == b['rejected_event_ids'], 'rechazados del lote')
require(len(expected['validos']) == manifest['expected_valid'], 'conteo válido')
require(len(expected['rechazados']) == manifest['expected_rejected'], 'conteo rechazado')
print('Kit OK: 3 lotes, 72 registros; 66 válidos y 6 rechazados.')
if a.validos:
    for kind, path in [('validos',a.validos),('rechazados',a.rechazados)]:
        seen = set()
        for line in path.read_text().splitlines():
            if not line.strip(): continue
            r = json.loads(line)
            key = r.get('event_id')
            require(key in expected[kind], kind+': event_id inesperado '+str(key))
            require(key not in seen, kind+': duplicado '+key)
            require(r == expected[kind][key], kind+': contenido alterado '+key)
            seen.add(key)
        require(seen == set(expected[kind]), kind+': faltan registros')
    print('Salidas HDFS OK: 66 válidos y 6 rechazados, sin pérdidas, duplicados ni cambios de contenido.')
