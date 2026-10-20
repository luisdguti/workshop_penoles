#!/usr/bin/env python3
"""Genera 100 CSV sintéticos de 50 000 registros, sin dependencias externas.

Salida: ampliado/ junto a este script. No sobrescribe ejecuciones previas.
No carga datos en NiFi ni modifica los tres lotes del taller.
"""

import argparse
import csv
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--salida', type=Path,
                        default=Path(__file__).resolve().parent / 'ampliado')
    args = parser.parse_args()
    base = Path(__file__).resolve().parent
    seeds = []
    for batch in range(1, 4):
        with (base / 'datos' / f'lote_{batch:03d}.csv').open(newline='') as source:
            reader = csv.DictReader(source)
            fields = reader.fieldnames
            seeds.extend(reader)
    if len(seeds) != 72:
        parser.error('Se requieren los tres lotes originales de 24 registros.')
    if args.salida.exists():
        parser.error('La salida ya existe. Usa --salida con un directorio nuevo.')
    data_dir = args.salida / 'datos'
    data_dir.mkdir(parents=True, exist_ok=False)
    start = datetime(2026, 1, 3, tzinfo=timezone.utc)
    manifest = {
        'dataset': 'plata_ampliado_v1', 'synthetic': True,
        'provenance': 'Simulación didáctica; no son datos reales de Peñoles.',
        'rows': 5_000_000, 'valid': 4_995_000, 'rejected': 5_000,
        'groups': 62, 'start': '2026-01-01T00:00:00Z',
        'end': '2026-01-31T22:26:03Z', 'schema': fields, 'files': []}
    for batch in range(1, 101):
        batch_id = f'lote_{batch:03d}'
        path = data_dir / f'{batch_id}.csv'
        # El primer archivo conserva seis rechazos de los 72 registros base.
        invalid = {1000 * z - z % 2 for z in range(1, 45 if batch == 1 else 51)}
        with path.open('w', encoding='utf-8', newline='') as output:
            writer = csv.DictWriter(output, fieldnames=fields, lineterminator='\n')
            writer.writeheader()
            for pos in range(1, 50_001):
                idx = (batch - 1) * 50_000 + pos
                if idx <= 72:
                    row = dict(seeds[idx - 1], batch_id=batch_id)
                else:
                    offset = idx - 73
                    instant = start + timedelta(seconds=offset // 2)
                    x = (idx * 2654435761) & 0xffffffff
                    y = ((x ^ (x >> 16)) * 2246822519) & 0xffffffff
                    row = dict(zip(fields, [
                        f'AG-VOL-{idx:07d}', batch_id,
                        instant.isoformat().replace('+00:00', 'Z'),
                        'UNIDAD_DEMO_A' if offset % 2 == 0 else 'UNIDAD_DEMO_B',
                        f'{(10 + y % 190) / 1000:.3f}',
                        f'{(1000 + (y // 190) % 1500) / 10:.1f}',
                        f'{(800 + (y // 285000) % 151) / 10:.1f}',
                        'SIMULADOR_TALLER']))
                    if pos in invalid:
                        kind = (pos // 1000) % 3
                        if kind == 0:
                            row['ore_tonnes'] = '-0.005'
                        else:
                            row['recovery_pct'] = '105.0' if kind == 1 else 'NULL'
                writer.writerow(row)
        manifest['files'].append({
            'file': f'datos/{path.name}', 'rows': 50_000,
            'valid': 49_950, 'rejected': 50, 'bytes': path.stat().st_size,
            'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
        print(f'{batch:03d}/100: {path.name}', flush=True)
    (args.salida / 'manifest.json').write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    (args.salida / 'SHA256SUMS').write_text(''.join(
        f"{entry['sha256']}  {entry['file']}\n" for entry in manifest['files']),
        encoding='utf-8')
    print('Listo: 5 000 000 registros; 4 995 000 válidos y 5 000 rechazados.')
    print('Para entregarlos a NiFi, utiliza enviar_100_lotes.py.')


if __name__ == '__main__':
    main()
