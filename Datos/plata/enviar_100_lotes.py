#!/usr/bin/env python3
"""Entrega los 100 CSV ampliados al directorio vigilado por ListFile.

Ejecutar en el servidor NiFi o en un montaje accesible por NiFi:
  python3 enviar_100_lotes.py --destino /tmp/nifi-plata/alumno01/r100/entrada

No genera datos ni modifica el flujo. Conserva el kit pequeño independiente.
Usa un run.id, topic/grupo, tabla y rutas nuevos para el conjunto ampliado.
"""

import argparse
import hashlib
import json
import shutil
import time
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--destino', required=True, type=Path)
    parser.add_argument('--intervalo', type=float, default=1,
                        help='Segundos entre archivos; por defecto 1. No garantiza orden de procesamiento.')
    parser.add_argument('--simular', action='store_true',
                        help='Verifica los 100 CSV y el destino sin copiar ni crear directorios.')
    args = parser.parse_args()
    if not 0 <= args.intervalo < float('inf'):
        parser.error('--intervalo debe ser un número finito >= 0')

    root = Path(__file__).resolve().parent / 'ampliado'
    files = [root / 'datos' / f'lote_{i:03d}.csv' for i in range(1, 101)]
    manifest = json.loads((root / 'manifest.json').read_text())
    expected = {entry['file']: entry['sha256'] for entry in manifest['files']}
    if args.destino.exists() and not args.destino.is_dir():
        parser.error('El destino debe ser un directorio')
    for source in files:
        if not source.is_file():
            parser.error(f'Falta {source}; ejecuta primero generar_lotes_ampliados.py')
        if hashlib.sha256(source.read_bytes()).hexdigest() != expected['datos/' + source.name]:
            parser.error(f'Checksum incorrecto: {source.name}')
        for name in (source.name, source.name + '.part'):
            if (args.destino / name).exists():
                parser.error('Hay lotes anteriores en el destino; utiliza una ejecución nueva')

    print('Verificados: 100 CSV / 5000000 registros; 4995000 válidos y 5000 rechazos esperados.')
    if args.simular:
        print('Simulación: no se copió ningún archivo.')
        return
    args.destino.mkdir(parents=True, exist_ok=True)
    for index, source in enumerate(files, 1):
        partial = args.destino / (source.name + '.part')
        with partial.open('xb') as output, source.open('rb') as input_file:
            shutil.copyfileobj(input_file, output)
        # Publicación atómica sin sobrescribir: ambos nombres están en el mismo filesystem.
        # ListFile debe filtrar lote_[0-9]{3}\.csv, excluyendo los .part.
        (args.destino / source.name).hardlink_to(partial)
        partial.unlink()
        print(f'{index:03d}/100 disponible: {source.name}', flush=True)
        if index < len(files):
            time.sleep(args.intervalo)
    print('Entrega terminada. Espera el drenaje de NiFi y MergeRecord antes de conciliar.')


if __name__ == '__main__':
    main()
