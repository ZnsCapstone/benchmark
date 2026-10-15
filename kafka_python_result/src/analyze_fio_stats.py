"""Join host FEMU CSV files to a completed guest fio summary without rerunning I/O."""
import argparse
import json
from pathlib import Path

from fio_evaluation import femu_delta


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--results', required=True)
    parser.add_argument('--femu-csv', action='append', required=True)
    parser.add_argument('--clock-offset-seconds', type=float, default=0,
                        help='host epoch minus guest epoch; verify both clocks')
    args = parser.parse_args()
    root = Path(args.results).resolve()
    records = json.loads((root / 'summary.json').read_text())
    offset = int(args.clock_offset_seconds * 1000000)
    for record in records:
        record['femu'] = {file: femu_delta(file, record['measurement_start_us_approx'] + offset,
                                         record['end_us'] + offset) for file in args.femu_csv}
        record['clock_offset_seconds'] = args.clock_offset_seconds
        programs = [value['nand_program_bytes'] for value in record['femu'].values()
                    if value.get('available') and value.get('nand_program_bytes') is not None]
        host_writes = sum(value.get('delta', {}).get('host_write_bytes', 0)
                          for value in record['femu'].values() if value.get('available'))
        record['aggregate_nand_wa_host'] = sum(programs) / host_writes if host_writes and len(programs) == len(args.femu_csv) else None
        denominator = record.get('dm_delta', {}).get('logical_write_bytes')
        record['aggregate_nand_wa_dm_logical'] = sum(programs) / denominator if denominator and len(programs) == len(args.femu_csv) else None
    target = root / 'summary-with-femu.json'
    if target.exists():
        raise SystemExit('Refusing to overwrite existing joined summary')
    target.write_text(json.dumps(records, indent=2))
    print(target)


if __name__ == '__main__':
    main()
