from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from quant.pipelines.single_date_eod_pipeline import SingleDateEodPipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run single-date EOD pipeline")
    parser.add_argument("--date", required=True, help="Target trade date in YYYY-MM-DD")
    parser.add_argument("--force", action="store_true", default=False, help="Force rerun")
    args = parser.parse_args()

    pipeline = SingleDateEodPipeline()
    result = pipeline.run(target_date=args.date, force=args.force)
    print(result)


if __name__ == "__main__":
    main()
