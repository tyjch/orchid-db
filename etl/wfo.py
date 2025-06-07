import csv
import re
from pathlib import Path
from loguru import logger

log = logger.bind(tags=["wfo-etl"])

input_dir = Path("datasets/World Flora Online/families")
output_dir = Path("dbt/seeds/wfo")
output_dir.mkdir(parents=True, exist_ok=True)
output_csv = output_dir / "classification.csv"

def sanitize_column(name: str) -> str:
    return re.sub(r"[^a-z0-9_]", "", name.strip().lower().replace(" ", "_"))

writer = None
written_rows = 0
skipped_files = 0

with output_csv.open("w", newline='', encoding="utf-8") as out_f:
    for family_dir in sorted(input_dir.iterdir()):
        classification_file = family_dir / "classification.csv"
        if not classification_file.exists():
            log.warning(f"Missing: {classification_file}")
            continue

        family_folder = family_dir.name
        match = re.match(r"(.+?)_wfo-(\d+)", family_folder)
        if not match:
            log.warning(f"Unrecognized folder name format: {family_folder}")
            continue
        family_name, wfo_id = match.groups()

        for encoding in ("utf-8", "utf-8-sig", "latin-1"):
            try:
                with classification_file.open("r", encoding=encoding) as in_f:
                    reader = csv.DictReader(in_f)

                    # Sanitize header names
                    if writer is None:
                        sanitized_fields = [sanitize_column(h) for h in reader.fieldnames]
                        fieldnames = ["family_name", "wfo_id"] + sanitized_fields
                        writer = csv.DictWriter(out_f, fieldnames=fieldnames)
                        writer.writeheader()

                    try:
                        for row in reader:
                            sanitized_row = {
                                sanitize_column(k): v for k, v in row.items()
                            }
                            sanitized_row["family_name"] = family_name
                            sanitized_row["wfo_id"] = wfo_id
                            writer.writerow(sanitized_row)
                            written_rows += 1
                    except csv.Error as e:
                        log.error(f"✗ Skipped: {classification_file} — CSV error: {e}")
                        skipped_files += 1
                        break


                log.info(f"✓ Processed: {classification_file} using {encoding}")
                break
            except UnicodeDecodeError:
                continue
        else:
            log.error(f"✗ Skipped: {classification_file} — unknown encoding")
            skipped_files += 1

log.success(f"✅ Done: wrote {written_rows} rows from {len(list(input_dir.iterdir())) - skipped_files} families")
