#!/usr/bin/env python3

import datetime
import pathlib
import subprocess
import sys

if len(sys.argv) < 3:
    print(f"usage: {sys.argv[0]} REPORTS_DIR PROGRAM [ARGS [...]]")
    exit(1)

reports_dir = pathlib.Path(sys.argv[1])
args = sys.argv[2:]

now = datetime.datetime.today()

base_path = reports_dir / f"{now.year:04}" / f"{now.month:02}" / f"{now.day:02}"
base_name = now.isoformat()

report_info   = base_path / f"{base_name}.info"
report_stdout = base_path / f"{base_name}.stdout"
report_stderr = base_path / f"{base_name}.stderr"

base_path.mkdir(parents=True, exist_ok=True)

with open(report_info, "a") as f_info:
    f_info.write(f"Arguments: {args}\n")

with open(report_stdout, "a") as f_stdout, open(report_stderr, "a") as f_stderr:
    result = subprocess.run(args, stdout=f_stdout, stderr=f_stderr)

with open(report_info, "a") as f_info:
    f_info.write(f"Return code: {result.returncode}\n")

exit(result.returncode)
