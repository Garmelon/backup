#!/usr/bin/env python3

import pathlib
import subprocess
import sys

if len(sys.argv) < 2:
    print(f"usage: {sys.argv[0]} PROGRAM [ARGS [...]]")
    exit(1)

here = pathlib.Path(__file__).parent
result = subprocess.run(sys.argv[1:], cwd=here)
exit(result.returncode)
