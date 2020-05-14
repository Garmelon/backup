import datetime
import subprocess
import sys
from typing import Any, List, Optional

CalledProcessError = subprocess.CalledProcessError

class Util:
    def __init__(self, prefix: str, dry_run: Optional[bool] = None):
        self.prefix = prefix

        if dry_run is None:
            self.dry_run = "--dry-run" in sys.argv[1:]

        else:
            self.dry_run = dry_run

    def format(self, text: str) -> str:
        return f"[[{self.prefix}]] {text}"

    def _say_stdout(self, text: str) -> None:
        print(text, file=sys.stdout, flush=True)

    def _say_stderr(self, text: str) -> None:
        print(text, file=sys.stderr, flush=True)

    def say(self, text: str) -> None:
        try:
            self._say_stdout(self.format(text))
        except BrokenPipeError:
            pass

    def sayboth(self, text: str) -> None:
        try:
            formatted = self.format(text)
            self._say_stdout(formatted)
            self._say_stderr(formatted)
        except BrokenPipeError:
            pass

    def _sayboth_with_time(self, text: str) -> None:
        now = datetime.datetime.today()
        self.sayboth(f"{text} ({now.isoformat()})")

    def run(self, cmdargs: List[Any], *args: Any, **kwargs: Any) -> None:
        self._sayboth_with_time(f"COMMAND: Running {cmdargs}")

        if self.dry_run:
            self._sayboth_with_time("COMMAND: Exited with code 'dry-run'")
            return

        try:
            result = subprocess.run(cmdargs, *args, **kwargs, check=True)  # type: ignore
            self._sayboth_with_time(f"COMMAND: Exited with code {result.returncode}")
        except CalledProcessError as e:
            self._sayboth_with_time(f"COMMAND: Exited with code {e.returncode}")
            raise
