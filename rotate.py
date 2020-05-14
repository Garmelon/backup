#!/usr/bin/env python3

import argparse
import configparser
import datetime
import subprocess
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, List, Optional, Tuple

DESCRIPTION = """
Create and rotate snapshots of a source directory in regular intervals.

Supports normal and hardlinked copies as well as btrfs snapshots. Supports
daily, weekly, monthly, bi-yearly, yearly and arbitrary day-based intervals.
Also supports offsetting those intervals by a fixed number of days.
"""

EPILOG = """
This script expects the following directory structure:

<directory>/                    the source directory
<directory>-snapshots/          the snapshot directory
  +- rotate.conf                the config file
  +- <section-1>/               a section
  +- <section-2>/               another section
  +- ...

A section is a directory containing a fixed number of snapshots of the source
directory taken at regular intervals. The sections are located in the snapshot
directory. The names and intervals of the sections are defined in the config
file.

The config file uses a structure similar to the INI file format. Each section
(except the "DEFAULT" section) in the config file represents a section in the
snapshot directory. The values of the "DEFAULT" section are used as default
values for the other sections. Here is a short example:

    [DEFAULT]
    method=hardlink

    [daily]
    interval=daily
    amount=7

    [bimonthly]
    interval=60d
    amount=4

The following options are recognized and can be set individually for each
section:

  method

The method used to create and remove snapshots. "copy" uses the "cp -ar" and
"rm -rf" commands. "hardlink" uses the "cp -arl" and "rm -rf" commands. "btrfs"
uses the "btrfs subvolume snapshot -r" and "btrfs subvolume delete" commands.

One of: copy, hardlink, btrfs
Default: copy

  interval

The interval in which to create new snapshots. This value can either be one of
the existing specifiers (daily, weekly, ...) or a custom interval (e. g. 3d,
180d). "biyearly" means "twice yearly" and splits the year into the months
Jan-Jun and Jul-Dec.

One of: daily, weekly, monthly, biyearly, yearly, <n>d
Default: daily

  amount

The maximum number of snapshots to keep in this section. If a section contains
too many snapshots, they are removed (from oldest to newest) until their amount
is below the section's maximum amount again.

Format: positive integer
Default: 7

  offset

A number of dates to be added to the current date before any interval
calculations take place (e. g. 5d, -3d). An example use for this option is to
delay the creation of montly snapshots until the middle of the month.

Format: <n>d
Default: 0d
"""

###################################
## Utility functions and classes ##
###################################

def julian_day(dt: datetime.datetime) -> int:
    delta = dt - datetime.datetime(1, 1, 1)
    return delta.days + 1721426

def days_from_string(string: str) -> Optional[int]:
    string = string.strip()
    if string and string[-1] == "d":
        try:
            return int(string[:-1])
        except ValueError:
            pass
    return None

def format_time(dt: datetime.datetime) -> str:
    return dt.isoformat(sep=" ", timespec="minutes")

@dataclass
class Options:
    source_dir: Path
    snapshot_dir: Path
    config_file: Path
    dry_run: bool
    time: datetime.datetime
    config: configparser.ConfigParser

class Util:
    def __init__(self, prefix: str, dry_run: bool = False):
        self.prefix = prefix
        self.dry_run = dry_run

    def format(self, text: str) -> str:
        return f"[[{self.prefix}]] {text}"

    def _say_stdout(self, text: str) -> None:
        print(text, file=sys.stdout, flush=True)

    def _say_stderr(self, text: str) -> None:
        print(text, file=sys.stderr, flush=True)

    def say(self, text: str) -> None:
        self._say_stdout(self.format(text))

    def sayboth(self, text: str) -> None:
        formatted = self.format(text)
        self._say_stdout(formatted)
        self._say_stderr(formatted)

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
        except subprocess.CalledProcessError as e:
            self._sayboth_with_time(f"COMMAND: Exited with code {e.returncode}")
            raise

###############
## Intervals ##
###############

Interval = Callable[[datetime.datetime], Any]

def interval_daily(dt: datetime.datetime) -> Tuple[int, int, int]:
    return (dt.year, dt.month, dt.day)

def interval_weekly(dt: datetime.datetime) -> Tuple[int, int]:
    (year, week, _) = dt.isocalendar()
    return (year, week)

def interval_monthly(dt: datetime.datetime) -> Tuple[int, int]:
    return (dt.year, dt.month)

def interval_biyearly(dt: datetime.datetime) -> Tuple[int, int]:
    return (dt.year, 1 if dt.month <= 6 else 2)

def interval_yearly(dt: datetime.datetime) -> int:
    return dt.year

def interval_custom(days: int) -> Callable[[datetime.datetime], int]:
    def interval(dt: datetime.datetime) -> int:
        return julian_day(dt) // days
    return interval

INTERVALS = {
    "daily": interval_daily,
    "weekly": interval_weekly,
    "monthly": interval_monthly,
    "biyearly": interval_biyearly,
    "yearly": interval_yearly,
}

def interval_from_string(string: str) -> Optional[Interval]:
    string = string.strip()

    interval = INTERVALS.get(string)
    if interval is not None:
        return interval

    days = days_from_string(string)
    if days is not None and days > 0:
        return interval_custom(days)

    return None

#############
## Methods ##
#############

class OperationFailedException(Exception):
    pass

class Method(ABC):
    def __init__(self, util: Util):
        self.util = util

    @abstractmethod
    def create_snapshot(self, source: Path, target: Path) -> None:
        pass

    @abstractmethod
    def remove_snapshot(self, target: Path) -> None:
        pass

    def prepare_target(self, target: Path) -> None:
        if target.exists():
            raise OperationFailedException(
                f"Could not create snapshot at {target}: "
                "Directory or file already exists"
            )

        if self.util.dry_run: return

        try:
            target.parent.mkdir(parents=True, exist_ok=True)
        except FileExistsError as e:
            raise OperationFailedException(
                f"Could not create snapshot at {target}: "
                "Could not create parent directory\n"
                f"{e}"
            )

    def run(self, args: List[Any]) -> None:
        try:
            self.util.run(args)
        except subprocess.CalledProcessError as e:
            raise OperationFailedException("External command failed")

class CopyMethod(Method):
    def create_snapshot(self, source: Path, target: Path) -> None:
        self.prepare_target(target)
        self.run(["cp", "-ar", source, target])

    def remove_snapshot(self, target: Path) -> None:
        self.run(["rm", "-rf", target])

class HardlinkMethod(Method):
    def create_snapshot(self, source: Path, target: Path) -> None:
        self.prepare_target(target)
        self.run(["cp", "-arl", source, target])

    def remove_snapshot(self, target: Path) -> None:
        self.run(["rm", "-rf", target])

class BtrfsSnapshotMethod(Method):
    def create_snapshot(self, source: Path, target: Path) -> None:
        self.prepare_target(target)
        self.run(["btrfs", "subvolume", "snapshot", "-r", source, target])

    def remove_snapshot(self, target: Path) -> None:
        self.run(["btrfs", "subvolume", "delete", target])

METHODS = {
    "copy": CopyMethod,
    "hardlink": HardlinkMethod,
    "btrfs": BtrfsSnapshotMethod,
}

def method_from_string(util: Util, string: str) -> Optional[Method]:
    if string == "copy":
        return CopyMethod(util)
    if string == "hardlink":
        return HardlinkMethod(util)
    if string == "btrfs":
        return BtrfsSnapshotMethod(util)
    return None

###############
## Snapshots ##
###############

@dataclass
class Snapshot:
    path: Path
    when: datetime.datetime

def find_snapshots(util: Util, directory: Path) -> List[Snapshot]:
    if not directory.exists():
        return []

    snapshots = []

    for child in directory.iterdir():
        if not child.is_dir():
            util.say(f"{child} is not a directory, skipping it")
            continue

        try:
            dt = datetime.datetime.fromisoformat(child.name)
            snapshots.append(Snapshot(child, dt))
        except ValueError as e:
            util.say(f"Name of {child} cannot be interpreted as datetime, skipping it")

    snapshots.sort(key=lambda x: x.when)
    return snapshots

##############
## Sections ##
##############

@dataclass
class SectionOptions:
    method: Method
    interval: Interval
    offset: int
    amount: int

def read_section_options(util: Util, options: Options, section_name: str) -> Optional[SectionOptions]:
    section = options.config[section_name]

    method = None
    maybe_method = method_from_string(util, section.get("method", fallback="copy"))
    if maybe_method is None:
        util.say("Invalid method (see the help text)")
    else:
        method = maybe_method

    interval = None
    maybe_interval = interval_from_string(section.get("interval", fallback="daily"))
    if maybe_interval is None:
        util.say("Invalid interval (see the help text)")
    else:
        interval = maybe_interval

    offset = None
    maybe_offset = days_from_string(section.get("offset", fallback="0d"))
    if maybe_offset is None:
        util.say("Invalid offset (see the help text)")
    else:
        offset = maybe_offset

    amount = None
    maybe_amount = section.getint("amount", fallback=7)
    if maybe_amount <= 0:
        util.say("Invalid amount (see the help text)")
    else:
        amount = maybe_amount

    if method is None or interval is None or offset is None or amount is None:
        return None

    return SectionOptions(method, interval, offset, amount)

def do_section(util: Util, options: Options, section_name: str) -> None:
    section_options = read_section_options(util, options, section_name)
    if section_options is None:
        util.say("Section is configured incorrectly, skipping it")
        return

    section_dir = options.snapshot_dir / section_name
    # Ugly hack to prevent python from calling the interval function with a
    # self argument. That's what I get for treating functions like first class
    # values, I guess.
    interval = section_options.__dict__["interval"]
    offset = datetime.timedelta(section_options.offset)

    snapshots = find_snapshots(util, section_dir)
    amount = section_options.amount

    # Check if we need to make a snapshot
    now_interval = interval(options.time + offset)
    for snapshot in snapshots:
        snapshot_interval = interval(snapshot.when + offset)
        if now_interval == snapshot_interval:
            util.say(f"Current interval already covered by {snapshot.path}")
            break
    else:  # The elusive for-else clause! :D
        util.say("Making a new snapshot")
        new_snapshot_path = section_dir / format_time(options.time)
        section_options.method.create_snapshot(options.source_dir, new_snapshot_path)
        amount -= 1  # The newly added snapshot is not in the snapshots list

    # Remove old snapshots
    while len(snapshots) > amount:
        snapshot = snapshots.pop(0)
        section_options.method.remove_snapshot(snapshot.path)

#####################
## Everything else ##
#####################

def type_time(string: str) -> datetime.datetime:
    try:
        return datetime.datetime.fromisoformat(string)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"{string!r} does not follow format YYYY-MM-DD[ HH:MM]"
        )

def main() -> None:
    parser = argparse.ArgumentParser(
        description=DESCRIPTION,
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("directory", type=Path, metavar="DIRECTORY",
                        help="the directory to create a snapshot of")
    parser.add_argument("-c", "--config", type=Path, metavar="CONFIG_FILE",
                        help="path to the config file")
    parser.add_argument("-s", "--snapshots", type=Path, metavar="SNAPSHOT_DIR",
                        help="path to the snapshot directory")
    parser.add_argument("-t", "--time", type=type_time, metavar="TIME",
                        help="use this time instead of the current time")
    parser.add_argument("-d", "--dry-run", action="store_true",
                        help="don't execute any commands, only print them")
    args = parser.parse_args()

    source_dir = args.directory
    snapshot_dir = args.snapshots or source_dir.with_name(f"{source_dir.name}-snapshots")
    config_file = args.config or snapshot_dir / "rotate.conf"
    time = args.time or datetime.datetime.today()
    dry_run = args.dry_run

    util = Util(__file__, dry_run)

    util.say(f"  Source directory: {source_dir}")
    util.say(f"Snapshot directory: {snapshot_dir}")
    util.say(f"       Config file: {config_file}")
    util.say(f"              Time: {format_time(time)}")
    util.say(f"           Dry run: {'yes' if dry_run else 'no'}")

    config = configparser.ConfigParser()
    config.read(config_file)

    util.say(f"          Sections: {config.sections()}")

    options = Options(source_dir, snapshot_dir, config_file, dry_run, time, config)

    for section_name in config.sections():
        util.say("")
        util.say(f"  Section {section_name!r}:")
        do_section(util, options, section_name)

if __name__ == "__main__":
    try:
        main()
    except OperationFailedException as e:
        print("Operation failed:")
        print(e)
        exit(1)
