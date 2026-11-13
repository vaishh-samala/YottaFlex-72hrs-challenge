"""Ingestion: file checks, header normalisation, schema-drift detection, malformed-row capture.

Every value is read as text. Type conversion happens later in validation so that a bad
value becomes a traceable record-level rejection instead of crashing the whole read.
"""
from __future__ import annotations

import csv
import hashlib
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from src.schemas import CONTRACTS, SourceContract

NULL_TOKENS = ("", "null", "NULL", "Null", "None", "none", "NaN", "nan", "N/A", "n/a")
ROW_NUMBER_COL = "source_row_number"


class BatchError(Exception):
    """A batch-level failure: the whole file (and run) must be rejected."""

    code = "BATCH_ERROR"


class SourceUnavailableError(BatchError):
    code = "SOURCE_UNAVAILABLE"


class SchemaValidationError(BatchError):
    code = "SCHEMA_MISMATCH"


@dataclass
class MalformedRecord:
    row_number: int                # first physical line of the record (header = 1)
    raw_line: str                  # exact original text of the record (line terminator removed)
    reason: str
    record_id: str | None = None   # primary-key field, if the row is long enough to contain it


@dataclass
class IngestedSource:
    source_type: str
    path: Path
    file_name: str
    file_hash: str
    size_bytes: int
    header_raw: list[str]
    header: list[str]
    data: pd.DataFrame                         # contract columns (text) + source_row_number
    malformed: list[MalformedRecord] = field(default_factory=list)
    source_lines: list[str] = field(default_factory=list)  # exact original text of each ``data`` row
    blank_lines: int = 0
    warnings: list[str] = field(default_factory=list)
    extra_columns: list[str] = field(default_factory=list)

    @property
    def row_count(self) -> int:
        """Physical data rows read from the file (well-formed + malformed)."""
        return len(self.data) + len(self.malformed)

    @property
    def is_empty(self) -> bool:
        return self.row_count == 0


_CAMEL_1 = re.compile(r"(.)([A-Z][a-z]+)")
_CAMEL_2 = re.compile(r"([a-z0-9])([A-Z])")
_NON_ALNUM = re.compile(r"[^0-9a-zA-Z]+")


def to_snake_case(name: str) -> str:
    """'Order Line ID' / 'orderLineId' / 'ORDER-LINE-ID' -> 'order_line_id'."""
    name = name.strip().lstrip("﻿")
    if not name.isupper():
        name = _CAMEL_1.sub(r"\1_\2", name)
        name = _CAMEL_2.sub(r"\1_\2", name)
    name = _NON_ALNUM.sub("_", name).strip("_").lower()
    return re.sub(r"_+", "_", name)


class _LineRecorder:
    """Line iterator for ``csv.reader`` that remembers the physical text of each record it consumes.

    ``csv.reader`` pulls exactly the lines one record needs (several for quoted fields containing
    newlines), so after each record ``take()`` returns that record's original text, byte for byte
    apart from the trailing line terminator.
    """

    def __init__(self, handle):
        self._lines = iter(handle)
        self._buffer: list[str] = []
        self.lines_consumed = 0

    def __iter__(self):
        return self

    def __next__(self) -> str:
        line = next(self._lines)
        self._buffer.append(line)
        self.lines_consumed += 1
        return line

    def take(self) -> str:
        text = "".join(self._buffer)
        self._buffer.clear()
        if text.endswith("\r\n"):
            return text[:-2]
        return text[:-1] if text.endswith(("\n", "\r")) else text


def file_sha256(path: Path, chunk_size: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_source(path: Path | str, source_type: str) -> IngestedSource:
    """Read one source file according to its contract.

    Raises ``SourceUnavailableError`` / ``SchemaValidationError`` for batch-level problems.
    Record-level problems (wrong field count) are returned in ``malformed``.
    """
    contract: SourceContract = CONTRACTS[source_type]
    path = Path(path)
    if not path.exists() or not path.is_file():
        raise SourceUnavailableError(f"{source_type} source file not found: {path}")

    size = path.stat().st_size
    file_hash = file_sha256(path)
    warnings: list[str] = []

    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as handle:
            recorder = _LineRecorder(handle)
            reader = csv.reader(recorder)
            try:
                header_raw = next(reader)
            except StopIteration:
                header_raw = []
            recorder.take()
            if not header_raw:
                # Zero-byte (or header-less) file: an empty delivery, not a schema change.
                warnings.append("EMPTY_FILE: file contains no header and no rows")
                empty = pd.DataFrame({c: pd.Series(dtype="object") for c in contract.columns})
                empty[ROW_NUMBER_COL] = pd.Series(dtype="int64")
                return IngestedSource(source_type, path, path.name, file_hash, size, [], [], empty, warnings=warnings)

            header = [to_snake_case(h) for h in header_raw]
            dupes = sorted({h for h in header if header.count(h) > 1})
            if dupes:
                raise SchemaValidationError(f"{source_type}: duplicate column names after normalisation: {dupes}")
            missing = [c for c in contract.mandatory_columns if c not in header]
            if missing:
                raise SchemaValidationError(
                    f"{source_type}: unexpected schema change - missing required column(s) {missing}; "
                    f"found {header}"
                )
            extra = [c for c in header if c not in contract.columns]
            if extra:
                warnings.append(f"SCHEMA_DRIFT: unexpected column(s) {extra} ignored (kept in raw copy)")
            renamed = [f"{r}->{n}" for r, n in zip(header_raw, header) if r != n]
            if renamed:
                warnings.append(f"COLUMNS_NORMALISED: {renamed}")

            width = len(header)
            key_pos = header.index(contract.primary_key) if contract.primary_key in header else None
            rows: list[list[str]] = []
            row_numbers: list[int] = []
            source_lines: list[str] = []
            malformed: list[MalformedRecord] = []
            blank_lines = 0
            first_line = recorder.lines_consumed + 1
            for fields in reader:
                text = recorder.take()
                line_no, first_line = first_line, recorder.lines_consumed + 1
                if not fields:  # blank line: carries no data, counted and reported as a warning
                    blank_lines += 1
                    continue
                if len(fields) != width:
                    rid = fields[key_pos].strip() if key_pos is not None and key_pos < len(fields) else ""
                    malformed.append(MalformedRecord(line_no, text, f"expected {width} fields, found {len(fields)}",
                                                     None if rid in NULL_TOKENS else rid))
                    continue
                rows.append(fields)
                row_numbers.append(line_no)
                source_lines.append(text)
    except UnicodeDecodeError as exc:
        raise SchemaValidationError(f"{source_type}: file is not valid UTF-8 ({exc})") from exc
    except csv.Error as exc:
        raise SchemaValidationError(f"{source_type}: CSV could not be parsed ({exc})") from exc

    frame = pd.DataFrame(rows, columns=header, dtype=object)
    keep = [c for c in contract.columns if c in frame.columns]
    frame = frame[keep].copy()
    for column in keep:
        stripped = frame[column].str.strip()
        frame[column] = stripped.where(~stripped.isin(NULL_TOKENS), None)
    for column in contract.columns:
        if column not in frame.columns:
            frame[column] = None
    # The authoritative lineage is the actual file name we read.
    if "source_file" in header:
        mismatched = frame["source_file"].notna() & (frame["source_file"] != path.name)
        if mismatched.any():
            warnings.append(f"SOURCE_FILE_OVERRIDDEN: {int(mismatched.sum())} row(s) declared a different source_file")
    frame["source_file"] = path.name
    frame[ROW_NUMBER_COL] = pd.Series(row_numbers, dtype="int64")
    frame = frame[list(contract.columns) + [ROW_NUMBER_COL]]

    if not rows and not malformed:
        warnings.append("EMPTY_FILE: header present but no data rows")
    if blank_lines:
        warnings.append(f"BLANK_LINES: {blank_lines} empty line(s) contain no data and were skipped")

    return IngestedSource(
        source_type=source_type,
        path=path,
        file_name=path.name,
        file_hash=file_hash,
        size_bytes=size,
        header_raw=header_raw,
        header=header,
        data=frame.reset_index(drop=True),
        malformed=malformed,
        source_lines=source_lines,
        blank_lines=blank_lines,
        warnings=warnings,
        extra_columns=extra,
    )


def preserve_raw(source: IngestedSource, raw_dir: Path, run_id: str) -> Path:
    """Copy the untouched input file into the run's raw landing zone."""
    target_dir = Path(raw_dir) / run_id
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / f"{source.source_type}__{source.file_name}"
    shutil.copy2(source.path, target)
    return target
