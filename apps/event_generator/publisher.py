from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4


class Publisher(Protocol):
    def publish(self, event_type: str, payload: dict[str, object]) -> None: ...

    def flush(self) -> None: ...


def _normalize_host(host: str) -> str:
    return host.rstrip("/")


def _normalize_volume_path(volume_path: str) -> str:
    # Accepts a UC volume path like "/Volumes/<catalog>/<schema>/<volume>[/...]".
    cleaned = volume_path.strip().rstrip("/")
    if not cleaned.startswith("/Volumes/"):
        raise ValueError("DATABRICKS_VOLUME_PATH must start with /Volumes/...")
    return cleaned


def _path_join(root: str, *parts: str) -> str:
    prefix = root.rstrip("/")
    cleaned_parts = [part.strip("/").replace("\\", "/") for part in parts if part]
    return "/".join([prefix, *cleaned_parts])


def _normalize_dataset_name(name: str) -> str:
    cleaned = name.strip().replace("\\", "/").strip("/")
    if not cleaned:
        raise ValueError("Dataset name cannot be empty.")
    return cleaned


@dataclass
class _DatabricksFilesClient:
    host: str
    token: str

    def __post_init__(self) -> None:
        self.host = _normalize_host(self.host)

    def _request_bytes(self, *, method: str, path: str, body: bytes, content_type: str) -> None:
        url = f"{self.host}{path}"
        req = Request(
            url=url,
            data=body,
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": content_type,
            },
        )
        try:
            with urlopen(req, timeout=60) as resp:
                resp.read()
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Databricks API error {exc.code} for {path}: {detail}") from exc

    def mkdirs(self, volume_path: str) -> None:
        # Unity Catalog volumes are managed via the Files API (not DBFS API).
        normalized = _normalize_volume_path(volume_path).lstrip("/")
        if not normalized.endswith("/"):
            normalized = f"{normalized}/"
        self._request_bytes(
            method="PUT",
            path=f"/api/2.0/fs/directories/{normalized}",
            body=b"",
            content_type="application/octet-stream",
        )

    def put_file(self, *, volume_file_path: str, contents: bytes, overwrite: bool = True) -> None:
        normalized = _normalize_volume_path(volume_file_path).lstrip("/")
        overwrite_qs = "true" if overwrite else "false"
        self._request_bytes(
            method="PUT",
            path=f"/api/2.0/fs/files/{normalized}?overwrite={overwrite_qs}",
            body=contents,
            content_type="application/octet-stream",
        )


@dataclass
class DatabricksVolumePublisher:
    host: str
    token: str
    volume_path: str
    _client: _DatabricksFilesClient = field(init=False)
    _buffers: dict[str, list[dict[str, object]]] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self._client = _DatabricksFilesClient(host=self.host, token=self.token)
        # `DATABRICKS_VOLUME_PATH` is treated as the write root.
        self._dataset_root = _normalize_volume_path(self.volume_path)

    def publish(self, event_type: str, payload: dict[str, object]) -> None:
        dataset_name = _normalize_dataset_name(event_type)
        self._buffers.setdefault(dataset_name, []).append(payload)

    def publish_records(self, *, dataset_name: str, records: list[dict[str, object]]) -> str:
        if not records:
            raise ValueError(f"Cannot publish empty dataset: {dataset_name}")

        now = datetime.now(UTC)
        ingest_date = now.date().isoformat()
        dataset_dir = _path_join(self._dataset_root, dataset_name, f"ingest_date={ingest_date}")
        self._client.mkdirs(dataset_dir)
        filename = f"snapshot_{now.strftime('%Y%m%dT%H%M%S%fZ')}_{uuid4().hex}.jsonl"
        file_path = _path_join(dataset_dir, filename)
        contents = (
            "\n".join(json.dumps(record, ensure_ascii=True, separators=(",", ":")) for record in records) + "\n"
        ).encode("utf-8")
        self._client.put_file(volume_file_path=file_path, contents=contents, overwrite=True)
        return file_path

    def flush(self) -> None:
        if not self._buffers:
            return

        now = datetime.now(UTC)
        ingest_date = now.date().isoformat()
        ingest_hour = now.strftime("%H")
        for dataset_name, records in self._buffers.items():
            if not records:
                continue

            batch_id = uuid4().hex
            dataset_dir = _path_join(self._dataset_root, dataset_name, f"dt={ingest_date}", f"hour={ingest_hour}")
            self._client.mkdirs(dataset_dir)
            filename = f"batch_{now.strftime('%Y%m%dT%H%M%S%fZ')}_{batch_id}.jsonl"
            file_path = _path_join(dataset_dir, filename)
            contents = (
                "\n".join(json.dumps(event, ensure_ascii=True, separators=(",", ":")) for event in records) + "\n"
            ).encode("utf-8")
            self._client.put_file(volume_file_path=file_path, contents=contents, overwrite=True)

            # Minimal signal for logs without leaking credentials.
            print(f"wrote 1 file with event batch to {self._dataset_root}/{dataset_name}/dt={ingest_date}/hour={ingest_hour}")

        self._buffers.clear()
