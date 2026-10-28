// Weekly snapshot files are stored gzip-compressed and never change once
// published (everything under snapshots/versions/). Serving the stored bytes
// as-is avoids decompressing, parsing and re-encoding them on every request,
// and lets the browser keep them for the rest of the week.

export const SNAPSHOT_VERSION_PREFIX = "snapshots/versions/";
const CACHE_LIMIT_BYTES = 96 * 1024 * 1024;

export function isGzip(raw: Buffer): boolean {
  return raw.length >= 2 && raw[0] === 0x1f && raw[1] === 0x8b;
}

export function snapshotCacheControl(objectPath: string): string {
  return objectPath.startsWith(SNAPSHOT_VERSION_PREFIX)
    ? "private, max-age=31536000, immutable"
    : "private, no-cache";
}

/** Least-recently-used cache of immutable snapshot files, bounded by total bytes. */
export class SnapshotFileCache {
  private readonly entries = new Map<string, Buffer>();
  private bytes = 0;

  constructor(private readonly limitBytes = CACHE_LIMIT_BYTES) {}

  get(objectPath: string): Buffer | undefined {
    const raw = this.entries.get(objectPath);
    if (!raw) return undefined;
    this.entries.delete(objectPath);
    this.entries.set(objectPath, raw);
    return raw;
  }

  set(objectPath: string, raw: Buffer): void {
    if (!objectPath.startsWith(SNAPSHOT_VERSION_PREFIX) || raw.length > this.limitBytes / 4) return;
    const existing = this.entries.get(objectPath);
    if (existing) {
      this.bytes -= existing.length;
      this.entries.delete(objectPath);
    }
    this.entries.set(objectPath, raw);
    this.bytes += raw.length;
    for (const [key, value] of this.entries) {
      if (this.bytes <= this.limitBytes) break;
      this.entries.delete(key);
      this.bytes -= value.length;
    }
  }

  get size(): number {
    return this.entries.size;
  }

  get totalBytes(): number {
    return this.bytes;
  }
}
