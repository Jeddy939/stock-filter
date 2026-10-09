import type {NextFunction, Request, Response} from "express";

interface RateLimitEntry {
  windowStartedAt: number;
  count: number;
  blockedUntil: number;
  lastSeenAt: number;
  blockLogged: boolean;
}

export interface RateLimitDecision {
  allowed: boolean;
  limit: number;
  remaining: number;
  retryAfterSeconds: number;
  firstBlockedRequest: boolean;
}

interface IpRateLimiterOptions {
  limit: number;
  windowMs: number;
  blockMs: number;
  maxEntries: number;
}

const OVERFLOW_KEY = "__overflow__";

export class IpRateLimiter {
  private readonly entries = new Map<string, RateLimitEntry>();
  private lastCleanupAt = 0;

  constructor(private readonly options: IpRateLimiterOptions) {}

  consume(rawKey: string, now = Date.now()): RateLimitDecision {
    this.cleanup(now);
    const normalizedKey = String(rawKey || "unknown").trim().slice(0, 128) || "unknown";
    const key = this.entries.has(normalizedKey) || this.entries.size < this.options.maxEntries
      ? normalizedKey
      : OVERFLOW_KEY;
    let entry = this.entries.get(key);
    if (!entry) {
      entry = {
        windowStartedAt: now,
        count: 0,
        blockedUntil: 0,
        lastSeenAt: now,
        blockLogged: false
      };
      this.entries.set(key, entry);
    }
    entry.lastSeenAt = now;

    if (entry.blockedUntil > now) {
      return this.blockedDecision(entry, now, false);
    }
    if (now - entry.windowStartedAt >= this.options.windowMs) {
      entry.windowStartedAt = now;
      entry.count = 0;
      entry.blockedUntil = 0;
      entry.blockLogged = false;
    }

    entry.count += 1;
    if (entry.count > this.options.limit) {
      entry.blockedUntil = now + this.options.blockMs;
      const firstBlockedRequest = !entry.blockLogged;
      entry.blockLogged = true;
      return this.blockedDecision(entry, now, firstBlockedRequest);
    }
    return {
      allowed: true,
      limit: this.options.limit,
      remaining: Math.max(0, this.options.limit - entry.count),
      retryAfterSeconds: 0,
      firstBlockedRequest: false
    };
  }

  private blockedDecision(entry: RateLimitEntry, now: number, firstBlockedRequest: boolean): RateLimitDecision {
    return {
      allowed: false,
      limit: this.options.limit,
      remaining: 0,
      retryAfterSeconds: Math.max(1, Math.ceil((entry.blockedUntil - now) / 1000)),
      firstBlockedRequest
    };
  }

  private cleanup(now: number): void {
    if (now - this.lastCleanupAt < this.options.windowMs) return;
    this.lastCleanupAt = now;
    const retentionMs = Math.max(this.options.blockMs * 2, this.options.windowMs * 2);
    for (const [key, entry] of this.entries) {
      if (now - entry.lastSeenAt > retentionMs) this.entries.delete(key);
    }
  }
}

function positiveInteger(value: unknown, fallback: number): number {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback;
}

export function requestClientIp(req: Request): string {
  const forwarded = String(req.header("x-forwarded-for") ?? "").trim();
  if (forwarded) return forwarded.split(",")[0].trim();
  return String(req.socket.remoteAddress ?? req.ip ?? "unknown").trim();
}

const requestLimit = positiveInteger(process.env.MONEYMAKER_IP_RATE_LIMIT_PER_MINUTE, 300);
const blockSeconds = positiveInteger(process.env.MONEYMAKER_IP_RATE_LIMIT_BLOCK_SECONDS, 600);
const limiter = new IpRateLimiter({
  limit: requestLimit,
  windowMs: 60_000,
  blockMs: blockSeconds * 1000,
  maxEntries: positiveInteger(process.env.MONEYMAKER_IP_RATE_LIMIT_MAX_IPS, 10_000)
});

export function ipRateLimit(req: Request, res: Response, next: NextFunction): void {
  const clientIp = requestClientIp(req);
  const decision = limiter.consume(clientIp);
  res.setHeader("RateLimit-Limit", String(decision.limit));
  res.setHeader("RateLimit-Remaining", String(decision.remaining));
  if (decision.allowed) {
    next();
    return;
  }

  res.setHeader("Retry-After", String(decision.retryAfterSeconds));
  res.setHeader("Cache-Control", "no-store");
  if (decision.firstBlockedRequest) {
    console.warn("IP request limit exceeded", {
      path: req.path,
      retry_after_seconds: decision.retryAfterSeconds
    });
  }
  res.status(429).json({
    ok: false,
    error: "Too many requests",
    detail: `This connection has been temporarily limited. Try again in ${decision.retryAfterSeconds} seconds.`
  });
}
