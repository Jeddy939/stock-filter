const APP_CHECK_EXEMPT_PATHS = new Set(["/scheduled-fetch"]);

export function requiresAppCheck(path: string): boolean {
  const normalized = `/${String(path || "").replace(/^\/+|\/+$/g, "")}`;
  return !APP_CHECK_EXEMPT_PATHS.has(normalized);
}
