import type {Request} from "express";
import {getApps, initializeApp} from "firebase-admin/app";
import {getAppCheck} from "firebase-admin/app-check";
import {getAuth, type DecodedIdToken} from "firebase-admin/auth";
import {FieldValue, getFirestore} from "firebase-admin/firestore";
import type {Pool} from "pg";

if (!getApps().length) {
  initializeApp();
}

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

export interface UserContext {
  uid: string;
  email: string | null;
  display_name: string | null;
  role: "admin" | "analyst" | "viewer";
  status: string;
}

type FirebaseTokenVerifier = (token: string) => Promise<DecodedIdToken>;

function authRequired(): boolean {
  return ["1", "true", "yes"].includes(String(process.env.MONEYMAKER_REQUIRE_AUTH ?? "true").toLowerCase());
}

export async function requireAuth(
  req: Request,
  pool: Pool,
  verifyToken: FirebaseTokenVerifier = (token) => getAuth().verifyIdToken(token)
): Promise<UserContext> {
  if (!authRequired()) {
    return {uid: "local", email: null, display_name: "Local user", role: "admin", status: "active"};
  }

  const header = String(req.header("authorization") ?? "");
  if (!header.startsWith("Bearer ")) {
    throw new ApiError(401, "Sign-in required");
  }

  let decoded: DecodedIdToken;
  try {
    decoded = await verifyToken(header.slice(7).trim());
  } catch (_error) {
    throw new ApiError(401, "Invalid Firebase ID token");
  }

  const uid = String(decoded.uid ?? "").trim();
  if (!uid) throw new ApiError(401, "Invalid Firebase ID token");

  const email = String(decoded.email ?? "").trim().toLowerCase() || null;
  const displayName = String(decoded.name ?? decoded.email ?? "").trim() || null;

  if (!email) {
    throw new ApiError(403, "Invited email account required");
  }

  try {
    const profileSnapshot = await getFirestore().collection("app_users").doc(uid).get();
    if (profileSnapshot.exists) {
      const profile = profileSnapshot.data() ?? {};
      const profileEmail = String(profile.email ?? email).trim().toLowerCase();
      if (profileEmail !== email) throw new ApiError(403, "This account profile does not match the signed-in email");
      const status = String(profile.status ?? "active").toLowerCase();
      if (status !== "active") throw new ApiError(403, "This account is disabled");
      return {
        uid,
        email,
        display_name: String(profile.display_name ?? displayName ?? email),
        role: normalizeRole(profile.role),
        status
      };
    }
  } catch (error) {
    if (error instanceof ApiError) throw error;
    console.warn("Firestore user profile lookup failed; falling back to PostgreSQL", error);
  }

  try {
    const inviteResult = await pool.query(
      "SELECT role, status FROM app_user_invites WHERE email = $1",
      [email]
    );
    const invite = inviteResult.rows[0] as {role?: string; status?: string} | undefined;
    if (!invite) {
      throw new ApiError(403, "This app is invite-only");
    }
    if (invite?.status === "disabled") {
      throw new ApiError(403, "This account is disabled");
    }
    const role = normalizeRole(invite?.role);

    const profileResult = await pool.query(
      `
      INSERT INTO user_profiles
        (firebase_uid, email, display_name, role, status, last_seen_at_utc)
      VALUES ($1, $2, $3, $4, 'active', NOW())
      ON CONFLICT (firebase_uid) DO UPDATE SET
        email = EXCLUDED.email,
        display_name = EXCLUDED.display_name,
        role = EXCLUDED.role,
        status = user_profiles.status,
        last_seen_at_utc = NOW()
      RETURNING role, status
      `,
      [uid, email, displayName, role]
    );
    const profile = profileResult.rows[0] as {role?: string; status?: string} | undefined;
    const status = String(profile?.status ?? "active").toLowerCase();
    if (status !== "active") {
      throw new ApiError(403, "This account is disabled");
    }
    const user = {uid, email, display_name: displayName, role: normalizeRole(profile?.role ?? role), status};
    await getFirestore().collection("app_users").doc(uid).set({
      email,
      display_name: displayName,
      role: user.role,
      status,
      migrated_at: FieldValue.serverTimestamp()
    }, {merge: true});
    return user;
  } catch (error) {
    if (error instanceof ApiError) throw error;
    console.error("Authentication profile lookup failed", error);
    throw new ApiError(503, "Database temporarily unavailable");
  }
}

function normalizeRole(role: unknown): UserContext["role"] {
  const value = String(role ?? "").toLowerCase();
  if (value === "owner" || value === "admin") return "admin";
  if (value === "member" || value === "analyst") return "analyst";
  return "viewer";
}

const ROLE_RANK: Record<UserContext["role"], number> = {
  viewer: 1,
  analyst: 2,
  admin: 3
};

export function requireRole(user: UserContext, role: UserContext["role"]): void {
  if (ROLE_RANK[user.role] < ROLE_RANK[role]) {
    throw new ApiError(403, `${role} access required`);
  }
}

export function requireAdmin(user: UserContext): void {
  requireRole(user, "admin");
}

export function requireAnalyst(user: UserContext): void {
  requireRole(user, "analyst");
}

export async function requireAppCheck(req: Request): Promise<void> {
  const required = ["1", "true", "yes"].includes(String(process.env.MONEYMAKER_REQUIRE_APP_CHECK ?? "false").toLowerCase());
  if (!required) return;

  const token = String(req.header("X-Firebase-AppCheck") ?? "").trim();
  if (!token) {
    throw new ApiError(401, "App Check token required");
  }

  try {
    await getAppCheck().verifyToken(token);
  } catch (error) {
    throw new ApiError(401, "Invalid App Check token");
  }
}
