# MoneyMaker SQL Connect

This service is connected to the existing Cloud SQL database `moneymaker`.
The database is brownfield: `firebase/migrations/001_schema.sql` owns the
PostgreSQL tables, constraints, defaults, indexes, and SQL grants.

Do not run `firebase dataconnect:sql:migrate` or deploy Data Connect migrations
blindly. The Firebase SQL Connect schema can compile against the current tables,
but `firebase dataconnect:sql:diff` currently proposes destructive compatibility
changes, including dropping defaults, check constraints, foreign keys, and
indexes that the Functions and Cloud Run workers rely on.

Use this safe check first:

```powershell
.\scripts\deploy_firebase_native.ps1 -DataConnect
```

That command performs a dry-run and stops if SQL migrations are detected. Only
use `-AllowDataConnectMigrations` after reviewing the generated SQL and deciding
those database changes are intentional.

The checked-in migration grants Firebase SQL Connect reader/writer roles access
to the app tables and sequences when those roles exist. This lets SQL Connect
operations read/write the brownfield tables without transferring schema
ownership away from the app migration.
