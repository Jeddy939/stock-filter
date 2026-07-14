# MoneyMaker SQL Connect

This service is connected to the existing Cloud SQL database `moneymaker`.
The database is brownfield: `firebase/migrations/001_schema.sql` owns the
PostgreSQL stock/app tables, constraints, defaults, indexes, and SQL grants.

The SQL Connect schema intentionally contains only service-owned metadata.
The app-facing queries and mutations use SQL Connect native SQL operations to
read/write the brownfield tables. This avoids letting SQL Connect strip
production defaults, check constraints, foreign keys, or indexes that the
Functions and Cloud Run workers rely on.

Do not run `firebase dataconnect:sql:migrate` blindly. In compatible mode, the
database must be compatible with the SQL Connect schema. Optional "match
exactly" output may still mention dropping brownfield app tables; those strict
mode changes are not part of the normal deployment path.

Use this safe check first:

```powershell
.\scripts\deploy_firebase_native.ps1 -DataConnect
```

That command performs a dry-run first. It stops only if the compatible plan
requires SQL migrations. Only use `-AllowDataConnectMigrations` after reviewing
the generated SQL and deciding those database changes are intentional.

The checked-in migration grants Firebase SQL Connect reader/writer roles access
to the app tables and sequences when those roles exist. This lets SQL Connect
operations read/write the brownfield tables without transferring schema
ownership away from the app migration.
