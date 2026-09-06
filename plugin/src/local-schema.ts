/**
 * Live SQLite schema for the local opencode.db.
 *
 * OpenCode adds columns without a plugin release. Hardcoded INSERT lists
 * then fail (`no column named data`) or silently drop new fields. Read
 * PRAGMA table_info and only write columns that exist on this machine.
 */
import type { Database } from "bun:sqlite";

export type SqliteColumn = {
  readonly name: string;
  readonly notNull: boolean;
  readonly pk: number;
  readonly dfltValue: string | null;
};

export type TableSchema = {
  readonly columns: readonly SqliteColumn[];
  readonly columnNames: ReadonlySet<string>;
  readonly pkColumns: readonly string[];
};

type PragmaRow = {
  name: string;
  notnull: number;
  dflt_value: string | null;
  pk: number;
};

export function quoteIdent(name: string): string {
  return `"${name.replaceAll('"', '""')}"`;
}

export function readTableSchema(db: Database, table: string): TableSchema | null {
  const rows = db.query<PragmaRow, []>(`PRAGMA table_info(${quoteIdent(table)})`).all();
  if (rows.length === 0) return null;

  const columns: SqliteColumn[] = rows.map((row) => ({
    name: row.name,
    notNull: row.notnull === 1,
    pk: row.pk,
    dfltValue: row.dflt_value,
  }));

  const pkColumns = columns
    .filter((col) => col.pk > 0)
    .sort((a, b) => a.pk - b.pk)
    .map((col) => col.name);

  return {
    columns,
    columnNames: new Set(columns.map((col) => col.name)),
    pkColumns,
  };
}

export function requiredColumnMissing(
  schema: TableSchema,
  data: Record<string, unknown>,
): boolean {
  for (const col of schema.columns) {
    if (!col.notNull || col.dfltValue !== null) continue;
    const value = data[col.name];
    if (value === undefined || value === null) return true;
  }
  return false;
}

export function writableColumns(
  schema: TableSchema,
  data: Record<string, unknown>,
): string[] {
  return schema.columns
    .map((col) => col.name)
    .filter((name) => {
      if (!Object.prototype.hasOwnProperty.call(data, name)) return false;
      return data[name] !== undefined;
    });
}

export function schemaSignature(schema: TableSchema): string {
  return schema.columns
    .map((col) => `${col.name}${col.notNull && col.dfltValue === null ? "!" : ""}`)
    .join(",");
}
