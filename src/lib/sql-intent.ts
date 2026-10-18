/**
 * Mirrors the Rust `classify_sql_intent` / `is_read_only_sql` in
 * `src-tauri/src/commands/mod.rs`. Keep the two in sync: the backend enforces the
 * guard, this module powers the confirmation prompt before we send the query.
 */

export type SqlIntent =
  | 'select'
  | 'insert'
  | 'update'
  | 'delete'
  | 'explain'
  | 'show'
  | 'describe'
  | 'values'
  | 'table'
  | 'unknown'

export function classifySqlIntent(sql: string): SqlIntent {
  const normalized = sql.trimStart().toLowerCase()
  if (normalized.startsWith('select') || normalized.startsWith('with')) return 'select'
  if (normalized.startsWith('insert')) return 'insert'
  if (normalized.startsWith('update')) return 'update'
  if (normalized.startsWith('delete')) return 'delete'
  if (normalized.startsWith('explain')) return 'explain'
  if (normalized.startsWith('show')) return 'show'
  if (normalized.startsWith('describe') || normalized.startsWith('desc')) return 'describe'
  if (normalized.startsWith('values')) return 'values'
  if (normalized.startsWith('table ')) return 'table'
  return 'unknown'
}

const TRANSACTION_CONTROL = ['begin', 'commit', 'rollback', 'start', 'savepoint', 'release']

/**
 * Statements that can appear after a `WITH` clause and still modify data.
 * A data-modifying CTE (`WITH x AS (INSERT ...) SELECT ...`) starts with
 * `WITH`, so it must not be treated as read-only just because of its prefix.
 */
const WRITE_KEYWORDS = [
  'insert', 'update', 'delete', 'merge',
  'create', 'alter', 'drop', 'truncate',
  'grant', 'revoke', 'copy', 'call', 'do', 'vacuum',
]

function containsWriteKeyword(sql: string): boolean {
  const tokens = sql.toLowerCase().split(/[^a-z0-9_]+/)
  return tokens.some((token) => WRITE_KEYWORDS.includes(token))
}

/**
 * Redis commands that never modify the keyspace. Anything not listed here is
 * treated as a write so the editor asks for confirmation before running it.
 * Keep in sync with `REDIS_READ_ONLY_COMMANDS` in
 * `src-tauri/src/commands/mod.rs`.
 */
const REDIS_READ_ONLY_COMMANDS = new Set([
  'PING', 'ECHO', 'TYPE', 'EXISTS', 'TTL', 'PTTL', 'EXPIRETIME', 'PEXPIRETIME',
  'KEYS', 'SCAN', 'RANDOMKEY', 'DBSIZE',
  'GET', 'MGET', 'GETRANGE', 'STRLEN', 'SUBSTR', 'BITCOUNT', 'BITPOS', 'BITFIELD_RO',
  'HGET', 'HMGET', 'HGETALL', 'HKEYS', 'HVALS', 'HLEN', 'HEXISTS', 'HRANDFIELD', 'HSCAN',
  'LINDEX', 'LRANGE', 'LLEN', 'LPOS',
  'SMEMBERS', 'SCARD', 'SISMEMBER', 'SMISMEMBER', 'SRANDMEMBER', 'SSCAN',
  'ZCARD', 'ZCOUNT', 'ZDIFF', 'ZINTER', 'ZUNION', 'ZINTERCARD', 'ZLEXCOUNT',
  'ZMSCORE', 'ZRANDMEMBER', 'ZRANGE', 'ZRANGEBYLEX', 'ZRANGEBYSCORE', 'ZRANK',
  'ZREVRANGE', 'ZREVRANGEBYLEX', 'ZREVRANGEBYSCORE', 'ZREVRANK', 'ZSCAN', 'ZSCORE',
  'XINFO', 'XLEN', 'XRANGE', 'XREVRANGE', 'XPENDING', 'XREAD',
  'GEODIST', 'GEOHASH', 'GEOPOS', 'GEORADIUS_RO', 'GEORADIUSBYMEMBER_RO', 'GEOSEARCH',
  'PFCOUNT', 'INFO', 'TIME', 'LASTSAVE', 'COMMAND', 'MEMORY', 'OBJECT', 'SORT_RO',
])

/** True when every non-empty line is a read-only Redis command. */
export function isReadOnlyRedisCommand(sql: string): boolean {
  const commands = sql
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0 && !line.startsWith('#'))
  if (commands.length === 0) return false
  return commands.every((line) => {
    const command = line.split(/\s+/)[0]?.toUpperCase() ?? ''
    return REDIS_READ_ONLY_COMMANDS.has(command)
  })
}

/** True when every statement in `sql` is read-only (select/explain/show/...). */
export function isReadOnlySql(sql: string, engine?: string): boolean {
  // MongoDB queries are read-only in practice: the engine only executes find().
  if (engine === 'mongo') return true
  if (engine === 'redis') return isReadOnlyRedisCommand(sql)
  let sawStatement = false
  for (const raw of sql.split(';')) {
    const statement = raw.trim()
    if (!statement) continue
    const normalized = statement.toLowerCase()
    if (TRANSACTION_CONTROL.some((kw) => normalized.startsWith(kw))) continue
    sawStatement = true
    const intent = classifySqlIntent(statement)
    if (intent === 'select') {
      // WITH-prefixed statements can be data-modifying CTEs.
      if (normalized.startsWith('with') && containsWriteKeyword(statement)) return false
      continue
    }
    // EXPLAIN ANALYZE actually executes the statement it explains.
    if (intent === 'explain') {
      if (containsWriteKeyword(statement)) return false
      continue
    }
    if (intent === 'show' || intent === 'describe' || intent === 'values' || intent === 'table') {
      continue
    }
    return false
  }
  return sawStatement
}
