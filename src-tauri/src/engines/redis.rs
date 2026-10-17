use std::collections::BTreeMap;
use std::time::Instant;

use tauri::AppHandle;

use crate::db::{build_redis_url, get_or_create_redis_client, AppState};
use crate::error::VeloxError;
use crate::models::{ColumnInfo, ConnectionInput, DatabaseInfo, QueryResult, TableInfo};

use super::DatabaseEngineOps;

pub struct RedisEngine;

/// Quote a value so it survives the Redis argument parser used below.
fn redis_arg(value: &str) -> String {
    if value.is_empty()
        || value
            .chars()
            .any(|c| c.is_whitespace() || c == '"' || c == '\'' || c == '\\')
    {
        format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
    } else {
        value.to_string()
    }
}

/// Split a Redis command line into arguments, honouring single/double quotes
/// and backslash escapes (for example `SET greeting "hello world"`).
fn split_redis_args(input: &str) -> Result<Vec<String>, VeloxError> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut has_token = false;
    let mut quote: Option<char> = None;
    let mut escaped = false;

    for ch in input.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }
        match quote {
            Some(q) => {
                if ch == '\\' {
                    escaped = true;
                } else if ch == q {
                    quote = None;
                } else {
                    current.push(ch);
                }
            }
            None => {
                if ch == '\'' || ch == '"' {
                    quote = Some(ch);
                    has_token = true;
                } else if ch.is_whitespace() {
                    if has_token {
                        args.push(std::mem::take(&mut current));
                        has_token = false;
                    }
                } else {
                    current.push(ch);
                    has_token = true;
                }
            }
        }
    }

    if quote.is_some() {
        return Err(VeloxError::Validation(
            "Unterminated quote in the Redis command.".to_string(),
        ));
    }
    if has_token {
        args.push(current);
    }
    Ok(args)
}

fn redis_scalar_to_string(value: &redis::Value) -> Option<String> {
    match value {
        redis::Value::Nil => None,
        redis::Value::Int(n) => Some(n.to_string()),
        redis::Value::Data(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
        redis::Value::Status(status) => Some(status.clone()),
        redis::Value::Okay => Some("OK".to_string()),
        redis::Value::Bulk(items) => Some(format!(
            "[{}]",
            items
                .iter()
                .filter_map(redis_scalar_to_string)
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

fn single_value_result(value: Option<String>, elapsed_ms: u128) -> QueryResult {
    QueryResult {
        columns: vec!["result".to_string()],
        rows: vec![BTreeMap::from([("result".to_string(), value)])],
        row_count: 1,
        execution_ms: elapsed_ms,
        truncated: false,
        command_tag: None,
    }
}

/// Convert a RESP2 reply into the tabular shape the results grid expects.
fn redis_result_to_query_result(
    command: &str,
    value: redis::Value,
    elapsed_ms: u128,
    max_rows: usize,
) -> QueryResult {
    match value {
        redis::Value::Bulk(items) if items.is_empty() => QueryResult {
            columns: vec!["value".to_string()],
            rows: vec![],
            row_count: 0,
            execution_ms: elapsed_ms,
            truncated: false,
            command_tag: None,
        },
        redis::Value::Bulk(items) => {
            // HGETALL replies with a flat [field, value, field, value, ...] array.
            if command == "HGETALL" && items.len() % 2 == 0 {
                let mut rows = Vec::new();
                for pair in items.chunks(2) {
                    if let [field, value] = pair {
                        let field = redis_scalar_to_string(field).unwrap_or_default();
                        let value = redis_scalar_to_string(value);
                        rows.push(BTreeMap::from([
                            ("field".to_string(), Some(field)),
                            ("value".to_string(), value),
                        ]));
                    }
                }
                let truncated = rows.len() > max_rows;
                rows.truncate(max_rows);
                return QueryResult {
                    columns: vec!["field".to_string(), "value".to_string()],
                    row_count: rows.len(),
                    rows,
                    execution_ms: elapsed_ms,
                    truncated,
                    command_tag: None,
                };
            }

            // SCAN-family replies are [cursor, [items...]].
            if matches!(command, "SCAN" | "HSCAN" | "SSCAN" | "ZSCAN") && items.len() == 2 {
                let cursor = redis_scalar_to_string(&items[0]).unwrap_or_default();
                let values = match &items[1] {
                    redis::Value::Bulk(values) => values
                        .iter()
                        .filter_map(redis_scalar_to_string)
                        .collect::<Vec<_>>()
                        .join(", "),
                    other => redis_scalar_to_string(other).unwrap_or_default(),
                };
                return QueryResult {
                    columns: vec!["cursor".to_string(), "values".to_string()],
                    rows: vec![BTreeMap::from([
                        ("cursor".to_string(), Some(cursor)),
                        ("values".to_string(), Some(values)),
                    ])],
                    row_count: 1,
                    execution_ms: elapsed_ms,
                    truncated: false,
                    command_tag: None,
                };
            }

            // LRANGE replies with an ordered list of values: surface the index.
            if command == "LRANGE" {
                let truncated = items.len() > max_rows;
                let rows: Vec<BTreeMap<String, Option<String>>> = items
                    .iter()
                    .take(max_rows)
                    .enumerate()
                    .map(|(index, item)| {
                        BTreeMap::from([
                            ("index".to_string(), Some(index.to_string())),
                            ("value".to_string(), redis_scalar_to_string(item)),
                        ])
                    })
                    .collect();
                return QueryResult {
                    columns: vec!["index".to_string(), "value".to_string()],
                    row_count: rows.len(),
                    rows,
                    execution_ms: elapsed_ms,
                    truncated,
                    command_tag: None,
                };
            }

            let truncated = items.len() > max_rows;
            let rows: Vec<BTreeMap<String, Option<String>>> = items
                .iter()
                .take(max_rows)
                .map(|item| BTreeMap::from([("value".to_string(), redis_scalar_to_string(item))]))
                .collect();
            QueryResult {
                columns: vec!["value".to_string()],
                row_count: rows.len(),
                rows,
                execution_ms: elapsed_ms,
                truncated,
                command_tag: None,
            }
        }
        other => single_value_result(redis_scalar_to_string(&other), elapsed_ms),
    }
}

/// When a browser previews a key it issues `GET <key>`. Hashes, lists, sets,
/// sorted sets and streams are not readable with GET, so transparently route
/// those keys to the matching read command.
async fn resolve_generic_get(
    client: &mut redis::aio::ConnectionManager,
    args: &mut Vec<String>,
) -> Result<(), VeloxError> {
    if args.len() != 2 || !args[0].eq_ignore_ascii_case("GET") {
        return Ok(());
    }
    let key = args[1].clone();
    let key_type: String = redis::cmd("TYPE")
        .arg(&key)
        .query_async(client)
        .await
        .map_err(|e| VeloxError::Query(format!("Redis TYPE failed: {}", e)))?;
    let replacement: Option<Vec<String>> = match key_type.as_str() {
        "hash" => Some(vec!["HGETALL".into(), key]),
        "list" => Some(vec!["LRANGE".into(), key, "0".into(), "-1".into()]),
        "set" => Some(vec!["SMEMBERS".into(), key]),
        "zset" => Some(vec!["ZRANGE".into(), key, "0".into(), "-1".into(), "WITHSCORES".into()]),
        "stream" => Some(vec!["XRANGE".into(), key, "-".into(), "+".into()]),
        _ => None,
    };
    if let Some(replacement) = replacement {
        *args = replacement;
    }
    Ok(())
}

impl DatabaseEngineOps for RedisEngine {
    async fn connect(
        &self,
        _app: &AppHandle,
        state: &AppState,
        input: &ConnectionInput,
        connection_id: &str,
    ) -> Result<(), VeloxError> {
        let url = build_redis_url(input);
        let client = redis::Client::open(url)
            .map_err(|e| VeloxError::Connection(format!("Redis connection failed: {}", e)))?;
        let mut conn = redis::aio::ConnectionManager::new(client).await
            .map_err(|e| VeloxError::Connection(format!("Redis connection failed: {}", e)))?;
        redis::cmd("PING").query_async::<_, String>(&mut conn).await
            .map_err(|e| VeloxError::Connection(format!("Redis ping failed: {}", e)))?;
        state.redis_clients.write().await.insert(connection_id.to_string(), conn);
        Ok(())
    }

    async fn ping(
        &self,
        app: &AppHandle,
        state: &AppState,
        connection_id: &str,
    ) -> Result<(), VeloxError> {
        let mut client = get_or_create_redis_client(app, state, connection_id).await?;
        redis::cmd("PING").query_async::<_, String>(&mut client).await
            .map_err(|e| VeloxError::Connection(format!("Redis ping failed: {}", e)))?;
        Ok(())
    }

    async fn run_query(
        &self,
        app: &AppHandle,
        state: &AppState,
        connection_id: &str,
        sql: &str,
        max_rows: usize,
    ) -> Result<QueryResult, VeloxError> {
        let mut client = get_or_create_redis_client(app, state, connection_id).await?;
        let started_at = Instant::now();

        let mut result: Option<QueryResult> = None;
        let mut executed = 0usize;

        for line in sql.lines().map(str::trim) {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut args = split_redis_args(line)?;
            if args.is_empty() {
                continue;
            }
            resolve_generic_get(&mut client, &mut args).await?;

            let command = args[0].to_ascii_uppercase();
            let mut redis_command = redis::cmd(&command);
            for arg in &args[1..] {
                redis_command.arg(arg);
            }
            let value: redis::Value = redis_command
                .query_async(&mut client)
                .await
                .map_err(|e| VeloxError::Query(format!("Redis {} failed: {}", command, e)))?;

            result = Some(redis_result_to_query_result(
                &command,
                value,
                started_at.elapsed().as_millis(),
                max_rows,
            ));
            executed += 1;
        }

        if executed == 0 {
            return Err(VeloxError::Validation(
                "Enter a Redis command before running the query.".to_string(),
            ));
        }

        result.ok_or_else(|| {
            VeloxError::Query("Redis returned no result for the command.".to_string())
        })
    }

    async fn get_tables(
        &self,
        app: &AppHandle,
        state: &AppState,
        connection_id: &str,
    ) -> Result<Vec<TableInfo>, VeloxError> {
        let mut client = get_or_create_redis_client(app, state, connection_id).await?;
        let schema = crate::db::load_connection(app, connection_id)?
            .map(|connection| connection.database)
            .filter(|database| !database.trim().is_empty())
            .unwrap_or_else(|| "0".to_string());
        let keys: Vec<String> = redis::cmd("KEYS").arg("*").query_async(&mut client).await
            .map_err(|e| VeloxError::Query(format!("Redis KEYS failed: {}", e)))?;
        Ok(keys.into_iter().map(|k| TableInfo {
            schema: schema.clone(),
            name: k.clone(),
            preview_query: format!("GET {}", redis_arg(&k)),
            kind: Some("table".to_string()),
        }).collect())
    }

    async fn get_schema(
        &self,
        app: &AppHandle,
        state: &AppState,
        connection_id: &str,
        table_schema: &str,
        table_name: &str,
    ) -> Result<Vec<ColumnInfo>, VeloxError> {
        let mut client = get_or_create_redis_client(app, state, connection_id).await?;
        let schema = if table_schema.trim().is_empty() { "0" } else { table_schema };

        let key_type: String = redis::cmd("TYPE").arg(table_name).query_async(&mut client)
            .await.map_err(|e| VeloxError::Query(format!("Redis TYPE failed: {}", e)))?;

        match key_type.as_str() {
            "string" => Ok(vec![ColumnInfo {
                table_schema: schema.to_string(), table_name: table_name.to_string(),
                column_name: "value".to_string(), data_type: "string".to_string(), is_nullable: true,
            }]),
            "hash" => {
                let fields: Vec<String> = redis::cmd("HKEYS").arg(table_name).query_async(&mut client)
                    .await.map_err(|e| VeloxError::Query(format!("Redis HKEYS failed: {}", e)))?;
                Ok(fields.into_iter().map(|f| ColumnInfo {
                    table_schema: schema.to_string(), table_name: table_name.to_string(),
                    column_name: f, data_type: "string".to_string(), is_nullable: true,
                }).collect())
            }
            "list" => Ok(vec![
                ColumnInfo { table_schema: schema.to_string(), table_name: table_name.to_string(), column_name: "index".to_string(), data_type: "integer".to_string(), is_nullable: false },
                ColumnInfo { table_schema: schema.to_string(), table_name: table_name.to_string(), column_name: "value".to_string(), data_type: "string".to_string(), is_nullable: true },
            ]),
            "set" => Ok(vec![ColumnInfo {
                table_schema: schema.to_string(), table_name: table_name.to_string(),
                column_name: "member".to_string(), data_type: "string".to_string(), is_nullable: false,
            }]),
            _ => Ok(vec![ColumnInfo {
                table_schema: schema.to_string(), table_name: table_name.to_string(),
                column_name: "value".to_string(), data_type: key_type, is_nullable: true,
            }]),
        }
    }

    async fn list_databases(
        &self,
        app: &AppHandle,
        state: &AppState,
        connection_id: &str,
    ) -> Result<Vec<DatabaseInfo>, VeloxError> {
        let mut client = get_or_create_redis_client(app, state, connection_id).await?;
        // Redis exposes the number of logical databases through CONFIG GET.
        // Managed providers may disable CONFIG; fall back to the default 16.
        let config: Vec<String> = redis::cmd("CONFIG")
            .arg("GET")
            .arg("databases")
            .query_async(&mut client)
            .await
            .unwrap_or_default();
        let count = config
            .get(1)
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|count| *count > 0)
            .unwrap_or(16);
        Ok((0..count)
            .map(|index| DatabaseInfo { name: index.to_string() })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::{redis_arg, redis_result_to_query_result, split_redis_args};

    #[test]
    fn splits_quoted_arguments() {
        assert_eq!(
            split_redis_args("SET greeting \"hello world\"").unwrap(),
            vec!["SET", "greeting", "hello world"]
        );
        assert_eq!(
            split_redis_args("SET greeting 'hello world'").unwrap(),
            vec!["SET", "greeting", "hello world"]
        );
        assert_eq!(split_redis_args("GET \"\"").unwrap(), vec!["GET", ""]);
        assert!(split_redis_args("SET key \"unterminated").is_err());
    }

    #[test]
    fn quotes_preview_keys_when_needed() {
        assert_eq!(redis_arg("user:1"), "user:1");
        assert_eq!(redis_arg("user 1"), "\"user 1\"");
        assert_eq!(redis_arg("say \"hi\""), "\"say \\\"hi\\\"\"");
    }

    #[test]
    fn renders_hash_replies_as_field_value_rows() {
        let result = redis_result_to_query_result(
            "HGETALL",
            redis::Value::Bulk(vec![
                redis::Value::Data(b"name".to_vec()),
                redis::Value::Data(b"bob".to_vec()),
                redis::Value::Data(b"age".to_vec()),
                redis::Value::Data(b"42".to_vec()),
            ]),
            1,
            10,
        );
        assert_eq!(result.columns, vec!["field", "value"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("field"), Some(&Some("name".to_string())));
    }

    #[test]
    fn renders_scalar_replies_as_result() {
        let result = redis_result_to_query_result("GET", redis::Value::Data(b"bob".to_vec()), 1, 10);
        assert_eq!(result.columns, vec!["result"]);
        assert_eq!(
            result.rows[0].get("result"),
            Some(&Some("bob".to_string()))
        );
    }

    #[test]
    fn truncates_bulk_replies_to_max_rows() {
        let result = redis_result_to_query_result(
            "KEYS",
            redis::Value::Bulk(vec![
                redis::Value::Data(b"a".to_vec()),
                redis::Value::Data(b"b".to_vec()),
                redis::Value::Data(b"c".to_vec()),
            ]),
            1,
            2,
        );
        assert_eq!(result.row_count, 2);
        assert!(result.truncated);
    }
}
