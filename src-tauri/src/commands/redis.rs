use tauri::{AppHandle, State};

use crate::db::{get_or_create_redis_client, resolve_connection_engine, AppState, MAX_QUERY_ROWS};
use crate::engines::{get_engine, DatabaseEngineOps};
use crate::models::{ColumnInfo, QueryRequest, QueryResult, TableInfo};

#[tauri::command]
pub async fn redis_run_query(
    app: AppHandle,
    state: State<'_, AppState>,
    input: QueryRequest,
) -> Result<QueryResult, String> {
    let (connection_id, engine) = resolve_connection_engine(&app, &state, input.connection_id.clone()).await?;
    let sql = input.sql.trim().to_string();
    if sql.is_empty() {
        return Err("Enter a Redis command.".to_string());
    }
    if !input.allow_write.unwrap_or(false) && !super::is_read_only_redis_command(&sql) {
        return Err(
            "This Redis command modifies data. Confirm execution in the editor.".to_string(),
        );
    }
    let max_rows = input.max_rows.unwrap_or(MAX_QUERY_ROWS);
    get_engine(engine)
        .run_query(&app, &state, &connection_id, &sql, max_rows)
        .await
        .map_err(String::from)
}

#[tauri::command]
pub async fn redis_get_keys(
    app: AppHandle,
    state: State<'_, AppState>,
    connection_id: Option<String>,
) -> Result<Vec<TableInfo>, String> {
    let (connection_id, _) = resolve_connection_engine(&app, &state, connection_id).await?;
    let mut client = get_or_create_redis_client(&app, &state, &connection_id).await?;
    let keys: Vec<String> = redis::cmd("KEYS").arg("*").query_async(&mut client).await.map_err(|e| e.to_string())?;
    Ok(keys.into_iter().map(|k| TableInfo {
        schema: "0".to_string(),
        name: k.clone(),
        preview_query: format!("GET \"{}\"", k.replace('\\', "\\\\").replace('"', "\\\"")),
        kind: Some("table".to_string()),
    }).collect())
}

/// Infer schema for a Redis key by checking its type and sampling data.
#[tauri::command]
pub async fn redis_get_schema(
    app: AppHandle,
    state: State<'_, AppState>,
    connection_id: Option<String>,
    _table_schema: String,
    table_name: String,
) -> Result<Vec<ColumnInfo>, String> {
    let (connection_id, _) = resolve_connection_engine(&app, &state, connection_id).await?;
    let mut client = get_or_create_redis_client(&app, &state, &connection_id).await?;

    let key_type: String = redis::cmd("TYPE").arg(&table_name).query_async(&mut client)
        .await.map_err(|e| format!("Redis TYPE failed: {}", e))?;

    match key_type.as_str() {
        "string" => Ok(vec![ColumnInfo {
            table_schema: "0".to_string(), table_name: table_name.clone(),
            column_name: "value".to_string(), data_type: "string".to_string(), is_nullable: true,
        }]),
        "hash" => {
            let fields: Vec<String> = redis::cmd("HKEYS").arg(&table_name).query_async(&mut client)
                .await.map_err(|e| format!("Redis HKEYS failed: {}", e))?;
            Ok(fields.into_iter().map(|f| ColumnInfo {
                table_schema: "0".to_string(), table_name: table_name.clone(),
                column_name: f, data_type: "string".to_string(), is_nullable: true,
            }).collect())
        }
        "list" => Ok(vec![
            ColumnInfo { table_schema: "0".to_string(), table_name: table_name.clone(), column_name: "index".to_string(), data_type: "integer".to_string(), is_nullable: false },
            ColumnInfo { table_schema: "0".to_string(), table_name: table_name.clone(), column_name: "value".to_string(), data_type: "string".to_string(), is_nullable: true },
        ]),
        "set" => Ok(vec![ColumnInfo {
            table_schema: "0".to_string(), table_name: table_name.clone(),
            column_name: "member".to_string(), data_type: "string".to_string(), is_nullable: false,
        }]),
        _ => Ok(vec![ColumnInfo {
            table_schema: "0".to_string(), table_name: table_name.clone(),
            column_name: "value".to_string(), data_type: key_type, is_nullable: true,
        }]),
    }
}
