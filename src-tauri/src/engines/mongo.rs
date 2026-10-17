use std::collections::{BTreeMap, HashSet};
use std::time::Instant;

use futures_util::StreamExt;
use mongodb::bson::{self, doc, Document};
use tauri::AppHandle;

use crate::db::{
    build_mongo_connection_string, get_or_create_mongo_client, load_connection, AppState,
};
use crate::error::VeloxError;
use crate::models::{ColumnInfo, ConnectionInput, DatabaseInfo, QueryResult, TableInfo};

use super::DatabaseEngineOps;

pub struct MongoEngine;

#[derive(Debug)]
struct ParsedMongoQuery {
    database: String,
    collection: String,
    filter: Document,
    limit: Option<i64>,
}

impl DatabaseEngineOps for MongoEngine {
    async fn connect(
        &self,
        _app: &AppHandle,
        state: &AppState,
        input: &ConnectionInput,
        connection_id: &str,
    ) -> Result<(), VeloxError> {
        let uri = build_mongo_connection_string(input);
        let client = mongodb::Client::with_uri_str(&uri)
            .await
            .map_err(|e| VeloxError::Connection(format!("MongoDB connection failed: {}", e)))?;
        client
            .database("admin")
            .run_command(doc! { "ping": 1 })
            .await
            .map_err(|e| VeloxError::Connection(format!("MongoDB ping failed: {}", e)))?;
        state.mongo_clients.write().await.insert(connection_id.to_string(), client);
        Ok(())
    }

    async fn ping(
        &self,
        app: &AppHandle,
        state: &AppState,
        connection_id: &str,
    ) -> Result<(), VeloxError> {
        let client = get_or_create_mongo_client(app, state, connection_id).await?;
        client.database("admin").run_command(doc! { "ping": 1 }).await
            .map_err(|e| VeloxError::Connection(format!("MongoDB ping failed: {}", e)))?;
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
        let client = get_or_create_mongo_client(app, state, connection_id).await?;
        let stored = load_connection(app, connection_id)?
            .ok_or_else(|| VeloxError::Connection("Stored connection details were not found.".to_string()))?;

        let parsed = parse_mongo_query(sql, &stored.database).map_err(VeloxError::Validation)?;
        let db = client.database(&parsed.database);
        let collection = db.collection::<Document>(&parsed.collection);

        let max_rows = max_rows.max(1);
        let requested_limit = parsed.limit.unwrap_or(max_rows as i64).max(1);
        let effective_limit = requested_limit.min(max_rows as i64);

        let started_at = Instant::now();
        let mut cursor = collection
            .find(parsed.filter)
            .limit(effective_limit)
            .await
            .map_err(|e| VeloxError::Query(format!("MongoDB find failed: {}", e)))?;

        let mut rows: Vec<BTreeMap<String, Option<String>>> = Vec::new();
        let mut columns_set = HashSet::new();
        let mut columns = Vec::new();

        while let Some(result) = cursor.next().await {
            let document = result
                .map_err(|e| VeloxError::Query(format!("MongoDB cursor error: {}", e)))?;
            let mut row = BTreeMap::new();
            for (key, value) in &document {
                if columns_set.insert(key.clone()) {
                    columns.push(key.clone());
                }
                row.insert(key.clone(), bson_to_display(value));
            }
            for col in &columns {
                row.entry(col.clone()).or_insert(None);
            }
            rows.push(row);
        }

        let truncated = requested_limit >= max_rows as i64 && rows.len() >= max_rows;

        Ok(QueryResult {
            row_count: rows.len(),
            rows,
            columns,
            execution_ms: started_at.elapsed().as_millis(),
            truncated,
            command_tag: None,
        })
    }

    async fn get_tables(
        &self,
        app: &AppHandle,
        state: &AppState,
        connection_id: &str,
    ) -> Result<Vec<TableInfo>, VeloxError> {
        let client = get_or_create_mongo_client(app, state, connection_id).await?;
        let db = client.default_database().ok_or_else(|| {
            VeloxError::Connection("No default database for MongoDB connection.".to_string())
        })?;
        let db_name = db.name().to_string();
        let names = db.list_collection_names().await
            .map_err(|e| VeloxError::Query(format!("Failed to list MongoDB collections: {}", e)))?;
        Ok(names.into_iter().map(|name| TableInfo {
            schema: db_name.clone(),
            name: name.clone(),
            preview_query: format!("{}.find({{}}).limit(100)", mongo_collection_ref(&name)),
            kind: Some("table".to_string()),
        }).collect())
    }

    async fn get_schema(
        &self,
        app: &AppHandle,
        state: &AppState,
        connection_id: &str,
        database: &str,
        collection: &str,
    ) -> Result<Vec<ColumnInfo>, VeloxError> {
        let client = get_or_create_mongo_client(app, state, connection_id).await?;
        let db = client.database(database);
        let coll = db.collection::<Document>(collection);
        let mut cursor = coll.find(doc! {}).limit(200).await
            .map_err(|e| VeloxError::Query(format!("MongoDB cursor error: {}", e)))?;

        let mut field_counts: BTreeMap<String, usize> = BTreeMap::new();
        let mut field_types: BTreeMap<String, &'static str> = BTreeMap::new();
        while let Some(result) = cursor.next().await {
            let document = result
                .map_err(|e| VeloxError::Query(format!("MongoDB cursor error: {}", e)))?;
            for (key, value) in &document {
                let entry = field_counts.entry(key.clone()).or_default();
                *entry += 1;
                // Track the first non-null BSON type seen for each field
                let btype = infer_bson_type(value);
                if btype != "null" {
                    field_types.entry(key.clone()).or_insert(btype);
                }
            }
        }

        let total = field_counts.values().max().copied().unwrap_or(1).max(1);
        Ok(field_counts.into_iter().map(|(name, count)| {
            let btype = field_types.get(&name).copied().unwrap_or("unknown");
            ColumnInfo {
                table_schema: database.to_string(),
                table_name: collection.to_string(),
                column_name: name,
                data_type: format!("{} ({}% present)", btype, count * 100 / total),
                is_nullable: count < total,
            }
        }).collect())
    }

    async fn list_databases(
        &self,
        app: &AppHandle,
        state: &AppState,
        connection_id: &str,
    ) -> Result<Vec<DatabaseInfo>, VeloxError> {
        let client = get_or_create_mongo_client(app, state, connection_id).await?;
        let db_names = client.list_database_names().await
            .map_err(|e| VeloxError::Query(format!("Failed to list MongoDB databases: {}", e)))?;
        Ok(db_names.into_iter().map(|name| DatabaseInfo { name }).collect())
    }
}

fn infer_bson_type(value: &mongodb::bson::Bson) -> &'static str {
    use mongodb::bson::Bson;
    match value {
        Bson::String(_) => "string",
        Bson::Int32(_) | Bson::Int64(_) => "integer",
        Bson::Double(_) => "double",
        Bson::Boolean(_) => "boolean",
        Bson::ObjectId(_) => "objectId",
        Bson::DateTime(_) => "date",
        Bson::Array(_) => "array",
        Bson::Document(_) => "object",
        Bson::Null => "null",
        Bson::Binary(_) => "binary",
        Bson::RegularExpression(_) => "regex",
        Bson::Decimal128(_) => "decimal",
        _ => "unknown",
    }
}

/// Build a shell reference for a collection, quoting names that are not plain
/// identifiers (`db.users` vs `db.getCollection("my-collection")`).
fn mongo_collection_ref(name: &str) -> String {
    if !name.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        format!("db.{}", name)
    } else {
        format!(
            "db.getCollection(\"{}\")",
            name.replace('\\', "\\\\").replace('"', "\\\"")
        )
    }
}

/// Parse the part before `.find(...)` into `(database, collection)`.
fn parse_collection_prefix(prefix: &str, default_db: &str) -> Result<(String, String), String> {
    let prefix = prefix.trim();
    if prefix.is_empty() {
        return Err("Specify a collection, for example db.users.find({}).".to_string());
    }

    if let Some(rest) = prefix.strip_prefix("db.getCollection(") {
        let name = rest
            .trim()
            .strip_suffix(')')
            .unwrap_or(rest)
            .trim()
            .trim_matches('"')
            .trim_matches('\'');
        if name.is_empty() {
            return Err("The collection name cannot be empty.".to_string());
        }
        return Ok((default_db.to_string(), name.to_string()));
    }

    if let Some(rest) = prefix.strip_prefix("db.") {
        let collection = rest.trim();
        if collection.is_empty() {
            return Err("Specify a collection, for example db.users.find({}).".to_string());
        }
        return Ok((default_db.to_string(), collection.to_string()));
    }

    if let Some((database, collection)) = prefix.split_once('.') {
        let database = database.trim();
        let collection = collection.trim();
        if database.is_empty() || collection.is_empty() {
            return Err("Specify a collection, for example db.users.find({}).".to_string());
        }
        return Ok((database.to_string(), collection.to_string()));
    }

    Ok((default_db.to_string(), prefix.to_string()))
}

/// Return the balanced `{...}` argument that starts `input`, respecting nested
/// objects and quoted strings.
fn extract_braced_json(input: &str) -> Option<String> {
    let trimmed = input.trim_start();
    if !trimmed.starts_with('{') {
        return None;
    }
    let mut depth = 0i32;
    let mut quote: Option<char> = None;
    let mut escaped = false;
    for (index, ch) in trimmed.char_indices() {
        if let Some(q) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == q {
                quote = None;
            }
            continue;
        }
        match ch {
            '"' | '\'' => quote = Some(ch),
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(trimmed[..index + 1].to_string());
                }
            }
            _ => {}
        }
    }
    None
}

fn parse_find_filter(after_find: &str) -> Result<Document, String> {
    let rest = after_find.trim_start();
    // `find()` is valid and matches every document.
    if rest.starts_with(')') {
        return Ok(doc! {});
    }
    let filter = extract_braced_json(rest).ok_or_else(|| {
        "Could not parse the .find() filter. Pass a JSON object, for example \
         db.users.find({\"active\": true})."
            .to_string()
    })?;
    parse_filter(&filter)
}

/// Parse `.limit(n)` from anywhere after the `find(` call.
fn parse_limit(sql: &str) -> Option<i64> {
    let marker = ".limit(";
    let start = sql.rfind(marker)? + marker.len();
    let rest = &sql[start..];
    let end = rest.find(')')?;
    rest[..end]
        .trim()
        .parse::<i64>()
        .ok()
        .filter(|limit| *limit > 0)
}

fn parse_mongo_query(sql: &str, default_db: &str) -> Result<ParsedMongoQuery, String> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err("Enter a MongoDB query or filter.".to_string());
    }

    if let Some(find_index) = trimmed.find(".find(") {
        let prefix = &trimmed[..find_index];
        let (database, collection) = parse_collection_prefix(prefix, default_db)?;
        let after_find = &trimmed[find_index + ".find(".len()..];
        let filter = parse_find_filter(after_find)?;
        let limit = parse_limit(trimmed);
        return Ok(ParsedMongoQuery { database, collection, filter, limit });
    }

    // A bare filter has no collection context in the generic query runner.
    if trimmed.starts_with('{') {
        return Err(
            "Specify a collection, for example db.users.find({\"status\": \"active\"})."
                .to_string(),
        );
    }

    // Anything else that looks like a method call (aggregate, updateMany, ...)
    // is not supported by this runner.
    if trimmed.contains('(') {
        return Err(
            "Only find() queries are supported. Use db.<collection>.find({...}).".to_string(),
        );
    }

    let (database, collection) = parse_collection_prefix(trimmed, default_db)?;
    Ok(ParsedMongoQuery {
        database,
        collection,
        filter: doc! {},
        limit: None,
    })
}

/// Expand MongoDB shell literals into Extended JSON understood by the bson
/// deserializer (`ObjectId("...")`, `ISODate("...")`).
fn normalize_shell_literals(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut index = 0usize;
    while index < input.len() {
        let rest = &input[index..];
        if let Some(inner) = rest.strip_prefix("ObjectId(") {
            if let Some(end) = inner.find(')') {
                let raw = inner[..end].trim().trim_matches('"').trim_matches('\'');
                output.push_str(&format!("{{\"$oid\":\"{}\"}}", raw));
                index += "ObjectId(".len() + end + 1;
                continue;
            }
        }
        if let Some(inner) = rest.strip_prefix("ISODate(") {
            if let Some(end) = inner.find(')') {
                let raw = inner[..end].trim().trim_matches('"').trim_matches('\'');
                output.push_str(&format!("{{\"$date\":\"{}\"}}", raw));
                index += "ISODate(".len() + end + 1;
                continue;
            }
        }
        let ch = rest.chars().next().unwrap();
        output.push(ch);
        index += ch.len_utf8();
    }
    output
}

fn parse_key_value_filter(input: &str) -> Option<Document> {
    let mut document = Document::new();
    for part in input.split(',') {
        let (key, value) = part.split_once(':')?;
        let key = key.trim().trim_matches('"').trim_matches('\'');
        if key.is_empty() {
            return None;
        }
        let raw = value.trim().trim_matches('"').trim_matches('\'');
        let bson_value = if let Ok(number) = raw.parse::<i32>() {
            bson::Bson::Int32(number)
        } else if let Ok(number) = raw.parse::<i64>() {
            bson::Bson::Int64(number)
        } else if let Ok(number) = raw.parse::<f64>() {
            bson::Bson::Double(number)
        } else if raw.eq_ignore_ascii_case("true") {
            bson::Bson::Boolean(true)
        } else if raw.eq_ignore_ascii_case("false") {
            bson::Bson::Boolean(false)
        } else if raw.eq_ignore_ascii_case("null") {
            bson::Bson::Null
        } else {
            bson::Bson::String(raw.to_string())
        };
        document.insert(key, bson_value);
    }
    if document.is_empty() { None } else { Some(document) }
}

/// Quote bare object keys so common shell-style filters (`{ status: "active" }`)
/// become valid JSON before handing them to the bson deserializer.
fn quote_bare_keys(input: &str) -> String {
    let chars: Vec<char> = input.chars().collect();
    let mut output = String::with_capacity(input.len());
    let mut containers: Vec<char> = Vec::new();
    let mut quote: Option<char> = None;
    let mut escaped = false;
    let mut last_significant: Option<char> = None;
    let mut index = 0usize;

    while index < chars.len() {
        let ch = chars[index];
        if let Some(q) = quote {
            output.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == q {
                quote = None;
                last_significant = Some(q);
            }
            index += 1;
            continue;
        }

        match ch {
            '"' | '\'' => {
                quote = Some(ch);
                output.push(ch);
                index += 1;
            }
            '{' => {
                containers.push('{');
                output.push(ch);
                last_significant = Some(ch);
                index += 1;
            }
            '[' => {
                containers.push('[');
                output.push(ch);
                last_significant = Some(ch);
                index += 1;
            }
            '}' | ']' => {
                containers.pop();
                output.push(ch);
                last_significant = Some(ch);
                index += 1;
            }
            ',' => {
                output.push(ch);
                last_significant = Some(ch);
                index += 1;
            }
            ':' => {
                output.push(ch);
                last_significant = Some(ch);
                index += 1;
            }
            _ if ch.is_whitespace() => {
                output.push(ch);
                index += 1;
            }
            _ if (ch.is_alphabetic() || ch == '_' || ch == '$')
                && containers.last() == Some(&'{')
                && matches!(last_significant, Some('{') | Some(',')) =>
            {
                let start = index;
                while index < chars.len()
                    && (chars[index].is_alphanumeric() || chars[index] == '_' || chars[index] == '$')
                {
                    index += 1;
                }
                let identifier: String = chars[start..index].iter().collect();

                let mut lookahead = index;
                while lookahead < chars.len() && chars[lookahead].is_whitespace() {
                    lookahead += 1;
                }
                if lookahead < chars.len() && chars[lookahead] == ':' {
                    output.push('"');
                    output.push_str(&identifier);
                    output.push('"');
                    last_significant = Some('"');
                } else {
                    last_significant = identifier.chars().last();
                    output.push_str(&identifier);
                }
            }
            _ => {
                output.push(ch);
                last_significant = Some(ch);
                index += 1;
            }
        }
    }

    output
}

fn parse_filter(input: &str) -> Result<Document, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() || trimmed == "{}" {
        return Ok(doc! {});
    }

    let normalized = normalize_shell_literals(trimmed);
    if let Ok(document) = serde_json::from_str::<Document>(&normalized) {
        return Ok(document);
    }

    // Relaxed JSON: unquoted object keys and single-quoted strings.
    let relaxed = quote_bare_keys(&normalized).replace('\'', "\"");
    if let Ok(document) = serde_json::from_str::<Document>(&relaxed) {
        return Ok(document);
    }

    // Last attempt: a flat `key: value, key2: value2` filter.
    let has_structure = trimmed
        .chars()
        .any(|c| matches!(c, '{' | '}' | '[' | ']'));
    if !has_structure {
        if let Some(document) = parse_key_value_filter(trimmed) {
            return Ok(document);
        }
    }

    Err(format!(
        "Could not parse the MongoDB filter `{}`. Use JSON, for example {{\"status\": \"active\"}}.",
        input
    ))
}

fn bson_to_display(value: &bson::Bson) -> Option<String> {
    match value {
        bson::Bson::Double(v) => Some(v.to_string()),
        bson::Bson::String(v) => Some(v.clone()),
        bson::Bson::Boolean(v) => Some(v.to_string()),
        bson::Bson::Null => None,
        bson::Bson::Int32(v) => Some(v.to_string()),
        bson::Bson::Int64(v) => Some(v.to_string()),
        bson::Bson::ObjectId(v) => Some(v.to_hex()),
        bson::Bson::DateTime(v) => Some(v.to_string()),
        bson::Bson::Binary(v) => Some(hex::encode(&v.bytes)),
        bson::Bson::Array(arr) => {
            let items: Vec<String> = arr.iter().filter_map(bson_to_display).collect();
            Some(format!("[{}]", items.join(", ")))
        }
        bson::Bson::Document(doc) => {
            let items: Vec<String> = doc.iter()
                .map(|(k, v)| format!("{}: {}", k, bson_to_display(v).unwrap_or_else(|| "null".to_string())))
                .collect();
            Some(format!("{{{}}}", items.join(", ")))
        }
        _ => Some(format!("{:?}", value)),
    }
}

#[cfg(test)]
mod tests {
    use super::{mongo_collection_ref, parse_filter, parse_mongo_query};

    #[test]
    fn parses_shell_find_with_default_database() {
        let parsed = parse_mongo_query("db.legals.find({})", "experiments").unwrap();
        assert_eq!(parsed.database, "experiments");
        assert_eq!(parsed.collection, "legals");
        assert!(parsed.filter.is_empty());
        assert_eq!(parsed.limit, None);
    }

    #[test]
    fn parses_preview_query_with_limit() {
        let parsed = parse_mongo_query("db.users.find({}).limit(100)", "app").unwrap();
        assert_eq!(parsed.collection, "users");
        assert_eq!(parsed.limit, Some(100));
    }

    #[test]
    fn parses_bare_collection_and_database_collection() {
        let parsed = parse_mongo_query("users", "app").unwrap();
        assert_eq!(parsed.database, "app");
        assert_eq!(parsed.collection, "users");

        let parsed = parse_mongo_query("otherdb.users.find({})", "app").unwrap();
        assert_eq!(parsed.database, "otherdb");
        assert_eq!(parsed.collection, "users");
    }

    #[test]
    fn parses_get_collection_syntax() {
        let parsed = parse_mongo_query("db.getCollection(\"my-collection\").find({})", "app")
            .unwrap();
        assert_eq!(parsed.collection, "my-collection");
    }

    #[test]
    fn rejects_filter_without_collection() {
        let error = parse_mongo_query("{\"status\": \"active\"}", "app").unwrap_err();
        assert!(error.contains("Specify a collection"));
    }

    #[test]
    fn rejects_unsupported_methods() {
        let error = parse_mongo_query("db.users.aggregate([])", "app").unwrap_err();
        assert!(error.contains("Only find()"));
    }

    #[test]
    fn parses_nested_and_relaxed_filters() {
        let parsed = parse_mongo_query("db.users.find({ name: 'bob', age: 30 })", "app").unwrap();
        assert_eq!(parsed.filter.get_str("name").unwrap(), "bob");
        assert_eq!(parsed.filter.get_i32("age").unwrap(), 30);
    }

    #[test]
    fn parses_object_id_literals() {
        let parsed = parse_mongo_query(
            "db.users.find({\"_id\": ObjectId(\"507f1f77bcf86cd799439011\")})",
            "app",
        )
        .unwrap();
        assert!(matches!(
            parsed.filter.get("_id"),
            Some(mongodb::bson::Bson::ObjectId(_))
        ));
    }

    #[test]
    fn parses_flat_key_value_filter() {
        let filter = parse_filter("status: active, attempts: 2").unwrap();
        assert_eq!(filter.get_str("status").unwrap(), "active");
        assert_eq!(filter.get_i32("attempts").unwrap(), 2);
    }

    #[test]
    fn collection_ref_quotes_unusual_names() {
        assert_eq!(mongo_collection_ref("users"), "db.users");
        assert_eq!(
            mongo_collection_ref("my-collection"),
            "db.getCollection(\"my-collection\")"
        );
    }
}
