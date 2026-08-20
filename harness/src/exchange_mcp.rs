use std::io::{self, BufRead, Write};

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::exchange::{
    ClaimedClientInfo, Exchange, MessageKind, MessageRef, PostOrigin, PostRequest, ResolveRequest,
    VerdictOutcome, VerdictVerification,
};

const MCP_PROTOCOL_VERSION: &str = "2025-03-26";

#[derive(Debug, Deserialize)]
struct CallToolParams {
    name: String,
    #[serde(default)]
    arguments: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InitializeParams {
    client_info: ClaimedClientInfo,
}

#[derive(Debug, Default)]
struct SessionState {
    claimed_client_info: Option<ClaimedClientInfo>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReadArguments {
    thread: String,
    #[serde(default)]
    since: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PostArguments {
    kind: MessageKind,
    subject: String,
    body: String,
    thread: Option<String>,
    #[serde(default)]
    refs: Vec<MessageRef>,
    replies_to: Option<u32>,
    answered_by: Option<String>,
    verification: Option<VerdictVerification>,
}

#[derive(Debug, Deserialize)]
struct ResolveArguments {
    thread: String,
    outcome: VerdictOutcome,
    body: String,
    refs: Vec<MessageRef>,
    verification: VerdictVerification,
}

pub fn serve_stdio(exchange: &Exchange, author: &str) -> Result<()> {
    exchange.validate_mcp_author(author)?;
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();
    let mut session = SessionState::default();
    for line in stdin.lock().lines() {
        let line = line.context("failed to read MCP request")?;
        if line.trim().is_empty() {
            continue;
        }
        let request = match serde_json::from_str::<Value>(&line) {
            Ok(request) => request,
            Err(error) => {
                write_response(
                    &mut stdout,
                    &json_rpc_error(Value::Null, -32700, &format!("parse error: {error}")),
                )?;
                continue;
            }
        };
        if let Some(response) = dispatch_message(exchange, author, &mut session, &request) {
            write_response(&mut stdout, &response)?;
        }
    }
    Ok(())
}

fn write_response(output: &mut impl Write, response: &Value) -> Result<()> {
    serde_json::to_writer(&mut *output, response)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}

fn dispatch_message(
    exchange: &Exchange,
    author: &str,
    session: &mut SessionState,
    message: &Value,
) -> Option<Value> {
    let Some(batch) = message.as_array() else {
        return handle_request(exchange, author, session, message);
    };
    if batch.is_empty() {
        return Some(json_rpc_error(Value::Null, -32600, "invalid request"));
    }
    let responses = batch
        .iter()
        .filter_map(|request| handle_request(exchange, author, session, request))
        .collect::<Vec<_>>();
    (!responses.is_empty()).then_some(Value::Array(responses))
}

fn handle_request(
    exchange: &Exchange,
    author: &str,
    session: &mut SessionState,
    request: &Value,
) -> Option<Value> {
    let Some(object) = request.as_object() else {
        return Some(json_rpc_error(Value::Null, -32600, "invalid request"));
    };
    let candidate_id = object.get("id").cloned();
    let valid_id = candidate_id
        .as_ref()
        .is_none_or(|id| id.is_null() || id.is_string() || id.is_number());
    if object.get("jsonrpc") != Some(&Value::String("2.0".to_owned())) || !valid_id {
        return Some(json_rpc_error(Value::Null, -32600, "invalid request"));
    }
    let Some(method) = object.get("method").and_then(Value::as_str) else {
        return Some(json_rpc_error(
            candidate_id.unwrap_or(Value::Null),
            -32600,
            "request method is required",
        ));
    };
    let id = candidate_id?;
    if let Err(error) = exchange.validate_mcp_author(author) {
        return Some(json_rpc_error(id, -32602, &error.to_string()));
    }
    let result = match method {
        "initialize" => request
            .get("params")
            .cloned()
            .context("initialize params are required")
            .and_then(|params| {
                serde_json::from_value::<InitializeParams>(params)
                    .context("initialize requires non-empty clientInfo.name and clientInfo.version")
            })
            .and_then(|params| {
                if params.client_info.name.trim().is_empty()
                    || params.client_info.version.trim().is_empty()
                {
                    anyhow::bail!(
                        "initialize requires non-empty clientInfo.name and clientInfo.version"
                    );
                }
                session.claimed_client_info = Some(params.client_info);
                Ok(json!({
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": {"tools": {}},
                    "serverInfo": {
                        "name": "overmesh-harness-exchange",
                        "version": env!("CARGO_PKG_VERSION")
                    }
                }))
            }),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({"tools": tool_definitions()})),
        "tools/call" => session
            .claimed_client_info
            .clone()
            .context("tools/call rejected before successful initialize")
            .and_then(|claimed_client_info| {
                request
                    .get("params")
                    .cloned()
                    .context("tools/call params are required")
                    .and_then(|params| {
                        serde_json::from_value::<CallToolParams>(params)
                            .context("invalid tools/call params")
                    })
                    .and_then(|params| call_tool(exchange, author, &claimed_client_info, params))
            }),
        method => {
            return Some(json_rpc_error(
                id,
                -32601,
                &format!("method not found: {method}"),
            ));
        }
    };
    Some(match result {
        Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
        Err(error) => json_rpc_error(id, -32602, &format!("{error:#}")),
    })
}

fn call_tool(
    exchange: &Exchange,
    author: &str,
    claimed_client_info: &ClaimedClientInfo,
    params: CallToolParams,
) -> Result<Value> {
    exchange.validate_mcp_author(author)?;
    let value = match params.name.as_str() {
        "exchange_list" => json!({"threads": exchange.list()?}),
        "exchange_read" => {
            let arguments: ReadArguments =
                serde_json::from_value(params.arguments).context("invalid exchange_read args")?;
            serde_json::to_value(exchange.read(&arguments.thread, arguments.since)?)?
        }
        "exchange_post" => {
            let arguments: PostArguments =
                serde_json::from_value(params.arguments).context("invalid exchange_post args")?;
            if matches!(arguments.kind, MessageKind::Verdict | MessageKind::Approval) {
                anyhow::bail!(
                    "exchange_post cannot post verdict or approval; use exchange_resolve or the operator CLI"
                );
            }
            serde_json::to_value(exchange.post(
                author,
                PostOrigin::Mcp,
                PostRequest {
                    kind: arguments.kind,
                    subject: arguments.subject,
                    body: arguments.body,
                    thread: arguments.thread,
                    refs: arguments.refs,
                    replies_to: arguments.replies_to,
                    answered_by: arguments.answered_by,
                    outcome: None,
                    claimed_client_info: Some(claimed_client_info.clone()),
                    verification: arguments.verification,
                },
            )?)?
        }
        "exchange_resolve" => {
            let arguments: ResolveArguments = serde_json::from_value(params.arguments)
                .context("invalid exchange_resolve args")?;
            serde_json::to_value(exchange.resolve(
                author,
                PostOrigin::Mcp,
                ResolveRequest {
                    thread: arguments.thread,
                    outcome: arguments.outcome,
                    body: arguments.body,
                    refs: arguments.refs,
                    claimed_client_info: claimed_client_info.clone(),
                    verification: arguments.verification,
                },
            )?)?
        }
        name => anyhow::bail!("unknown exchange tool {name:?}"),
    };
    if !value.is_object() {
        anyhow::bail!("exchange tool result must be a JSON object");
    }
    let text = serde_json::to_string_pretty(&value)?;
    Ok(json!({
        "content": [{"type": "text", "text": text}],
        "structuredContent": value,
        "isError": false
    }))
}

fn json_rpc_error(id: Value, code: i32, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": code, "message": message}
    })
}

fn tool_definitions() -> Vec<Value> {
    vec![
        json!({
            "name": "exchange_list",
            "description": "List exchange threads with derived state and waiting participant.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }
        }),
        json!({
            "name": "exchange_read",
            "description": "Read messages after a sequence number. Unapproved spec bodies are withheld.",
            "inputSchema": {
                "type": "object",
                "required": ["thread"],
                "properties": {
                    "thread": {"type": "string"},
                    "since": {"type": "integer", "minimum": 0, "default": 0}
                },
                "additionalProperties": false
            }
        }),
        json!({
            "name": "exchange_post",
            "description": "Post a typed finding, question, correction, spec, or report; findings and reports may include verification metadata.",
            "inputSchema": {
                "type": "object",
                "required": ["kind", "subject", "body"],
                "properties": {
                    "kind": {
                        "enum": ["finding", "question", "correction", "spec", "report"]
                    },
                    "subject": {"type": "string", "minLength": 1},
                    "body": {"type": "string", "maxLength": 16384},
                    "thread": {"type": "string"},
                    "refs": {
                        "type": "array",
                        "items": {"$ref": "#/$defs/ref"}
                    },
                    "repliesTo": {"type": "integer", "minimum": 1},
                    "answeredBy": {"type": "string", "minLength": 1},
                    "verification": verification_schema()
                },
                "$defs": {"ref": ref_schema()},
                "additionalProperties": false
            }
        }),
        json!({
            "name": "exchange_resolve",
            "description": "Post a deliberate verdict. A human approval is still required to resolve the thread.",
            "inputSchema": {
                "type": "object",
                "required": ["thread", "outcome", "body", "refs", "verification"],
                "properties": {
                    "thread": {"type": "string"},
                    "outcome": {
                        "enum": ["verified", "not-verified", "withdrawn", "superseded"]
                    },
                    "body": {"type": "string", "maxLength": 16384},
                    "refs": {
                        "type": "array",
                        "minItems": 1,
                        "items": {"$ref": "#/$defs/ref"}
                    },
                    "verification": verification_schema()
                },
                "$defs": {"ref": ref_schema()},
                "additionalProperties": false
            }
        }),
    ]
}

fn ref_schema() -> Value {
    json!({
        "type": "object",
        "required": ["kind", "value"],
        "properties": {
            "kind": {
                "enum": ["code", "commit", "artifact", "record", "url"]
            },
            "value": {"type": "string", "minLength": 1}
        },
        "additionalProperties": false
    })
}

fn verification_schema() -> Value {
    json!({
        "type": "object",
        "required": ["methods", "commands"],
        "properties": {
            "methods": {
                "type": "array",
                "minItems": 1,
                "uniqueItems": true,
                "items": {
                    "enum": ["source-review", "tests-executed"]
                }
            },
            "commands": {
                "type": "array",
                "items": {"type": "string", "minLength": 1}
            }
        },
        "additionalProperties": false
    })
}

#[cfg(test)]
mod tests {
    use std::{fs, process::Command};

    use tempfile::TempDir;

    use super::*;

    fn fixture() -> (TempDir, Exchange) {
        let root = tempfile::tempdir().unwrap();
        assert!(
            Command::new("git")
                .args(["init", "-q"])
                .current_dir(root.path())
                .status()
                .unwrap()
                .success()
        );
        fs::write(root.path().join("code.rs"), "fn main() {}\n").unwrap();
        let exchange = Exchange::default_for_repository(root.path()).unwrap();
        (root, exchange)
    }

    fn client_info() -> ClaimedClientInfo {
        ClaimedClientInfo {
            name: "test-client".to_owned(),
            version: "1.0.0".to_owned(),
        }
    }

    fn initialized_session(exchange: &Exchange, author: &str) -> SessionState {
        let mut session = SessionState::default();
        let response = handle_request(
            exchange,
            author,
            &mut session,
            &json!({
                "jsonrpc": "2.0",
                "id": 0,
                "method": "initialize",
                "params": {"clientInfo": client_info()}
            }),
        )
        .unwrap();
        assert!(response.get("result").is_some(), "{response}");
        session
    }

    #[test]
    fn lists_exactly_four_tools() {
        let (_root, exchange) = fixture();
        let mut session = SessionState::default();
        let response = handle_request(
            &exchange,
            "copilot",
            &mut session,
            &json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}),
        )
        .unwrap();
        assert_eq!(response["result"]["tools"].as_array().unwrap().len(), 4);
    }

    #[test]
    fn every_tool_result_has_object_structured_content() {
        let (_root, exchange) = fixture();
        let listed = call_tool(
            &exchange,
            "copilot",
            &client_info(),
            CallToolParams {
                name: "exchange_list".to_owned(),
                arguments: json!({}),
            },
        )
        .unwrap();
        assert!(listed["structuredContent"].is_object());
        assert!(listed["structuredContent"]["threads"].is_array());
    }

    #[test]
    fn posts_and_withholds_spec_through_mcp() {
        let (_root, exchange) = fixture();
        let mut copilot_session = initialized_session(&exchange, "copilot");
        let posted = handle_request(
            &exchange,
            "copilot",
            &mut copilot_session,
            &json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "exchange_post",
                    "arguments": {
                        "kind": "spec",
                        "subject": "Implement",
                        "body": "withheld"
                    }
                }
            }),
        )
        .unwrap();
        let thread = posted["result"]["structuredContent"]["thread"]
            .as_str()
            .unwrap();
        let mut claude_session = initialized_session(&exchange, "claude");
        let read = handle_request(
            &exchange,
            "claude",
            &mut claude_session,
            &json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "exchange_read",
                    "arguments": {"thread": thread}
                }
            }),
        )
        .unwrap();
        assert!(read["result"]["structuredContent"]["messages"][0]["body"].is_null());
        assert_eq!(
            read["result"]["structuredContent"]["messages"][0]["withheld"],
            "awaiting approval"
        );
        let stored = exchange.read_operator(thread, 0).unwrap();
        assert_eq!(stored.messages[0].schema_version, 2);
        assert_eq!(
            stored.messages[0].claimed_client_info.as_ref(),
            Some(&client_info())
        );

        let report = handle_request(
            &exchange,
            "copilot",
            &mut copilot_session,
            &json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "exchange_post",
                    "arguments": {
                        "kind": "report",
                        "thread": thread,
                        "subject": "Executed verification",
                        "body": "Recorded structurally",
                        "refs": [{"kind": "code", "value": "code.rs"}],
                        "verification": {
                            "methods": ["tests-executed"],
                            "commands": ["cargo test -p overmesh-harness exchange"]
                        }
                    }
                }
            }),
        )
        .unwrap();
        assert_eq!(report["result"]["structuredContent"]["seq"], 2);
        let stored = exchange.read_operator(thread, 0).unwrap();
        assert_eq!(
            stored.messages[1].verification.as_ref().unwrap().commands,
            vec!["cargo test -p overmesh-harness exchange"]
        );
    }

    #[test]
    fn rejects_tool_calls_before_successful_initialize() {
        let (_root, exchange) = fixture();
        let mut session = SessionState::default();
        let response = handle_request(
            &exchange,
            "copilot",
            &mut session,
            &json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "exchange_list", "arguments": {}}
            }),
        )
        .unwrap();
        assert_eq!(response["error"]["code"], -32602);
        assert!(
            response["error"]["message"]
                .as_str()
                .unwrap()
                .contains("before successful initialize")
        );
    }

    #[test]
    fn rejects_missing_or_empty_initialize_client_info() {
        let (_root, exchange) = fixture();
        for params in [
            json!({}),
            json!({"clientInfo": {"name": "", "version": "1.0"}}),
            json!({"clientInfo": {"name": "client", "version": " "}}),
        ] {
            let mut session = SessionState::default();
            let response = handle_request(
                &exchange,
                "copilot",
                &mut session,
                &json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": params
                }),
            )
            .unwrap();
            assert_eq!(response["error"]["code"], -32602);
            assert!(session.claimed_client_info.is_none());
        }
    }

    #[test]
    fn resolve_schema_requires_verification() {
        let definition = tool_definitions()
            .into_iter()
            .find(|tool| tool["name"] == "exchange_resolve")
            .unwrap();
        assert!(
            definition["inputSchema"]["required"]
                .as_array()
                .unwrap()
                .contains(&json!("verification"))
        );
        assert_eq!(
            definition["inputSchema"]["properties"]["verification"]["properties"]["methods"]["minItems"],
            1
        );
    }

    #[test]
    fn rejects_human_server_identity() {
        let (_root, exchange) = fixture();
        let mut session = SessionState::default();
        let response = handle_request(
            &exchange,
            "human",
            &mut session,
            &json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}),
        )
        .unwrap();
        assert_eq!(response["error"]["code"], -32602);
    }

    #[test]
    fn ignores_notifications() {
        let (_root, exchange) = fixture();
        let mut session = SessionState::default();
        assert!(
            handle_request(
                &exchange,
                "copilot",
                &mut session,
                &json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
            )
            .is_none()
        );
    }

    #[test]
    fn rejects_invalid_json_rpc_envelopes() {
        let (_root, exchange) = fixture();
        let mut session = SessionState::default();
        for request in [
            json!("not an object"),
            json!({"id": 1, "method": "tools/list"}),
            json!({"jsonrpc": "2.0", "id": true, "method": "tools/list"}),
            json!({"jsonrpc": "2.0", "id": 1}),
        ] {
            let response = handle_request(&exchange, "copilot", &mut session, &request).unwrap();
            assert_eq!(response["error"]["code"], -32600);
        }
    }

    #[test]
    fn dispatches_json_rpc_batches_and_omits_notifications() {
        let (_root, exchange) = fixture();
        let mut session = SessionState::default();
        let response = dispatch_message(
            &exchange,
            "copilot",
            &mut session,
            &json!([
                {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
                {"jsonrpc": "2.0", "method": "notifications/initialized"},
                {"jsonrpc": "2.0", "id": 2, "method": "ping"}
            ]),
        )
        .unwrap();
        let responses = response.as_array().unwrap();
        assert_eq!(responses.len(), 2);
        assert_eq!(responses[0]["id"], 1);
        assert_eq!(responses[1]["id"], 2);
        assert!(
            dispatch_message(
                &exchange,
                "copilot",
                &mut session,
                &json!([
                    {"jsonrpc": "2.0", "method": "notifications/initialized"}
                ]),
            )
            .is_none()
        );
        assert_eq!(
            dispatch_message(&exchange, "copilot", &mut session, &json!([])).unwrap()["error"]["code"],
            -32600
        );
    }
}
