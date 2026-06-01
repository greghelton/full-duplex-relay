//! Full-Duplex Browser Chat Relay
//!
//! A direct Rust port of FullDuplexRelay.java using Axum + Tokio.
//!
//! Each browser registers itself with a unique name and receives messages
//! via Server-Sent Events (SSE). Any browser can send a message to any
//! other connected browser by name.
//!
//! Endpoints:
//!   GET  /listen?id=<name>                      — open SSE stream
//!   GET  /send?to=<name>&from=<name>&msg=<text> — deliver a message
//!   GET  /who                                   — list connected listeners
//!   GET  /disconnect?id=<name>                  — graceful disconnect
//!   GET  /                                      — serves the chat UI
//!
//! Run:
//!   cargo run --bin full-duplex-relay
//!   Then open http://localhost:8080 in two different browsers.

use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{Query, State},
    response::{Html, IntoResponse, Sse},
    routing::get,
    Router,
};
use axum::response::sse::{Event, KeepAlive};
use dashmap::DashMap;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

// ── Types ─────────────────────────────────────────────────────────────────────

/// One broadcast channel per connected listener.
/// Sender capacity: 100 messages before old ones are dropped.
type Listeners = Arc<DashMap<String, broadcast::Sender<String>>>;

// ── Query param structs ────────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct ListenParams {
    id: String,
}

#[derive(serde::Deserialize)]
struct SendParams {
    to:   String,
    from: String,
    msg:  String,
}

#[derive(serde::Deserialize)]
struct IdParam {
    id: String,
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let listeners: Listeners = Arc::new(DashMap::new());

    let app = Router::new()
        .route("/",           get(serve_ui))
        .route("/listen",     get(listen))
        .route("/send",       get(send_msg))
        .route("/who",        get(who))
        .route("/disconnect", get(disconnect))
        .with_state(listeners);

    let addr = "0.0.0.0:8080";
    println!("Relay on http://localhost:8080");

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// GET /listen?id=<name>
///
/// Registers the caller as a named listener and opens an SSE stream.
/// Messages sent to this name will arrive as `data: ...` events.
/// The stream stays open until the client disconnects or /disconnect is called.
async fn listen(
    State(listeners): State<Listeners>,
    Query(params): Query<ListenParams>,
) -> impl IntoResponse {
    let id = params.id.clone();

    // One broadcast channel per listener; 100-message buffer.
    let (tx, rx) = broadcast::channel::<String>(100);
    listeners.insert(id.clone(), tx);
    println!("Listener registered: {id}");

    // Wrap the broadcast receiver in a Stream of SSE Events.
    let stream = BroadcastStream::new(rx)
        .filter_map(move |result| {
            let id_clone = id.clone();
            match result {
                Ok(msg) if msg == "__close__" => {
                    println!("Listener removed: {id_clone}");
                    None   // end the stream
                }
                Ok(msg) => {
                    println!("→ {id_clone}: {msg}");
                    Some(Ok::<Event, std::convert::Infallible>(Event::default().data(msg)))
                }
                Err(_) => None,  // lagged / channel closed
            }
        });

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

/// GET /send?to=<name>&from=<name>&msg=<text>
///
/// Delivers a message to the named listener.
/// Returns "delivered" or an error string — identical behaviour to the Java version.
async fn send_msg(
    State(listeners): State<Listeners>,
    Query(params): Query<SendParams>,
) -> impl IntoResponse {
    let formatted = format!("[{}] {}", params.from, params.msg);

    match listeners.get(&params.to) {
        Some(tx) => {
            let _ = tx.send(formatted);
            println!("{} → {}: {}", params.from, params.to, params.msg);
            "delivered".to_string()
        }
        None => {
            println!("Undelivered (no listener): {}", params.to);
            format!("no listener named '{}'", params.to)
        }
    }
}

/// GET /who
///
/// Returns a plain-text list of currently connected listener names.
async fn who(State(listeners): State<Listeners>) -> impl IntoResponse {
    let names: Vec<String> = listeners.iter().map(|e| e.key().clone()).collect();
    format!("Online: [{}]", names.join(", "))
}

/// GET /disconnect?id=<name>
///
/// Sends the internal `__close__` sentinel to gracefully end the SSE stream.
async fn disconnect(
    State(listeners): State<Listeners>,
    Query(params): Query<IdParam>,
) -> impl IntoResponse {
    if let Some(tx) = listeners.get(&params.id) {
        let _ = tx.send("__close__".to_string());
    }
    "disconnected".to_string()
}

/// GET /
///
/// Serves the self-contained browser chat UI (identical to the Java version).
async fn serve_ui() -> impl IntoResponse {
    Html(UI_HTML)
}

// ── Embedded chat UI ──────────────────────────────────────────────────────────

const UI_HTML: &str = r#"<!DOCTYPE html>
<html>
<head>
<style>
  body { font-family: sans-serif; max-width: 500px; margin: 40px auto; padding: 0 16px; }
  #log { border: 1px solid #ccc; border-radius: 6px; height: 260px;
         overflow-y: auto; padding: 10px; margin: 12px 0;
         background: #fafafa; font-size: 14px; }
  .msg-in   { color: #1a6; }
  .msg-out  { color: #339; }
  .msg-sys  { color: #999; font-style: italic; }
  input, button, select { font-size: 14px; padding: 6px 10px; }
  input[type=text] { width: 100%; box-sizing: border-box; margin: 4px 0; }
  .row { display: flex; gap: 8px; margin-top: 8px; }
  .row input { flex: 1; }
</style>
</head>
<body>
<h3>Browser Chat</h3>

<label>Your name:
  <input id="myId" type="text" placeholder="e.g. firefox" style="width:140px;margin-left:6px"/>
</label>
<button onclick="connect()">Connect</button>
<button onclick="disconnect()">Disconnect</button>

<div id="log"></div>

<label>To: <select id="toId"><option value="">— nobody connected —</option></select></label>
<div class="row">
  <input id="msgBox" type="text" placeholder="Type a message..." onkeydown="if(event.key==='Enter')sendMsg()"/>
  <button onclick="sendMsg()">Send</button>
</div>

<script>
let es;
const log    = document.getElementById('log');
const myId   = document.getElementById('myId');
const toId   = document.getElementById('toId');
const msgBox = document.getElementById('msgBox');

function addLine(text, cls) {
    const div = document.createElement('div');
    div.className = cls;
    div.textContent = text;
    log.appendChild(div);
    log.scrollTop = log.scrollHeight;
}

function connect() {
    const id = myId.value.trim();
    if (!id) { alert('Enter your name first'); return; }
    if (es) es.close();
    es = new EventSource('/listen?id=' + encodeURIComponent(id));
    es.onmessage = e => { addLine('IN: ' + e.data, 'msg-in'); refreshWho(); };
    es.onerror   = () => addLine('Connection lost', 'msg-sys');
    addLine('Connected as ' + id, 'msg-sys');
    refreshWho();
}

function disconnect() {
    const id = myId.value.trim();
    if (es) { es.close(); es = null; }
    fetch('/disconnect?id=' + encodeURIComponent(id));
    addLine('Disconnected', 'msg-sys');
    refreshWho();
}

function sendMsg() {
    const to   = toId.value;
    const msg  = msgBox.value.trim();
    const from = myId.value.trim();
    if (!to || !msg) return;
    fetch('/send?to='   + encodeURIComponent(to)
             + '&from=' + encodeURIComponent(from)
             + '&msg='  + encodeURIComponent(msg))
        .then(r => r.text())
        .then(result => {
            if (result === 'delivered') {
                addLine('OUT to ' + to + ': ' + msg, 'msg-out');
                msgBox.value = '';
            } else {
                addLine('Not delivered: ' + result, 'msg-sys');
            }
        });
}

function refreshWho() {
    fetch('/who')
        .then(r => r.text())
        .then(text => {
            const names = text.replace('Online: [','').replace(']','')
                              .split(',').map(s => s.trim()).filter(Boolean);
            const me = myId.value.trim();
            toId.innerHTML = '';
            const others = names.filter(n => n !== me && n !== '');
            if (others.length === 0) {
                toId.innerHTML = '<option value="">— nobody else online —</option>';
            } else {
                others.forEach(n => {
                    const opt = document.createElement('option');
                    opt.value = n; opt.textContent = n;
                    toId.appendChild(opt);
                });
            }
        });
}

setInterval(refreshWho, 3000);
</script>
</body>
</html>"#;
