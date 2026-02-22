use crate::app_event::AppEvent;
use crate::app_event_sender::AppEventSender;
use codex_utils_rustls_provider::ensure_rustls_crypto_provider;
use futures::SinkExt;
use futures::StreamExt;
use image::DynamicImage;
use image::ImageFormat;
use reqwest::Client;
use reqwest::multipart::Form;
use reqwest::multipart::Part;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use std::collections::HashMap;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::Instant;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use url::Url;
use uuid::Uuid;
use xcap::Window;

const DISCORD_API_BASE: &str = "https://discord.com/api/v10";
const DISCORD_GATEWAY_VERSION: &str = "10";
const DISCORD_MESSAGE_INTENTS: i64 = (1 << 9) | (1 << 12) | (1 << 15);
const STARTUP_HEALTHCHECK_MESSAGE_LIMIT: usize = 600;
const PROMPT_DETAILS_LIMIT: usize = 900;
const WAITING_CONTEXT_LIMIT: usize = 700;
const ASSISTANT_MESSAGE_BATCH_DELAY: Duration = Duration::from_secs(2);
const DISCORD_MAX_MESSAGE_CHARS: usize = 1900;
const DISCORD_SCREENSHOT_COMMAND: &str = "/cc";
const BUTTON_WAIT_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const GATEWAY_RETRY_DELAY: Duration = Duration::from_secs(2);
const SNAPSHOT_IMAGE_FILENAME: &str = "codex-cli-screen.png";

#[derive(Clone)]
pub(crate) struct DiscordApprovalBridge {
    enabled: bool,
    client: Client,
    bot_token: Option<String>,
    channel_id: Option<String>,
    app_event_tx: AppEventSender,
    assistant_message_tx: Option<UnboundedSender<String>>,
    terminal_window_id: Arc<AtomicU32>,
}

#[derive(Clone)]
struct PendingChoice {
    request_key: String,
    shortcut: char,
    label: String,
    custom_id: String,
}

struct SelectedChoice {
    choice: PendingChoice,
    interaction_id: String,
    interaction_token: String,
}

#[derive(Debug, PartialEq, Eq)]
struct DiscordInstructionMessage {
    message_id: String,
    text: String,
}

enum DiscordMentionCommand {
    CaptureCurrentScreen,
    Unknown { command: String },
}

impl DiscordApprovalBridge {
    pub(crate) fn new(enabled: bool, app_event_tx: AppEventSender) -> Self {
        let client = Client::new();
        let bot_token = std::env::var("CODEX_DISCORD_BOT_TOKEN").ok();
        let channel_id = std::env::var("CODEX_DISCORD_CHANNEL_ID").ok();
        let terminal_window_id = Arc::new(AtomicU32::new(
            detect_focused_window_id().unwrap_or_default(),
        ));
        let assistant_message_tx =
            spawn_assistant_message_forwarder(enabled, &client, &bot_token, &channel_id);
        let bridge = Self {
            enabled,
            client,
            bot_token,
            channel_id,
            app_event_tx,
            assistant_message_tx,
            terminal_window_id,
        };
        bridge.spawn_message_listener();
        bridge
    }

    fn spawn_message_listener(&self) {
        if !self.enabled {
            return;
        }
        let Some(bot_token) = self.bot_token.clone() else {
            return;
        };
        let Some(channel_id) = self.channel_id.clone() else {
            return;
        };
        let app_event_tx = self.app_event_tx.clone();
        let client = self.client.clone();
        let terminal_window_id = self.terminal_window_id.clone();
        tokio::spawn(async move {
            ensure_rustls_crypto_provider();
            loop {
                let bot_user_id = match fetch_bot_user_id(&client, &bot_token).await {
                    Ok(id) => id,
                    Err(err) => {
                        tracing::warn!(error = %err, "failed to fetch Discord bot user id");
                        tokio::time::sleep(GATEWAY_RETRY_DELAY).await;
                        continue;
                    }
                };
                let gateway_url = match fetch_gateway_ws_url(&client, &bot_token).await {
                    Ok(url) => url,
                    Err(err) => {
                        tracing::warn!(error = %err, "failed to fetch Discord gateway websocket url");
                        tokio::time::sleep(GATEWAY_RETRY_DELAY).await;
                        continue;
                    }
                };

                let result = listen_for_discord_instructions(
                    &client,
                    &bot_token,
                    &channel_id,
                    &bot_user_id,
                    &app_event_tx,
                    terminal_window_id.clone(),
                    gateway_url,
                )
                .await;
                if let Err(err) = result {
                    tracing::warn!(error = %err, "Discord instruction listener dropped; reconnecting");
                }
                tokio::time::sleep(GATEWAY_RETRY_DELAY).await;
            }
        });
    }

    pub(crate) fn notify_approval_prompt(
        &self,
        request_key: String,
        title: String,
        details: String,
        options: Vec<(char, String)>,
    ) {
        if !self.enabled {
            return;
        }
        let Some(bot_token) = self.bot_token.clone() else {
            tracing::warn!("discord approvals enabled but CODEX_DISCORD_BOT_TOKEN not set");
            return;
        };
        let Some(channel_id) = self.channel_id.clone() else {
            tracing::warn!("discord approvals enabled but CODEX_DISCORD_CHANNEL_ID not set");
            return;
        };

        let choices = options
            .into_iter()
            .map(|(shortcut, label)| PendingChoice {
                request_key: request_key.clone(),
                shortcut,
                label,
                custom_id: format!("codex-approval-{}", Uuid::new_v4().simple()),
            })
            .collect::<Vec<_>>();
        if choices.is_empty() {
            tracing::warn!(request_key = %request_key, "discord approval prompt had no options");
            return;
        }

        let app_event_tx = self.app_event_tx.clone();
        let client = self.client.clone();
        let terminal_window_id = self.terminal_window_id.clone();
        tokio::spawn(async move {
            ensure_rustls_crypto_provider();
            let snapshot_png =
                capture_terminal_window_screenshot_png(load_window_id(&terminal_window_id)).await;
            let mut screenshot_sent = false;
            if let Some((snapshot_png, captured_window_id)) = snapshot_png
                && send_screenshot_message(
                    &client,
                    &bot_token,
                    &channel_id,
                    &request_key,
                    snapshot_png,
                )
                .await
                .is_ok()
            {
                store_window_id(&terminal_window_id, captured_window_id);
                screenshot_sent = true;
            }

            let mut prompt = format!(
                "**Codex approval requested**\n\nRequest key: `{request_key}`\n{title}\n\n```\n{}\n```\n\nUse one of the buttons below.",
                truncate_for_discord(&details, PROMPT_DETAILS_LIMIT)
            );
            if screenshot_sent {
                prompt.push_str("\n\nA CLI screenshot was sent in a separate message.");
            }

            if let Err(err) =
                send_message_with_buttons(&client, &bot_token, &channel_id, &prompt, &choices).await
            {
                tracing::warn!(error = %err, request_key = %request_key, "failed to post discord approval prompt");
                return;
            }

            match wait_for_button_choice(&client, &bot_token, &choices).await {
                Ok(Some(selected)) => {
                    let ack = format!(
                        "You chose `{}` for `{}`. Codex received it.",
                        selected.choice.label, selected.choice.request_key
                    );
                    if let Err(err) = send_interaction_callback(
                        &client,
                        &selected.interaction_id,
                        &selected.interaction_token,
                        &ack,
                    )
                    .await
                    {
                        tracing::warn!(error = %err, request_key = %request_key, "failed to send discord interaction acknowledgment");
                    }
                    app_event_tx.send(AppEvent::DiscordApprovalShortcut {
                        request_key: selected.choice.request_key,
                        shortcut: selected.choice.shortcut,
                    });
                }
                Ok(None) => {
                    tracing::warn!(request_key = %request_key, "discord approval timed out without button click");
                }
                Err(err) => {
                    tracing::warn!(error = %err, request_key = %request_key, "discord interaction listener failed");
                }
            }
        });
    }

    pub(crate) fn notify_waiting_for_input(&self, context: String) {
        if !self.enabled {
            return;
        }
        let Some(bot_token) = self.bot_token.clone() else {
            return;
        };
        let Some(channel_id) = self.channel_id.clone() else {
            return;
        };
        let client = self.client.clone();
        let terminal_window_id = self.terminal_window_id.clone();
        tokio::spawn(async move {
            ensure_rustls_crypto_provider();
            if let Some((screenshot_png, captured_window_id)) =
                capture_terminal_window_screenshot_png(load_window_id(&terminal_window_id)).await
            {
                store_window_id(&terminal_window_id, captured_window_id);
                let _ = send_screenshot_message(
                    &client,
                    &bot_token,
                    &channel_id,
                    "idle-waiting",
                    screenshot_png,
                )
                .await;
            }

            let message = format!(
                "**Codex is waiting for input**\n\nContext:\n```\n{}\n```\n\nMention the bot in this channel with your next instruction.",
                truncate_for_discord(&context, WAITING_CONTEXT_LIMIT)
            );
            if let Err(err) =
                send_plain_message(&client, &bot_token, &channel_id, &message, None).await
            {
                tracing::warn!(error = %err, "failed to send Discord waiting-for-input message");
            }
        });
    }

    pub(crate) fn notify_assistant_message(&self, message: String) {
        if !self.enabled {
            return;
        }
        let Some(tx) = &self.assistant_message_tx else {
            return;
        };
        if tx.send(message).is_err() {
            tracing::warn!("failed to enqueue Discord assistant message");
        }
    }
}

pub(crate) async fn announce_startup_or_fail(enabled: bool, cwd: &Path) -> Result<(), String> {
    if !enabled {
        return Ok(());
    }
    ensure_rustls_crypto_provider();

    let bot_token = std::env::var("CODEX_DISCORD_BOT_TOKEN").map_err(|_| {
        "CODEX_DISCORD_BOT_TOKEN must be set when --enable-discord is used".to_string()
    })?;
    let channel_id = std::env::var("CODEX_DISCORD_CHANNEL_ID").map_err(|_| {
        "CODEX_DISCORD_CHANNEL_ID must be set when --enable-discord is used".to_string()
    })?;

    let client = Client::new();
    fetch_gateway_url(&client, &bot_token)
        .await
        .map_err(|err| format!("failed to connect to Discord gateway API: {err}"))?;

    let startup_message = format!(
        "Codex started in `{}`. Discord approvals are enabled.",
        truncate_for_discord(
            &cwd.display().to_string(),
            STARTUP_HEALTHCHECK_MESSAGE_LIMIT
        )
    );
    send_plain_message(&client, &bot_token, &channel_id, &startup_message, None)
        .await
        .map_err(|err| format!("failed to send startup message to Discord: {err}"))?;

    Ok(())
}

fn truncate_for_discord(input: &str, max_chars: usize) -> String {
    let char_count = input.chars().count();
    if char_count <= max_chars {
        return input.to_string();
    }
    let keep = max_chars.saturating_sub(1);
    let truncated = input.chars().take(keep).collect::<String>();
    format!("{truncated}…")
}

async fn capture_terminal_window_screenshot_png(
    preferred_window_id: Option<u32>,
) -> Option<(Vec<u8>, u32)> {
    tokio::task::spawn_blocking(move || {
        let windows = Window::all().ok().unwrap_or_default();
        if let Some(window_id) = preferred_window_id {
            if let Some(window) = windows
                .into_iter()
                .find(|window| window.id().ok() == Some(window_id))
                && let Some(png) = window.capture_image().ok().and_then(encode_png)
            {
                return Some((png, window_id));
            }
            return None;
        }

        let focused_window = windows
            .into_iter()
            .find(|window| window.is_focused().unwrap_or(false))?;
        let focused_window_id = focused_window.id().ok()?;
        let png = focused_window.capture_image().ok().and_then(encode_png)?;
        Some((png, focused_window_id))
    })
    .await
    .ok()
    .flatten()
}

fn detect_focused_window_id() -> Option<u32> {
    Window::all()
        .ok()?
        .into_iter()
        .find(|window| window.is_focused().unwrap_or(false))
        .and_then(|window| window.id().ok())
}

fn load_window_id(window_id: &AtomicU32) -> Option<u32> {
    let value = window_id.load(Ordering::Relaxed);
    if value == 0 { None } else { Some(value) }
}

fn store_window_id(window_id: &AtomicU32, value: u32) {
    window_id.store(value, Ordering::Relaxed);
}

fn encode_png(image: image::RgbaImage) -> Option<Vec<u8>> {
    let mut png = Vec::new();
    let mut writer = Cursor::new(&mut png);
    DynamicImage::ImageRgba8(image)
        .write_to(&mut writer, ImageFormat::Png)
        .ok()?;
    Some(png)
}

async fn send_screenshot_message(
    client: &Client,
    bot_token: &str,
    channel_id: &str,
    request_key: &str,
    screenshot_png: Vec<u8>,
) -> Result<String, String> {
    let caption = truncate_for_discord(&format!("CLI screenshot for request `{request_key}`"), 180);
    let screenshot_part = Part::bytes(screenshot_png)
        .file_name(SNAPSHOT_IMAGE_FILENAME.to_string())
        .mime_str("image/png")
        .map_err(|err| format!("failed to build screenshot attachment: {err}"))?;
    let form = Form::new()
        .text("payload_json", json!({ "content": caption }).to_string())
        .part("files[0]", screenshot_part);
    client
        .post(format!("{DISCORD_API_BASE}/channels/{channel_id}/messages"))
        .header("Authorization", format!("Bot {bot_token}"))
        .multipart(form)
        .send()
        .await
        .map_err(|err| format!("failed to send screenshot message: {err}"))?
        .error_for_status()
        .map_err(|err| format!("discord rejected screenshot message: {err}"))?
        .json::<SendMessageResponse>()
        .await
        .map(|response| response.id)
        .map_err(|err| format!("failed to decode screenshot response: {err}"))
}

async fn wait_for_button_choice(
    client: &Client,
    bot_token: &str,
    choices: &[PendingChoice],
) -> Result<Option<SelectedChoice>, String> {
    ensure_rustls_crypto_provider();
    let choices_by_custom_id = choices
        .iter()
        .cloned()
        .map(|choice| (choice.custom_id.clone(), choice))
        .collect::<HashMap<_, _>>();

    let deadline = Instant::now() + BUTTON_WAIT_TIMEOUT;
    while Instant::now() < deadline {
        let gateway_url = fetch_gateway_ws_url(client, bot_token).await?;
        match listen_for_interaction_choice(bot_token, &choices_by_custom_id, gateway_url, deadline)
            .await
        {
            Ok(Some(choice)) => return Ok(Some(choice)),
            Ok(None) => return Ok(None),
            Err(err) => {
                tracing::warn!(error = %err, "discord gateway connection dropped; retrying");
                tokio::time::sleep(GATEWAY_RETRY_DELAY).await;
            }
        }
    }

    Ok(None)
}

async fn listen_for_interaction_choice(
    bot_token: &str,
    choices_by_custom_id: &HashMap<String, PendingChoice>,
    gateway_url: Url,
    deadline: Instant,
) -> Result<Option<SelectedChoice>, String> {
    let (mut websocket, _) = connect_async(gateway_url.as_str())
        .await
        .map_err(|err| format!("failed websocket connect: {err}"))?;

    let mut seq: Option<i64> = None;
    let mut heartbeat: Option<tokio::time::Interval> = None;
    let mut identified = false;

    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                return Ok(None);
            }
            _ = async {
                if let Some(interval) = &mut heartbeat {
                    interval.tick().await;
                }
            }, if heartbeat.is_some() => {
                let payload = json!({"op": 1, "d": seq});
                websocket
                    .send(Message::Text(payload.to_string().into()))
                    .await
                    .map_err(|err| format!("failed heartbeat send: {err}"))?;
            }
            message = websocket.next() => {
                let Some(message) = message else {
                    return Err("discord gateway closed connection".to_string());
                };
                let message = message.map_err(|err| format!("discord gateway message error: {err}"))?;
                match message {
                    Message::Text(text) => {
                        let payload: Value = serde_json::from_str(&text)
                            .map_err(|err| format!("invalid discord gateway payload: {err}"))?;

                        if let Some(next_seq) = payload.get("s").and_then(Value::as_i64) {
                            seq = Some(next_seq);
                        }

                        let Some(op) = payload.get("op").and_then(Value::as_i64) else {
                            continue;
                        };

                        match op {
                            0 => {
                                if payload.get("t").and_then(Value::as_str) == Some("INTERACTION_CREATE")
                                    && let Some(selected) = parse_interaction_choice(&payload, choices_by_custom_id)
                                {
                                    return Ok(Some(selected));
                                }
                            }
                            7 | 9 => {
                                return Err(format!("discord gateway requested reconnect (op={op})"));
                            }
                            10 => {
                                let interval_ms = payload
                                    .get("d")
                                    .and_then(|d| d.get("heartbeat_interval"))
                                    .and_then(Value::as_u64)
                                    .ok_or_else(|| "discord gateway missing heartbeat interval".to_string())?;

                                heartbeat = Some(tokio::time::interval(Duration::from_millis(interval_ms)));

                                if !identified {
                                    let identify = json!({
                                        "op": 2,
                                        "d": {
                                            "token": bot_token,
                                            "intents": 0,
                                            "properties": {
                                                "os": std::env::consts::OS,
                                                "browser": "codex-tui",
                                                "device": "codex-tui"
                                            }
                                        }
                                    });
                                    websocket
                                        .send(Message::Text(identify.to_string().into()))
                                        .await
                                        .map_err(|err| format!("failed to send identify: {err}"))?;
                                    identified = true;
                                }
                            }
                            11 => {}
                            _ => {}
                        }
                    }
                    Message::Binary(_) | Message::Frame(_) => {}
                    Message::Ping(payload) => {
                        websocket
                            .send(Message::Pong(payload))
                            .await
                            .map_err(|err| format!("failed to respond to ping: {err}"))?;
                    }
                    Message::Pong(_) => {}
                    Message::Close(close_frame) => {
                        return Err(format!("discord gateway closed: {close_frame:?}"));
                    }
                }
            }
        }
    }
}

fn parse_interaction_choice(
    gateway_payload: &Value,
    choices_by_custom_id: &HashMap<String, PendingChoice>,
) -> Option<SelectedChoice> {
    let interaction = gateway_payload.get("d")?;
    let custom_id = interaction
        .get("data")
        .and_then(|data| data.get("custom_id"))
        .and_then(Value::as_str)?;
    let choice = choices_by_custom_id.get(custom_id)?.clone();

    let interaction_id = interaction.get("id")?.as_str()?.to_string();
    let interaction_token = interaction.get("token")?.as_str()?.to_string();

    Some(SelectedChoice {
        choice,
        interaction_id,
        interaction_token,
    })
}

async fn listen_for_discord_instructions(
    client: &Client,
    bot_token: &str,
    channel_id: &str,
    bot_user_id: &str,
    app_event_tx: &AppEventSender,
    terminal_window_id: Arc<AtomicU32>,
    gateway_url: Url,
) -> Result<(), String> {
    let (mut websocket, _) = connect_async(gateway_url.as_str())
        .await
        .map_err(|err| format!("failed websocket connect for discord messages: {err}"))?;

    let mut seq: Option<i64> = None;
    let mut heartbeat: Option<tokio::time::Interval> = None;
    let mut identified = false;

    loop {
        tokio::select! {
            _ = async {
                if let Some(interval) = &mut heartbeat {
                    interval.tick().await;
                }
            }, if heartbeat.is_some() => {
                let payload = json!({"op": 1, "d": seq});
                websocket
                    .send(Message::Text(payload.to_string().into()))
                    .await
                    .map_err(|err| format!("failed heartbeat send for discord messages: {err}"))?;
            }
            message = websocket.next() => {
                let Some(message) = message else {
                    return Err("discord message listener closed connection".to_string());
                };
                let message = message.map_err(|err| format!("discord message listener receive error: {err}"))?;
                match message {
                    Message::Text(text) => {
                        let payload: Value = serde_json::from_str(&text)
                            .map_err(|err| format!("invalid discord message gateway payload: {err}"))?;
                        if let Some(next_seq) = payload.get("s").and_then(Value::as_i64) {
                            seq = Some(next_seq);
                        }

                        let Some(op) = payload.get("op").and_then(Value::as_i64) else {
                            continue;
                        };

                        match op {
                            0 => {
                                if payload.get("t").and_then(Value::as_str) == Some("MESSAGE_CREATE")
                                    && let Some(instruction) = parse_discord_instruction_message(&payload, channel_id, bot_user_id)
                                {
                                    if let Some(command) = parse_discord_mention_command(&instruction.text) {
                                        if let Err(err) = handle_discord_mention_command(
                                            client,
                                            bot_token,
                                            channel_id,
                                            &instruction,
                                            command,
                                            terminal_window_id.as_ref(),
                                        )
                                        .await
                                        {
                                            tracing::warn!(error = %err, "failed to handle Discord command");
                                        }
                                        continue;
                                    }
                                    app_event_tx.send(AppEvent::SubmitDiscordUserInput {
                                        text: instruction.text.clone(),
                                    });
                                    let ack = format!(
                                        "Received your instruction and sent it to Codex:\n```\n{}\n```",
                                        truncate_for_discord(&instruction.text, 280)
                                    );
                                    if let Err(err) = send_plain_message(
                                        client,
                                        bot_token,
                                        channel_id,
                                        &ack,
                                        Some(&instruction.message_id),
                                    )
                                    .await
                                    {
                                        tracing::warn!(error = %err, "failed to send Discord instruction acknowledgment");
                                    }
                                }
                            }
                            7 | 9 => {
                                return Err(format!("discord message listener reconnect requested (op={op})"));
                            }
                            10 => {
                                let interval_ms = payload
                                    .get("d")
                                    .and_then(|d| d.get("heartbeat_interval"))
                                    .and_then(Value::as_u64)
                                    .ok_or_else(|| "discord message listener missing heartbeat interval".to_string())?;
                                heartbeat = Some(tokio::time::interval(Duration::from_millis(interval_ms)));

                                if !identified {
                                    let identify = json!({
                                        "op": 2,
                                        "d": {
                                            "token": bot_token,
                                            "intents": DISCORD_MESSAGE_INTENTS,
                                            "properties": {
                                                "os": std::env::consts::OS,
                                                "browser": "codex-tui",
                                                "device": "codex-tui"
                                            }
                                        }
                                    });
                                    websocket
                                        .send(Message::Text(identify.to_string().into()))
                                        .await
                                        .map_err(|err| format!("failed to identify discord message listener: {err}"))?;
                                    identified = true;
                                }
                            }
                            11 => {}
                            _ => {}
                        }
                    }
                    Message::Binary(_) | Message::Frame(_) => {}
                    Message::Ping(payload) => {
                        websocket
                            .send(Message::Pong(payload))
                            .await
                            .map_err(|err| format!("failed to respond to discord message ping: {err}"))?;
                    }
                    Message::Pong(_) => {}
                    Message::Close(close_frame) => {
                        return Err(format!("discord message listener closed: {close_frame:?}"));
                    }
                }
            }
        }
    }
}

fn parse_discord_instruction_message(
    gateway_payload: &Value,
    channel_id: &str,
    bot_user_id: &str,
) -> Option<DiscordInstructionMessage> {
    let message = gateway_payload.get("d")?;
    if message.get("channel_id")?.as_str()? != channel_id {
        return None;
    }
    if message
        .get("author")
        .and_then(|author| author.get("bot"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return None;
    }
    let mentions_bot = message
        .get("mentions")
        .and_then(Value::as_array)
        .is_some_and(|mentions| {
            mentions
                .iter()
                .any(|mention| mention.get("id").and_then(Value::as_str) == Some(bot_user_id))
        });
    if !mentions_bot {
        return None;
    }

    let mut text = message.get("content")?.as_str()?.to_string();
    text = text.replace(&format!("<@{bot_user_id}>"), "");
    text = text.replace(&format!("<@!{bot_user_id}>"), "");
    let text = text
        .trim()
        .trim_start_matches([':', '-', ','])
        .trim()
        .to_string();
    if text.is_empty() {
        return None;
    }

    Some(DiscordInstructionMessage {
        message_id: message.get("id")?.as_str()?.to_string(),
        text,
    })
}

fn parse_discord_mention_command(text: &str) -> Option<DiscordMentionCommand> {
    let command = text.split_whitespace().next()?;
    if !command.starts_with('/') {
        return None;
    }
    Some(match command {
        DISCORD_SCREENSHOT_COMMAND => DiscordMentionCommand::CaptureCurrentScreen,
        _ => DiscordMentionCommand::Unknown {
            command: command.to_string(),
        },
    })
}

async fn handle_discord_mention_command(
    client: &Client,
    bot_token: &str,
    channel_id: &str,
    instruction: &DiscordInstructionMessage,
    command: DiscordMentionCommand,
    terminal_window_id: &AtomicU32,
) -> Result<(), String> {
    match command {
        DiscordMentionCommand::CaptureCurrentScreen => {
            if let Some((snapshot_png, captured_window_id)) =
                capture_terminal_window_screenshot_png(load_window_id(terminal_window_id)).await
            {
                store_window_id(terminal_window_id, captured_window_id);
                send_screenshot_message(
                    client,
                    bot_token,
                    channel_id,
                    "discord-command-cc",
                    snapshot_png,
                )
                .await?;
                send_plain_message(
                    client,
                    bot_token,
                    channel_id,
                    "Captured current screen.",
                    Some(&instruction.message_id),
                )
                .await
                .map_err(|err| format!("failed to send /cc acknowledgment: {err}"))?;
                return Ok(());
            }
            send_plain_message(
                client,
                bot_token,
                channel_id,
                "Could not capture the current screen.",
                Some(&instruction.message_id),
            )
            .await
            .map_err(|err| format!("failed to send /cc failure response: {err}"))?;
            Ok(())
        }
        DiscordMentionCommand::Unknown { command } => {
            let message = format!(
                "Unknown command `{command}`. Supported commands: `{DISCORD_SCREENSHOT_COMMAND}`."
            );
            send_plain_message(
                client,
                bot_token,
                channel_id,
                &message,
                Some(&instruction.message_id),
            )
            .await
            .map_err(|err| format!("failed to send unknown command response: {err}"))?;
            Ok(())
        }
    }
}

#[derive(Serialize)]
struct SendMessageRequest {
    content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    components: Option<Vec<DiscordActionRow>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message_reference: Option<SendMessageReference>,
}

#[derive(Serialize)]
struct SendMessageReference {
    message_id: String,
}

#[derive(Serialize)]
struct DiscordActionRow {
    #[serde(rename = "type")]
    kind: u8,
    components: Vec<DiscordButton>,
}

#[derive(Serialize)]
struct DiscordButton {
    #[serde(rename = "type")]
    kind: u8,
    style: u8,
    label: String,
    custom_id: String,
}

#[derive(Deserialize)]
struct SendMessageResponse {
    id: String,
}

#[derive(Deserialize)]
struct GatewayBotResponse {
    url: String,
}

#[derive(Deserialize)]
struct BotUserResponse {
    id: String,
}

async fn send_message_with_buttons(
    client: &Client,
    bot_token: &str,
    channel_id: &str,
    content: &str,
    choices: &[PendingChoice],
) -> Result<String, String> {
    let components = choices
        .iter()
        .take(5)
        .map(|choice| DiscordActionRow {
            kind: 1,
            components: vec![DiscordButton {
                kind: 2,
                style: button_style_for_shortcut(choice.shortcut),
                label: truncate_for_discord(&format!("{}: {}", choice.shortcut, choice.label), 80),
                custom_id: choice.custom_id.clone(),
            }],
        })
        .collect::<Vec<_>>();
    if choices.len() > 5 {
        tracing::warn!(
            total_options = choices.len(),
            "discord buttons support at most 5 rows; truncating choices"
        );
    }

    let payload = SendMessageRequest {
        content: truncate_for_discord(content, 1900),
        components: Some(components),
        message_reference: None,
    };

    send_message(client, bot_token, channel_id, payload)
        .await
        .map(|response| response.id)
        .map_err(|err| format!("failed to send discord message: {err}"))
}

async fn send_plain_message(
    client: &Client,
    bot_token: &str,
    channel_id: &str,
    content: &str,
    reply_to: Option<&str>,
) -> reqwest::Result<String> {
    send_message(
        client,
        bot_token,
        channel_id,
        SendMessageRequest {
            content: truncate_for_discord(content, 1900),
            components: None,
            message_reference: reply_to.map(|message_id| SendMessageReference {
                message_id: message_id.to_string(),
            }),
        },
    )
    .await
    .map(|response| response.id)
}

async fn send_message(
    client: &Client,
    bot_token: &str,
    channel_id: &str,
    payload: SendMessageRequest,
) -> reqwest::Result<SendMessageResponse> {
    client
        .post(format!("{DISCORD_API_BASE}/channels/{channel_id}/messages"))
        .header("Authorization", format!("Bot {bot_token}"))
        .json(&payload)
        .send()
        .await?
        .error_for_status()?
        .json::<SendMessageResponse>()
        .await
}

async fn send_interaction_callback(
    client: &Client,
    interaction_id: &str,
    interaction_token: &str,
    content: &str,
) -> reqwest::Result<()> {
    client
        .post(format!(
            "{DISCORD_API_BASE}/interactions/{interaction_id}/{interaction_token}/callback"
        ))
        .json(&json!({
            "type": 4,
            "data": {
                "content": truncate_for_discord(content, 1900)
            }
        }))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

async fn fetch_gateway_url(
    client: &Client,
    bot_token: &str,
) -> reqwest::Result<GatewayBotResponse> {
    client
        .get(format!("{DISCORD_API_BASE}/gateway/bot"))
        .header("Authorization", format!("Bot {bot_token}"))
        .send()
        .await?
        .error_for_status()?
        .json::<GatewayBotResponse>()
        .await
}

async fn fetch_bot_user_id(client: &Client, bot_token: &str) -> Result<String, String> {
    client
        .get(format!("{DISCORD_API_BASE}/users/@me"))
        .header("Authorization", format!("Bot {bot_token}"))
        .send()
        .await
        .map_err(|err| format!("failed to fetch Discord bot user profile: {err}"))?
        .error_for_status()
        .map_err(|err| format!("Discord rejected bot user profile request: {err}"))?
        .json::<BotUserResponse>()
        .await
        .map(|response| response.id)
        .map_err(|err| format!("failed to parse Discord bot user profile response: {err}"))
}

async fn fetch_gateway_ws_url(client: &Client, bot_token: &str) -> Result<Url, String> {
    let response = fetch_gateway_url(client, bot_token)
        .await
        .map_err(|err| format!("failed to fetch gateway url: {err}"))?;

    let mut url = Url::parse(&response.url).map_err(|err| format!("invalid gateway url: {err}"))?;
    url.query_pairs_mut()
        .append_pair("v", DISCORD_GATEWAY_VERSION)
        .append_pair("encoding", "json");
    Ok(url)
}

fn button_style_for_shortcut(shortcut: char) -> u8 {
    match shortcut {
        'y' => 3,
        'n' | 'c' => 4,
        'p' | 'a' => 1,
        _ => 2,
    }
}

fn spawn_assistant_message_forwarder(
    enabled: bool,
    client: &Client,
    bot_token: &Option<String>,
    channel_id: &Option<String>,
) -> Option<UnboundedSender<String>> {
    if !enabled {
        return None;
    }
    let (Some(bot_token), Some(channel_id)) = (bot_token.clone(), channel_id.clone()) else {
        return None;
    };

    let client = client.clone();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    tokio::spawn(async move {
        ensure_rustls_crypto_provider();
        let mut pending_messages: Vec<String> = Vec::new();
        let flush_timer = tokio::time::sleep(Duration::from_secs(60 * 60));
        tokio::pin!(flush_timer);
        let mut flush_scheduled = false;

        loop {
            tokio::select! {
                maybe_message = rx.recv() => {
                    let Some(message) = maybe_message else {
                        break;
                    };
                    if let Some(message) = sanitize_assistant_message_for_discord(&message) {
                        pending_messages.push(message);
                        flush_timer.as_mut().reset(Instant::now() + ASSISTANT_MESSAGE_BATCH_DELAY);
                        flush_scheduled = true;
                    }
                }
                _ = &mut flush_timer, if flush_scheduled => {
                    flush_scheduled = false;
                    forward_batched_assistant_messages(
                        &client,
                        &bot_token,
                        &channel_id,
                        &pending_messages,
                    )
                    .await;
                    pending_messages.clear();
                }
            }
        }

        if !pending_messages.is_empty() {
            forward_batched_assistant_messages(&client, &bot_token, &channel_id, &pending_messages)
                .await;
        }
    });
    Some(tx)
}

async fn forward_batched_assistant_messages(
    client: &Client,
    bot_token: &str,
    channel_id: &str,
    messages: &[String],
) {
    for chunk in chunk_discord_messages(messages, DISCORD_MAX_MESSAGE_CHARS) {
        if let Err(err) = send_plain_message(client, bot_token, channel_id, &chunk, None).await {
            tracing::warn!(error = %err, "failed to send assistant update to Discord");
        }
    }
}

fn chunk_discord_messages(messages: &[String], max_chars: usize) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut current = String::new();

    for message in messages {
        let message = message.trim();
        if message.is_empty() {
            continue;
        }

        if current.is_empty() {
            if message.chars().count() <= max_chars {
                current.push_str(message);
            } else {
                chunks.push(truncate_for_discord(message, max_chars));
            }
            continue;
        }

        let candidate = format!("{current}\n\n{message}");
        if candidate.chars().count() <= max_chars {
            current = candidate;
        } else {
            chunks.push(std::mem::take(&mut current));
            if message.chars().count() <= max_chars {
                current = message.to_string();
            } else {
                chunks.push(truncate_for_discord(message, max_chars));
                current.clear();
            }
        }
    }

    if !current.is_empty() {
        chunks.push(current);
    }

    chunks
}

fn sanitize_assistant_message_for_discord(message: &str) -> Option<String> {
    let no_fenced_code = strip_fenced_code_blocks(message);
    let mut cleaned_lines: Vec<String> = Vec::new();
    let mut previous_was_blank = false;

    for line in no_fenced_code.lines() {
        let line = line.trim_end();
        if line.trim().is_empty() {
            if !previous_was_blank {
                cleaned_lines.push(String::new());
            }
            previous_was_blank = true;
            continue;
        }

        previous_was_blank = false;
        cleaned_lines.push(line.to_string());
    }

    let cleaned = cleaned_lines.join("\n").trim().to_string();
    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned)
    }
}

fn strip_fenced_code_blocks(input: &str) -> String {
    let mut in_fence = false;
    let mut fence = "";
    let mut output: Vec<&str> = Vec::new();

    for line in input.lines() {
        let trimmed = line.trim_start();
        let starts_backtick_fence = trimmed.starts_with("```");
        let starts_tilde_fence = trimmed.starts_with("~~~");

        if !in_fence && (starts_backtick_fence || starts_tilde_fence) {
            in_fence = true;
            fence = if starts_backtick_fence { "```" } else { "~~~" };
            continue;
        }

        if in_fence {
            if trimmed.starts_with(fence) {
                in_fence = false;
            }
            continue;
        }

        output.push(line);
    }

    output.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    #[test]
    fn parse_interaction_choice_extracts_matching_custom_id() {
        let choice = PendingChoice {
            request_key: "exec:123".to_string(),
            shortcut: 'y',
            label: "Yes".to_string(),
            custom_id: "custom-1".to_string(),
        };
        let map = HashMap::from([(choice.custom_id.clone(), choice)]);
        let payload = json!({
            "d": {
                "id": "interaction-1",
                "token": "token-1",
                "data": {
                    "custom_id": "custom-1"
                }
            }
        });

        let selected =
            parse_interaction_choice(&payload, &map).expect("expected interaction choice");
        assert_eq!(selected.choice.custom_id, "custom-1");
        assert_eq!(selected.interaction_id, "interaction-1");
        assert_eq!(selected.interaction_token, "token-1");
    }

    #[test]
    fn truncate_for_discord_truncates_long_text() {
        let input = "abcdef";
        assert_eq!(truncate_for_discord(input, 4), "abc…");
        assert_eq!(truncate_for_discord(input, 8), input.to_string());
    }

    #[test]
    fn parse_discord_instruction_message_extracts_mentioned_text() {
        let payload = json!({
            "d": {
                "id": "m-1",
                "channel_id": "chan-1",
                "content": "<@12345> do the thing",
                "mentions": [{"id": "12345"}],
                "author": {"bot": false}
            }
        });

        let parsed = parse_discord_instruction_message(&payload, "chan-1", "12345")
            .expect("expected message parse");
        assert_eq!(parsed.message_id, "m-1");
        assert_eq!(parsed.text, "do the thing");
    }

    #[test]
    fn parse_discord_instruction_message_ignores_non_mentions() {
        let payload = json!({
            "d": {
                "id": "m-2",
                "channel_id": "chan-1",
                "content": "hello bot",
                "mentions": [],
                "author": {"bot": false}
            }
        });

        let parsed = parse_discord_instruction_message(&payload, "chan-1", "12345");
        assert_eq!(parsed, None);
    }

    #[test]
    fn parse_discord_mention_command_recognizes_cc() {
        let command =
            parse_discord_mention_command("/cc please").expect("expected command parsing");
        assert!(matches!(
            command,
            DiscordMentionCommand::CaptureCurrentScreen
        ));
    }

    #[test]
    fn parse_discord_mention_command_returns_unknown_for_other_slash_commands() {
        let command = parse_discord_mention_command("/unknown").expect("expected command parsing");
        assert!(matches!(
            command,
            DiscordMentionCommand::Unknown { command } if command == "/unknown"
        ));
    }

    #[test]
    fn parse_discord_mention_command_ignores_non_commands() {
        let command = parse_discord_mention_command("normal prompt");
        assert!(command.is_none());
    }

    #[test]
    fn sanitize_assistant_message_for_discord_removes_fenced_code_blocks() {
        let input = "Edited `src/main.rs`\n\n```rust\nfn main() {}\n```\n\nDone.";
        let sanitized = sanitize_assistant_message_for_discord(input).expect("expected message");
        assert_eq!(sanitized, "Edited `src/main.rs`\n\nDone.");
    }

    #[test]
    fn chunk_discord_messages_batches_messages_without_exceeding_limit() {
        let chunks = chunk_discord_messages(
            &[
                "First update".to_string(),
                "Second update".to_string(),
                "Third update".to_string(),
            ],
            27,
        );
        assert_eq!(
            chunks,
            vec!["First update\n\nSecond update", "Third update"]
        );
    }
}
