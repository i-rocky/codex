use crate::app_event::AppEvent;
use crate::app_event_sender::AppEventSender;
use reqwest::Client;
use serde::Deserialize;
use serde::Serialize;
use std::time::Duration;

const DISCORD_API_BASE: &str = "https://discord.com/api/v10";

#[derive(Clone)]
pub(crate) struct DiscordApprovalBridge {
    enabled: bool,
    client: Client,
    bot_token: Option<String>,
    channel_id: Option<String>,
    app_event_tx: AppEventSender,
}

impl DiscordApprovalBridge {
    pub(crate) fn new(enabled: bool, app_event_tx: AppEventSender) -> Self {
        Self {
            enabled,
            client: Client::new(),
            bot_token: std::env::var("CODEX_DISCORD_BOT_TOKEN").ok(),
            channel_id: std::env::var("CODEX_DISCORD_CHANNEL_ID").ok(),
            app_event_tx,
        }
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
        let client = self.client.clone();
        let app_event_tx = self.app_event_tx.clone();
        tokio::spawn(async move {
            let options_text = options
                .iter()
                .map(|(shortcut, label)| format!("- {shortcut}: {label}"))
                .collect::<Vec<_>>()
                .join("\n");
            let content = format!(
                "**Codex approval requested**\n\nRequest key: `{request_key}`\n{title}\n\n```\n{details}\n```\nOptions:\n{options_text}\n\nReply to this message with one of: `{}`",
                options
                    .iter()
                    .map(|(shortcut, _)| shortcut.to_string())
                    .collect::<Vec<_>>()
                    .join("`, `")
            );

            let sent = send_message(&client, &bot_token, &channel_id, content).await;
            let Ok(message_id) = sent else {
                tracing::warn!("failed to post approval prompt to discord");
                return;
            };

            for _ in 0..900 {
                tokio::time::sleep(Duration::from_secs(2)).await;
                let Ok(messages) =
                    fetch_messages(&client, &bot_token, &channel_id, &message_id).await
                else {
                    continue;
                };
                if let Some(shortcut) = parse_discord_shortcut(&messages, &message_id, &options) {
                    app_event_tx.send(AppEvent::DiscordApprovalShortcut {
                        request_key: request_key.clone(),
                        shortcut,
                    });
                    return;
                }
            }
        });
    }
}

#[derive(Serialize)]
struct SendMessageRequest {
    content: String,
}

#[derive(Deserialize)]
struct SendMessageResponse {
    id: String,
}

#[derive(Deserialize)]
struct DiscordMessage {
    content: String,
    #[serde(default)]
    message_reference: Option<MessageReference>,
    author: DiscordAuthor,
}

#[derive(Deserialize)]
struct MessageReference {
    #[serde(default)]
    message_id: Option<String>,
}

#[derive(Deserialize)]
struct DiscordAuthor {
    bot: Option<bool>,
}

async fn send_message(
    client: &Client,
    bot_token: &str,
    channel_id: &str,
    content: String,
) -> reqwest::Result<String> {
    let response = client
        .post(format!("{DISCORD_API_BASE}/channels/{channel_id}/messages"))
        .bearer_auth(bot_token)
        .json(&SendMessageRequest { content })
        .send()
        .await?
        .error_for_status()?;
    let body = response.json::<SendMessageResponse>().await?;
    Ok(body.id)
}

async fn fetch_messages(
    client: &Client,
    bot_token: &str,
    channel_id: &str,
    after_message_id: &str,
) -> reqwest::Result<Vec<DiscordMessage>> {
    client
        .get(format!(
            "{DISCORD_API_BASE}/channels/{channel_id}/messages?limit=20&after={after_message_id}"
        ))
        .bearer_auth(bot_token)
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<DiscordMessage>>()
        .await
}

fn parse_discord_shortcut(
    messages: &[DiscordMessage],
    prompt_message_id: &str,
    options: &[(char, String)],
) -> Option<char> {
    let valid = options.iter().map(|(ch, _)| *ch).collect::<Vec<_>>();
    messages.iter().find_map(|message| {
        if message.author.bot.unwrap_or(false) {
            return None;
        }
        let refers_to_prompt = message
            .message_reference
            .as_ref()
            .and_then(|reference| reference.message_id.as_deref())
            .is_some_and(|message_id| message_id == prompt_message_id);
        if !refers_to_prompt {
            return None;
        }
        let content = message.content.trim().to_lowercase();
        let shortcut = if content.len() == 1 {
            content.chars().next()
        } else {
            content
                .split_whitespace()
                .next()
                .and_then(|word| word.chars().next())
        };
        if let Some(shortcut) = shortcut
            && valid.contains(&shortcut)
        {
            return Some(shortcut);
        }
        None
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn parse_discord_shortcut_uses_reply_reference() {
        let messages = vec![DiscordMessage {
            content: "y".to_string(),
            message_reference: Some(MessageReference {
                message_id: Some("42".to_string()),
            }),
            author: DiscordAuthor { bot: Some(false) },
        }];
        let options = vec![('y', "Yes".to_string()), ('n', "No".to_string())];
        assert_eq!(parse_discord_shortcut(&messages, "42", &options), Some('y'));
    }
    #[test]
    fn parse_discord_shortcut_ignores_non_replies_and_bots() {
        let messages = vec![
            DiscordMessage {
                content: "y".to_string(),
                message_reference: None,
                author: DiscordAuthor { bot: Some(false) },
            },
            DiscordMessage {
                content: "y".to_string(),
                message_reference: Some(MessageReference {
                    message_id: Some("42".to_string()),
                }),
                author: DiscordAuthor { bot: Some(true) },
            },
        ];
        let options = vec![('y', "Yes".to_string()), ('n', "No".to_string())];
        assert_eq!(parse_discord_shortcut(&messages, "42", &options), None);
    }

    #[test]
    fn parse_discord_shortcut_accepts_word_replies() {
        let messages = vec![DiscordMessage {
            content: "yes, proceed".to_string(),
            message_reference: Some(MessageReference {
                message_id: Some("42".to_string()),
            }),
            author: DiscordAuthor { bot: Some(false) },
        }];
        let options = vec![('y', "Yes".to_string()), ('n', "No".to_string())];
        assert_eq!(parse_discord_shortcut(&messages, "42", &options), Some('y'));
    }
}
