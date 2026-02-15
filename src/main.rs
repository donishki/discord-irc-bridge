use anyhow::{Context, Result};
use futures::pin_mut;
use futures::stream::{StreamExt as _, TryStreamExt as _};
use irc::client::prelude::{Message as IrcMessage, *};
use serde::{Deserialize, Serialize};
use std::iter::Iterator as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use structopt::StructOpt;
use toml;
use twilight_gateway::{parse, EventTypeFlags, Intents, Message, Shard, ShardId};
use twilight_http::Client as HttpClient;
use twilight_model::channel::Message as DiscordMessage;
use twilight_model::gateway::event::{DispatchEvent, GatewayEvent};
use twilight_model::gateway::payload::incoming::MessageCreate;
use twilight_model::id::Id;

mod issue;
mod util;

#[derive(Clone, Debug, StructOpt)]
struct Args {
    #[structopt(
        short = "c",
        long = "config",
        default_value = "config.toml",
        parse(from_os_str)
    )]
    config_path: PathBuf,
}

type IrcTarget = String;
type DiscordTarget = u64;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Default)]
struct DiscordConf {
    token: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Default)]
struct IrcSource {
    from: IrcTarget,
    user: Option<String>,
    to: Vec<DiscordTarget>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Default)]
struct DiscordSource {
    from: DiscordTarget,
    user: Option<String>,
    to: Vec<IrcTarget>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Default)]
struct Mapping {
    irc: Vec<IrcSource>,
    discord: Vec<DiscordSource>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Default)]
struct Misc {
    badwords: Vec<String>,
    repository: String,
    filterchars: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Default)]
struct CombinedConfig {
    irc: Option<Config>,
    discord: Option<DiscordConf>,
    mapping: Mapping,
    misc: Misc,
}

impl CombinedConfig {
    pub fn from_path<T: AsRef<Path>>(p: T) -> Result<Self> {
        use std::fs::File;
        use std::io::Read as _;
        let mut file = File::open(p.as_ref())?;
        let mut data = String::new();
        file.read_to_string(&mut data)?;

        toml::from_str(&data).context("Parsing Combined Config from toml")
    }
}

fn get_irc_messages(msg: &MessageCreate) -> Vec<String> {
    let attachments = if !msg.attachments.is_empty() {
        let mut atts = vec![];
        for a in &msg.attachments {
            atts.push(format!("[Attachment: {} ({})]", a.filename, a.url));
        }
        atts.join("\n")
    } else {
        "".into()
    };
    let mut c = msg.content.clone();

    for u in &msg.mentions {
        c = c.replace(&format!("<@!{}>", u.id), &format!("@{}", u.name));
        c = c.replace(&format!("<@{}>", u.id), &format!("@{}", u.name));
    }

    // TODO: use caching for role and channel names
    // for u in msg.mention_roles.values() {
    //     c = c.replace(&format!("<@!{}>", u.id), &format!("@{}", u.name));
    //     c = c.replace(&format!("<@{}>", u.id), &format!("@{}", u.name));
    // }

    if !attachments.is_empty() {
        if !c.is_empty() {
            c.push('\n');
        }
        c.push_str(&attachments);
    }

    c.lines()
        .map(|l| {
            format!(
                "<\x03{}{}\x03> {}",
                util::colorize(&msg.author.name),
                msg.author.name,
                l
            )
        })
        .collect()
}

fn get_irc_target<'a>(
    sources: &'a [DiscordSource],
    msg: &DiscordMessage,
) -> Option<&'a [IrcTarget]> {
    sources
        .iter()
        .find(|&s| msg.channel_id.get() == s.from)
        .map(|s| &s.to[..])
}

struct IrcMsg {
    user: String,
    target: String,
    content: String,
}

impl IrcMsg {
    fn from_messsage(msg: IrcMessage) -> Self {
        let user = match msg.prefix {
            Some(Prefix::Nickname(uname, _, _)) => uname,
            _ => panic!("Invalid msg (wrong prefix)"),
        };
        let (target, content) = match msg.command {
            Command::PRIVMSG(target, content) => (target, content),
            _ => panic!("Invalid msg (wrong command)"),
        };

        Self {
            user,
            target,
            content,
        }
    }
}

fn get_issues_string<'a>(msg: &'a str, repo: &str) -> impl futures::Stream<Item = String> + 'a {
    issue::get_issues(msg, repo).map(|i| format!("{} | {}", i.title, i.html_url))
}

fn get_discord_message(msg: &IrcMsg) -> String {
    format!("<{}> {}", msg.user, util::remove_formatting(&msg.content))
}

fn get_discord_targets<'a>(sources: &'a [IrcSource], msg: &IrcMsg) -> Option<&'a [DiscordTarget]> {
    sources
        .iter()
        .find(|&s| msg.target == s.from && (s.user.is_none() || s.user.as_ref() == Some(&msg.user)))
        .map(|s| &s.to[..])
}

async fn handle_irc_msg(
    http: Arc<HttpClient>,
    config: Arc<CombinedConfig>,
    tx: Sender,
    msg: IrcMessage,
) {
    let msg = IrcMsg::from_messsage(msg);
    if config
        .misc
        .filterchars
        .chars()
        .any(|c| msg.content.starts_with(c))
        || util::contains_bad_words(&msg.content, &config.misc.badwords)
    {
        return;
    }
    if let Some(targets) = get_discord_targets(&config.mapping.irc, &msg) {
        let discord_msg = get_discord_message(&msg);
        for t in targets {
            let _ = http.create_message(Id::new(*t)).content(&discord_msg).await;
            let issues = get_issues_string(&msg.content, &config.misc.repository);
            pin_mut!(issues);
            while let Some(i) = issues.next().await {
                let _ = tx.send_privmsg(&msg.target, &i);
                let _ = http.create_message(Id::new(*t)).content(&i).await;
            }
        }
    }
}

async fn handle_discord_msg(
    tx: Sender,
    config: Arc<CombinedConfig>,
    http: Arc<HttpClient>,
    msg: Box<MessageCreate>,
) {
    if msg.author.bot {
        return;
    }
    if config
        .misc
        .filterchars
        .chars()
        .any(|c| msg.content.starts_with(c))
        || util::contains_bad_words(&msg.content, &config.misc.badwords)
    {
        return;
    }
    if let Some(targets) = get_irc_target(&config.mapping.discord, &msg) {
        let irc_msgs = get_irc_messages(&msg);
        for t in targets {
            for m in &irc_msgs {
                let _ = tx.send_privmsg(&t, &m);
                let issues = get_issues_string(&msg.content, &config.misc.repository);
                pin_mut!(issues);
                while let Some(i) = issues.next().await {
                    let _ = tx.send_privmsg(&t, &i);
                    let _ = http.create_message(msg.channel_id).content(&i).await;
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::from_args();

    // connect to irc:
    let mut config = CombinedConfig::from_path(args.config_path)?;

    let discord_conf = config.discord.take().unwrap();

    let mut irc_client = Client::from_config(config.irc.take().unwrap()).await?;
    irc_client.identify()?;

    let config = Arc::new(config);

    // connect to discord:
    let http = Arc::new(HttpClient::new(discord_conf.token.clone()));
    let shard = Shard::new(
        ShardId::ONE,
        discord_conf.token,
        Intents::MESSAGE_CONTENT | Intents::GUILD_MESSAGES,
    );

    let tx = irc_client.sender();

    let irc_stream = irc_client
        .stream()?
        .map_err(|e| return anyhow::Error::new(e))
        .filter_map(|r| async move { r.ok() })
        .filter(|msg| futures::future::ready(matches!(msg.command, Command::PRIVMSG(_, _))));

    let discord_stream = shard.fuse().filter_map(|r| async move {
        tracing::info!("Got shard event: {:?}", r);
        let Ok(m) = r else { return None };
        let ge = match m {
            Message::Text(json) => parse(json, EventTypeFlags::MESSAGE_CREATE).ok().flatten(),
            _ => return None,
        };
        match ge {
            Some(GatewayEvent::Dispatch(_, DispatchEvent::MessageCreate(msg))) => Some(msg),
            _ => return None,
        }
    });

    pin_mut!(irc_stream, discord_stream);

    loop {
        futures::select! {
            msg = irc_stream.select_next_some() => {
                tracing::info!("Got irc message: {:?}", msg);
                tokio::spawn(handle_irc_msg(http.clone(), config.clone(), tx.clone(), msg));
            },
            msg = discord_stream.select_next_some() => {
                tokio::spawn(handle_discord_msg(tx.clone(), config.clone(), http.clone(), msg));
            },
            complete => break,
        };
    }

    Ok(())
}
