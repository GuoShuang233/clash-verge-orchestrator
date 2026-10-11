//! Subscription orchestration primitives.
//!
//! The module keeps source subscriptions and user bindings separate from the generated
//! mihomo configuration. This makes upstream updates and configuration mode switches
//! deterministic without rewriting the downloaded profile.

use anyhow::{Context as _, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use percent_encoding::percent_decode_str;
use reqwest::{HeaderMap, header::{ETAG, IF_NONE_MATCH, IF_MODIFIED_SINCE, LAST_MODIFIED}};
use reqwest_dav::re_exports::url::Url;
use serde::{Deserialize, Serialize};
use serde_yaml_ng::{Mapping, Value};
use sha2::{Digest as _, Sha256};
use smartstring::alias::String as SmartString;
use std::collections::{BTreeMap, BTreeSet};

use crate::core::validate::CoreConfigValidator;
use crate::utils::network::{NetworkManager, ProxyType};
use crate::utils::yaml_emitter::to_mihomo_config_string;
use clash_verge_logging::{Type, logging};

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum GroupMode {
    #[default]
    Upstream,
    Hybrid,
    Custom,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct NodeRef {
    pub source_id: String,
    pub node_id: String,
    pub name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct GroupBinding {
    #[serde(default)]
    pub mode: Option<GroupMode>,
    #[serde(default)]
    pub upstream_name: Option<String>,
    #[serde(default)]
    pub nodes: Vec<NodeRef>,
    #[serde(default)]
    pub custom: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct OrchestrationState {
    #[serde(default)]
    pub sources: Vec<SubscriptionSource>,
    #[serde(default)]
    pub active_mode: GroupMode,
    #[serde(default)]
    pub default_mode: GroupMode,
    #[serde(default)]
    pub groups: BTreeMap<String, GroupBinding>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourceInput {
    pub source_id: String,
    pub content: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SubscriptionSource {
    pub source_id: String,
    pub name: String,
    pub url: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct SourceCache {
    content: String,
    fetched_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_modified: Option<String>,
}

const fn default_true() -> bool {
    true
}

#[derive(Debug, Clone)]
pub struct ProxyNode {
    pub source_id: String,
    pub node_id: String,
    pub name: String,
    pub value: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct NodeSummary {
    pub source_id: String,
    pub node_id: String,
    pub name: String,
    pub proxy_type: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceFetchResult {
    pub nodes: Vec<NodeSummary>,
    pub used_cache: bool,
    pub fetched_at: i64,
}

#[derive(Debug, Clone)]
pub struct SourceContentResult {
    pub content: String,
    pub used_cache: bool,
    pub fetched_at: i64,
}

pub fn node_summaries(parsed: &ParsedSubscription) -> Vec<NodeSummary> {
    parsed
        .nodes
        .iter()
        .map(|node| NodeSummary {
            source_id: node.source_id.clone(),
            node_id: node.node_id.clone(),
            name: node.name.clone(),
            proxy_type: node
                .value
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_owned(),
        })
        .collect()
}

#[derive(Debug, Clone)]
pub struct ParsedSubscription {
    pub source_id: String,
    pub config: Value,
    pub nodes: Vec<ProxyNode>,
}

pub fn state_path(profile_uid: &str) -> Result<std::path::PathBuf> {
    let profile_uid = profile_uid.trim();
    if profile_uid.is_empty() || profile_uid.chars().any(|ch| matches!(ch, '/' | '\\' | ':')) {
        bail!("invalid profile uid");
    }
    Ok(crate::utils::dirs::app_home_dir()?
        .join("orchestration")
        .join(format!("{profile_uid}.yaml")))
}

pub async fn load_state(profile_uid: &str) -> Result<OrchestrationState> {
    let path = state_path(profile_uid)?;
    match crate::utils::help::read_yaml(&path).await {
        Ok(state) => Ok(state),
        Err(error) if error.to_string().contains("file not found") => Ok(OrchestrationState::default()),
        Err(error) => Err(error),
    }
}

pub async fn save_state(profile_uid: &str, state: &OrchestrationState) -> Result<()> {
    let path = state_path(profile_uid)?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    crate::utils::help::save_yaml(&path, state, Some("# Subscription orchestration state")).await
}

fn cache_path(profile_uid: &str, source_id: &str) -> Result<std::path::PathBuf> {
    let profile_uid = profile_uid.trim();
    let source_id = source_id.trim();
    if profile_uid.is_empty()
        || source_id.is_empty()
        || profile_uid.chars().any(|ch| matches!(ch, '/' | '\\' | ':'))
        || source_id.chars().any(|ch| matches!(ch, '/' | '\\' | ':'))
    {
        bail!("invalid orchestration cache key");
    }
    Ok(crate::utils::dirs::app_home_dir()?
        .join("orchestration")
        .join("cache")
        .join(profile_uid)
        .join(format!("{source_id}.yaml")))
}

async fn load_source_cache(profile_uid: &str, source_id: &str) -> Result<Option<SourceCache>> {
    let path = cache_path(profile_uid, source_id)?;
    match crate::utils::help::read_yaml(&path).await {
        Ok(cache) => Ok(Some(cache)),
        Err(error) if error.to_string().contains("file not found") => Ok(None),
        Err(error) => Err(error),
    }
}

async fn save_source_cache(
    profile_uid: &str,
    source: &SubscriptionSource,
    response: &crate::utils::network::HttpResponse,
) -> Result<i64> {
    let path = cache_path(profile_uid, &source.source_id)?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let fetched_at = chrono::Utc::now().timestamp();
    let cache = SourceCache {
        content: response.text().to_owned(),
        fetched_at,
        etag: response.headers().get(ETAG).and_then(|value| value.to_str().ok()).map(str::to_owned),
        last_modified: response
            .headers()
            .get(LAST_MODIFIED)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
    };
    crate::utils::help::save_yaml(&path, &cache, Some("# Subscription orchestration cache")).await?;
    Ok(fetched_at)
}

pub async fn fetch_source_content(
    profile_uid: &str,
    source: &SubscriptionSource,
    timeout_seconds: Option<u64>,
) -> Result<SourceContentResult> {
    let cache = load_source_cache(profile_uid, &source.source_id).await?;
    let mut request_headers = HeaderMap::new();
    if let Some(value) = cache.as_ref().and_then(|cache| cache.etag.as_deref()) {
        request_headers.insert(IF_NONE_MATCH, value.parse()?);
    }
    if let Some(value) = cache.as_ref().and_then(|cache| cache.last_modified.as_deref()) {
        request_headers.insert(IF_MODIFIED_SINCE, value.parse()?);
    }
    let response = NetworkManager::new()
        .get_with_headers(
            &source.url,
            ProxyType::Localhost,
            timeout_seconds.or(Some(30)),
            source.user_agent.clone().map(SmartString::from),
            false,
            request_headers,
        )
        .await;

    match response {
        Ok(response) if response.status() == reqwest::StatusCode::NOT_MODIFIED => {
            let cache = cache.context("subscription returned 304 but no cache is available")?;
            logging!(info, Type::Config, "orchestration source {} unchanged since {}", source.source_id, cache.fetched_at);
            Ok(SourceContentResult { content: cache.content, used_cache: true, fetched_at: cache.fetched_at })
        }
        Ok(response) if response.status().is_success() => {
            let content = response.text().to_owned();
            let fetched_at = save_source_cache(profile_uid, source, &response).await?;
            Ok(SourceContentResult {
                content,
                used_cache: false,
                fetched_at,
            })
        }
        Ok(response) => {
            let reason = format!("HTTP status {}", response.status());
            if let Some(cache) = cache {
                logging!(
                    warn,
                    Type::Config,
                    "orchestration source {} unavailable ({reason}); using cached content from {}",
                    source.source_id,
                    cache.fetched_at
                );
                Ok(SourceContentResult {
                    content: cache.content,
                    used_cache: true,
                    fetched_at: cache.fetched_at,
                })
            } else {
                bail!(
                    "orchestration source {} returned {reason} and has no cache",
                    source.source_id
                );
            }
        }
        Err(error) => {
            if let Some(cache) = load_source_cache(profile_uid, &source.source_id).await? {
                logging!(
                    warn,
                    Type::Config,
                    "orchestration source {} failed ({error:#}); using cached content from {}",
                    source.source_id,
                    cache.fetched_at
                );
                Ok(SourceContentResult {
                    content: cache.content,
                    used_cache: true,
                    fetched_at: cache.fetched_at,
                })
            } else {
                Err(error).with_context(|| {
                    format!(
                        "failed to fetch orchestration source {} and no cache is available",
                        source.source_id
                    )
                })
            }
        }
    }
}

/// Applies the saved orchestration state to the generated profile configuration.
///
/// The source subscriptions are fetched only when there is an active binding. A failed
/// external source is returned to the caller so the runtime generation layer can keep the
/// last valid upstream configuration instead of replacing it with a partial result.
pub async fn apply_to_profile_config(profile_uid: &str, config: &mut Mapping) -> Result<()> {
    let state = load_state(profile_uid).await?;
    if state.active_mode == GroupMode::Upstream
        && !state
            .groups
            .values()
            .any(|binding| binding.mode.is_some_and(|mode| mode != GroupMode::Upstream) && !binding.nodes.is_empty())
    {
        return Ok(());
    }

    let mut parsed_sources = Vec::new();
    for source in state.sources.iter().filter(|source| source.enabled) {
        let fetched = fetch_source_content(profile_uid, source, Some(30)).await?;
        parsed_sources.push(parse_subscription(&source.source_id, &fetched.content)?);
    }

    let mut value = Value::Mapping(config.clone());
    compile_value(&mut value, &parsed_sources, &state)?;
    let generated = value
        .as_mapping()
        .cloned()
        .context("generated orchestration config is not a mapping")?;
    let yaml = to_mihomo_config_string(&Value::Mapping(generated.clone()))
        .context("failed to serialize generated orchestration config")?;
    let validation = CoreConfigValidator::global()
        .validate_config_outcome_with(&yaml)
        .await
        .context("failed to validate generated orchestration config")?;
    if !validation.is_valid() {
        bail!("generated orchestration config failed Mihomo validation: {validation}");
    }
    *config = generated;
    Ok(())
}

pub fn parse_subscription(source_id: &str, content: &str) -> Result<ParsedSubscription> {
    let source_id = source_id.trim();
    if source_id.is_empty() {
        bail!("subscription source id is empty");
    }

    let value = parse_content(content)?;
    let config = match value {
        Value::Mapping(_) => value,
        Value::Sequence(nodes) => {
            let mut config = Mapping::new();
            config.insert(Value::String("proxies".into()), Value::Sequence(nodes));
            Value::Mapping(config)
        }
        _ => bail!("subscription content is not a YAML mapping or node list"),
    };

    let nodes_value = config
        .get("proxies")
        .and_then(Value::as_sequence)
        .context("subscription does not contain a proxies list")?;

    let mut nodes = Vec::with_capacity(nodes_value.len());
    for node in nodes_value {
        let mapping = node.as_mapping().context("proxy entry is not a mapping")?;
        let name = mapping
            .get("name")
            .and_then(Value::as_str)
            .context("proxy entry does not contain a name")?
            .to_owned();
        let node_id = stable_node_id(source_id, node)?;
        nodes.push(ProxyNode {
            source_id: source_id.to_owned(),
            node_id,
            name,
            value: node.clone(),
        });
    }

    Ok(ParsedSubscription {
        source_id: source_id.to_owned(),
        config,
        nodes,
    })
}

pub fn stable_node_id(source_id: &str, node: &Value) -> Result<String> {
    let identity = if let Some(mapping) = node.as_mapping() {
        let mut mapping = mapping.clone();
        mapping.remove(Value::String("name".into()));
        Value::Mapping(mapping)
    } else {
        node.clone()
    };
    let mut digest = Sha256::new();
    digest.update(source_id.as_bytes());
    digest.update([0]);
    digest.update(serde_yaml_ng::to_string(&identity)?);
    Ok(digest.finalize().iter().map(|byte| format!("{byte:02x}")).collect())
}

pub fn compile_config(main_config: &str, sources: &[SourceInput], state: &OrchestrationState) -> Result<String> {
    let mut config: Value = serde_yaml_ng::from_str(main_config).context("main profile is not valid YAML")?;
    let parsed = sources
        .iter()
        .map(|source| parse_subscription(&source.source_id, &source.content))
        .collect::<Result<Vec<_>>>()?;
    compile_value(&mut config, &parsed, state)?;
    crate::utils::yaml_emitter::to_mihomo_config_string(&config).context("failed to serialize generated config")
}

pub fn compile_value(config: &mut Value, sources: &[ParsedSubscription], state: &OrchestrationState) -> Result<()> {
    let config_mapping = config.as_mapping_mut().context("main profile is not a YAML mapping")?;
    if state.active_mode == GroupMode::Upstream {
        return Ok(());
    }
    let node_index = build_node_index(sources);
    let mut injected = BTreeSet::new();

    for binding in state
        .groups
        .values()
        .filter(|binding| binding.mode.unwrap_or(state.active_mode) != GroupMode::Upstream)
    {
        for node_ref in &binding.nodes {
            let node = node_index
                .get(&(node_ref.source_id.clone(), node_ref.node_id.clone()))
                .with_context(|| {
                    format!(
                        "orchestration node binding is missing: source={} node={}",
                        node_ref.source_id, node_ref.node_id
                    )
                })?;
            injected.insert(node_ref.name.clone());
            let mut value = node.value.clone();
            if let Some(mapping) = value.as_mapping_mut() {
                mapping.insert(Value::String("name".into()), Value::String(node_ref.name.clone()));
            }
            append_proxy(config_mapping, value)?;
        }
    }

    for (group_name, binding) in &state.groups {
        let mode = binding.mode.unwrap_or(state.active_mode);
        if mode == GroupMode::Upstream {
            continue;
        }

        let target_name = binding.upstream_name.as_deref().unwrap_or(group_name);
        let group = find_or_create_group(config_mapping, target_name)?;
        match mode {
            GroupMode::Upstream => {}
            GroupMode::Hybrid => {
                let group = group.as_mapping_mut().context("proxy group is not a mapping")?;
                for node_ref in &binding.nodes {
                    if injected.contains(&node_ref.name) {
                        append_group_proxy(group, &node_ref.name)?;
                    }
                }
            }
            GroupMode::Custom => {
                let custom = binding
                    .custom
                    .as_ref()
                    .context("custom group mode requires a custom group")?;
                *group = custom.clone();
                let group = group.as_mapping_mut().context("custom proxy group is not a mapping")?;
                for node_ref in &binding.nodes {
                    if injected.contains(&node_ref.name) {
                        append_group_proxy(group, &node_ref.name)?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn parse_content(content: &str) -> Result<Value> {
    let content = content.trim();
    if content.is_empty() {
        bail!("subscription content is empty");
    }

    if let Ok(value) = serde_yaml_ng::from_str::<Value>(content)
        && yaml_contains_proxy_data(&value)
    {
        return normalize_yaml_value(value);
    }

    if looks_like_uri_list(content) {
        return parse_uri_list(content);
    }

    let compact: String = content.chars().filter(|ch| !ch.is_whitespace()).collect();
    let decoded = STANDARD
        .decode(&compact)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(&compact))
        .context("subscription is neither supported YAML nor Base64 URI content")?;
    let decoded = String::from_utf8(decoded).context("Base64 subscription is not UTF-8")?;
    parse_content(&decoded)
}

fn yaml_contains_proxy_data(value: &Value) -> bool {
    match value {
        Value::Mapping(mapping) => mapping.contains_key("proxies"),
        Value::Sequence(items) => !items.is_empty(),
        _ => false,
    }
}

fn normalize_yaml_value(value: Value) -> Result<Value> {
    let Value::Sequence(items) = value else {
        return Ok(value);
    };
    if items.iter().all(|item| item.as_str().is_some()) {
        let uris = items.iter().filter_map(Value::as_str).collect::<Vec<_>>().join("\n");
        return parse_uri_list(&uris);
    }
    Ok(Value::Sequence(items))
}

fn looks_like_uri_list(content: &str) -> bool {
    content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .all(|line| {
            line.starts_with("vless://")
                || line.starts_with("vmess://")
                || line.starts_with("trojan://")
                || line.starts_with("ss://")
        })
}

fn parse_uri_list(content: &str) -> Result<Value> {
    let nodes = content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(parse_node_uri)
        .collect::<Result<Vec<_>>>()?;
    if nodes.is_empty() {
        bail!("URI subscription contains no nodes");
    }
    let mut mapping = Mapping::new();
    mapping.insert(Value::String("proxies".into()), Value::Sequence(nodes));
    Ok(Value::Mapping(mapping))
}

fn parse_node_uri(uri: &str) -> Result<Value> {
    let parsed = Url::parse(uri).with_context(|| format!("invalid proxy URI scheme in {uri:?}"))?;
    match parsed.scheme() {
        "vless" => parse_vless_uri(&parsed),
        "vmess" => parse_vmess_uri(&parsed),
        "trojan" => parse_trojan_uri(&parsed),
        "ss" => parse_ss_uri(&parsed),
        scheme => bail!("unsupported proxy URI scheme {scheme:?}"),
    }
}

fn uri_name(url: &Url, fallback: &str) -> String {
    url.fragment()
        .map(|fragment| percent_decode_str(fragment).decode_utf8_lossy().into_owned())
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| fallback.to_owned())
}

fn query_value(url: &Url, key: &str) -> Option<String> {
    url.query_pairs()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.into_owned())
}

fn required_host_port(url: &Url) -> Result<(String, u16)> {
    let host = url.host_str().context("proxy URI has no server host")?.to_owned();
    let port = url.port().context("proxy URI has no port")?;
    Ok((host, port))
}

fn parse_vless_uri(url: &Url) -> Result<Value> {
    let (server, port) = required_host_port(url)?;
    let uuid = percent_decode_str(url.username()).decode_utf8_lossy().into_owned();
    if uuid.is_empty() {
        bail!("VLESS URI has no UUID");
    }
    let mut node = Mapping::new();
    node.insert("name".into(), uri_name(url, "VLESS").into());
    node.insert("type".into(), "vless".into());
    node.insert("server".into(), server.into());
    node.insert("port".into(), port.into());
    node.insert("uuid".into(), uuid.into());

    let security = query_value(url, "security").unwrap_or_default();
    if security == "tls" || query_value(url, "tls").as_deref() == Some("1") {
        node.insert("tls".into(), true.into());
    }
    for (query_key, node_key) in [("sni", "servername"), ("flow", "flow"), ("fp", "client-fingerprint")] {
        if let Some(value) = query_value(url, query_key) {
            node.insert(node_key.into(), value.into());
        }
    }

    let network = query_value(url, "type").or_else(|| query_value(url, "network"));
    if let Some(network) = network {
        node.insert("network".into(), network.clone().into());
        if network == "ws" {
            let mut ws = Mapping::new();
            if let Some(path) = query_value(url, "path") {
                ws.insert("path".into(), path.into());
            }
            if let Some(host) = query_value(url, "host") {
                let mut headers = Mapping::new();
                headers.insert("Host".into(), host.into());
                ws.insert("headers".into(), headers.into());
            }
            node.insert("ws-opts".into(), ws.into());
        }
    }
    Ok(node.into())
}

fn parse_vmess_uri(url: &Url) -> Result<Value> {
    let encoded = url
        .host_str()
        .filter(|value| !value.is_empty())
        .or_else(|| Some(url.path().trim_start_matches('/')))
        .unwrap_or_default();
    let decoded = STANDARD
        .decode(encoded)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(encoded))
        .context("VMess URI payload is not valid Base64")?;
    let json: serde_json::Value = serde_json::from_slice(&decoded).context("VMess URI payload is not JSON")?;
    let object = json.as_object().context("VMess URI payload is not an object")?;
    let mut node = Mapping::new();
    node.insert(
        "name".into(),
        object
            .get("ps")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("VMess")
            .into(),
    );
    node.insert("type".into(), "vmess".into());
    node.insert(
        "server".into(),
        object
            .get("add")
            .and_then(serde_json::Value::as_str)
            .context("VMess has no server")?
            .into(),
    );
    node.insert(
        "port".into(),
        object.get("port").map(json_to_yaml).context("VMess has no port")?,
    );
    node.insert(
        "uuid".into(),
        object
            .get("id")
            .and_then(serde_json::Value::as_str)
            .context("VMess has no UUID")?
            .into(),
    );
    if let Some(value) = object.get("aid").map(json_to_yaml) {
        node.insert("alterId".into(), value);
    }
    if let Some(value) = object.get("scy").map(json_to_yaml) {
        node.insert("cipher".into(), value);
    }
    if object
        .get("tls")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| !value.is_empty())
    {
        node.insert("tls".into(), true.into());
    }
    if let Some(network) = object.get("net").and_then(serde_json::Value::as_str) {
        node.insert("network".into(), network.into());
        if network == "ws" {
            let mut ws = Mapping::new();
            if let Some(path) = object.get("path").and_then(serde_json::Value::as_str) {
                ws.insert("path".into(), path.into());
            }
            if let Some(host) = object.get("host").and_then(serde_json::Value::as_str) {
                let mut headers = Mapping::new();
                headers.insert("Host".into(), host.into());
                ws.insert("headers".into(), headers.into());
            }
            node.insert("ws-opts".into(), ws.into());
        }
    }
    Ok(node.into())
}

fn parse_trojan_uri(url: &Url) -> Result<Value> {
    let (server, port) = required_host_port(url)?;
    let password = percent_decode_str(url.username()).decode_utf8_lossy().into_owned();
    let mut node = Mapping::new();
    node.insert("name".into(), uri_name(url, "Trojan").into());
    node.insert("type".into(), "trojan".into());
    node.insert("server".into(), server.into());
    node.insert("port".into(), port.into());
    node.insert("password".into(), password.into());
    if query_value(url, "security").as_deref() == Some("tls") || query_value(url, "tls").as_deref() == Some("1") {
        node.insert("tls".into(), true.into());
    }
    if let Some(sni) = query_value(url, "sni") {
        node.insert("sni".into(), sni.into());
    }
    Ok(node.into())
}

fn parse_ss_uri(url: &Url) -> Result<Value> {
    let (server, port) = required_host_port(url)?;
    let credentials = percent_decode_str(url.username()).decode_utf8_lossy().into_owned();
    let credentials = STANDARD
        .decode(&credentials)
        .ok()
        .and_then(|value| String::from_utf8(value).ok())
        .unwrap_or(credentials);
    let (cipher, password) = credentials
        .split_once(':')
        .context("SS URI credentials must be cipher:password or Base64(cipher:password)")?;
    let mut node = Mapping::new();
    node.insert("name".into(), uri_name(url, "Shadowsocks").into());
    node.insert("type".into(), "ss".into());
    node.insert("server".into(), server.into());
    node.insert("port".into(), port.into());
    node.insert(
        "cipher".into(),
        percent_decode_str(cipher).decode_utf8_lossy().into_owned().into(),
    );
    node.insert(
        "password".into(),
        percent_decode_str(password).decode_utf8_lossy().into_owned().into(),
    );
    Ok(node.into())
}

fn json_to_yaml(value: &serde_json::Value) -> Value {
    serde_yaml_ng::to_value(value).unwrap_or(Value::Null)
}

fn build_node_index(sources: &[ParsedSubscription]) -> BTreeMap<(String, String), ProxyNode> {
    sources
        .iter()
        .flat_map(|source| source.nodes.iter().cloned())
        .map(|node| ((node.source_id.clone(), node.node_id.clone()), node))
        .collect()
}

fn append_proxy(config: &mut Mapping, proxy: Value) -> Result<()> {
    let proxies = config
        .entry(Value::String("proxies".into()))
        .or_insert_with(|| Value::Sequence(Vec::new()));
    let proxies = proxies
        .as_sequence_mut()
        .context("main profile proxies is not a sequence")?;
    let proxy_name = proxy.get("name").cloned();
    proxies.retain(|existing| existing.get("name") != proxy_name.as_ref());
    proxies.push(proxy);
    Ok(())
}

fn find_or_create_group<'a>(config: &'a mut Mapping, name: &str) -> Result<&'a mut Value> {
    let groups = config
        .entry(Value::String("proxy-groups".into()))
        .or_insert_with(|| Value::Sequence(Vec::new()));
    let groups = groups
        .as_sequence_mut()
        .context("main profile proxy-groups is not a sequence")?;
    if let Some(index) = groups
        .iter()
        .position(|group| group.get("name").and_then(Value::as_str) == Some(name))
    {
        return Ok(&mut groups[index]);
    }
    let mut group = Mapping::new();
    group.insert(Value::String("name".into()), Value::String(name.into()));
    group.insert(Value::String("type".into()), Value::String("select".into()));
    group.insert(Value::String("proxies".into()), Value::Sequence(Vec::new()));
    groups.push(Value::Mapping(group));
    groups.last_mut().context("failed to create proxy group")
}

fn append_group_proxy(group: &mut Mapping, name: &str) -> Result<()> {
    let proxies = group
        .entry(Value::String("proxies".into()))
        .or_insert_with(|| Value::Sequence(Vec::new()));
    let proxies = proxies
        .as_sequence_mut()
        .context("proxy group proxies must be a sequence")?;
    if !proxies.iter().any(|value| value.as_str() == Some(name)) {
        proxies.push(Value::String(name.into()));
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    const MAIN: &str = "proxies:\n  - name: ytoo-us\n    type: ss\n    server: ytoo.example\n    port: 443\nproxy-groups:\n  - name: US\n    type: select\n    proxies:\n      - ytoo-us\n";

    const EXTERNAL: &str = "proxies:\n  - name: moyu-node\n    type: vless\n    server: xray.example\n    port: 443\n    uuid: test\n    tls: true\n    network: ws\n    ws-opts:\n      path: /test/\n";

    fn state(mode: GroupMode) -> OrchestrationState {
        let node = parse_subscription("moyu", EXTERNAL)
            .expect("test subscription must parse")
            .nodes
            .remove(0);
        OrchestrationState {
            sources: vec![],
            active_mode: mode,
            default_mode: mode,
            groups: BTreeMap::from([(
                "US".into(),
                GroupBinding {
                    mode: Some(mode),
                    upstream_name: Some("US".into()),
                    nodes: vec![NodeRef {
                        source_id: node.source_id,
                        node_id: node.node_id,
                        name: "[Moyu] moyu-node".into(),
                    }],
                    custom: (mode == GroupMode::Custom)
                        .then(|| serde_yaml_ng::from_str("{name: US, type: select, proxies: [ytoo-us]}").unwrap()),
                },
            )]),
        }
    }

    #[test]
    fn node_id_ignores_display_name_changes() {
        let first = parse_subscription(
            "moyu",
            "proxies:
  - name: first
    type: vless
    server: example.test
    port: 443
    uuid: test
",
        )
        .unwrap();
        let second = parse_subscription(
            "moyu",
            "proxies:
  - name: renamed
    type: vless
    server: example.test
    port: 443
    uuid: test
",
        )
        .unwrap();
        assert_eq!(first.nodes[0].node_id, second.nodes[0].node_id);
    }

    #[test]
    fn parses_yaml_and_keeps_protocol_fields() {
        let parsed = parse_subscription("moyu", EXTERNAL).unwrap();
        assert_eq!(parsed.nodes.len(), 1);
        assert_eq!(parsed.nodes[0].value.get("network").and_then(Value::as_str), Some("ws"));
        assert_eq!(parsed.nodes[0].node_id.len(), 64);
    }

    #[test]
    fn hybrid_mode_appends_external_node_to_upstream_group() {
        let output = compile_config(
            MAIN,
            &[SourceInput {
                source_id: "moyu".into(),
                content: EXTERNAL.into(),
            }],
            &state(GroupMode::Hybrid),
        )
        .unwrap();
        assert!(output.contains("[Moyu] moyu-node"));
        assert!(output.contains("name: US"));
    }

    #[test]
    fn custom_mode_replaces_group_shape_but_keeps_bound_nodes() {
        let output = compile_config(
            MAIN,
            &[SourceInput {
                source_id: "moyu".into(),
                content: EXTERNAL.into(),
            }],
            &state(GroupMode::Custom),
        )
        .unwrap();
        assert!(output.contains("type: select"));
        assert!(output.contains("[Moyu] moyu-node"));
    }

    #[test]
    fn parses_vless_ws_uri_without_dropping_transport_fields() {
        let parsed = parse_subscription(
            "moyu",
            "vless://00000000-0000-0000-0000-000000000000@example.test:443?security=tls&type=ws&sni=example.test&fp=chrome&path=%2Fws%2F&host=cdn.example.test#Moyu%20WS",
        )
        .unwrap();
        let node = &parsed.nodes[0].value;
        assert_eq!(node.get("type").and_then(Value::as_str), Some("vless"));
        assert_eq!(node.get("servername").and_then(Value::as_str), Some("example.test"));
        assert_eq!(node.get("client-fingerprint").and_then(Value::as_str), Some("chrome"));
        assert_eq!(
            node.get("ws-opts")
                .and_then(|value| value.get("path"))
                .and_then(Value::as_str),
            Some("/ws/")
        );
        assert_eq!(
            node.get("ws-opts")
                .and_then(|value| value.get("headers"))
                .and_then(|value| value.get("Host"))
                .and_then(Value::as_str),
            Some("cdn.example.test")
        );
        assert_eq!(node.get("name").and_then(Value::as_str), Some("Moyu WS"));
    }

    #[test]
    fn parses_base64_encoded_uri_list() {
        let uri = "vless://00000000-0000-0000-0000-000000000000@example.test:443?security=tls&type=ws&path=%2F#node";
        let encoded = STANDARD.encode(uri);
        let parsed = parse_subscription("moyu", &encoded).unwrap();
        assert_eq!(parsed.nodes.len(), 1);
        assert_eq!(parsed.nodes[0].name, "node");
    }

    #[test]
    fn parses_vmess_base64_json() {
        let payload = serde_json::json!({
            "v": "2",
            "ps": "vmess-node",
            "add": "example.test",
            "port": "443",
            "id": "00000000-0000-0000-0000-000000000000",
            "aid": "0",
            "net": "ws",
            "path": "/vmess/",
            "host": "cdn.example.test",
            "tls": "tls"
        });
        let payload = STANDARD.encode(serde_json::to_vec(&payload).unwrap());
        let parsed = parse_subscription("source", &format!("vmess://{payload}")).unwrap();
        let node = &parsed.nodes[0].value;
        assert_eq!(node.get("type").and_then(Value::as_str), Some("vmess"));
        assert_eq!(node.get("network").and_then(Value::as_str), Some("ws"));
        assert_eq!(
            node.get("ws-opts")
                .and_then(|value| value.get("path"))
                .and_then(Value::as_str),
            Some("/vmess/")
        );
    }
}
