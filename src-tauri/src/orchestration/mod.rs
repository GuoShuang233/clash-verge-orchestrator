//! Subscription orchestration primitives.
//!
//! The module keeps source subscriptions and user bindings separate from the generated
//! mihomo configuration. This makes upstream updates and configuration mode switches
//! deterministic without rewriting the downloaded profile.

use anyhow::{Context as _, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_yaml_ng::{Mapping, Value};
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, BTreeSet};

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

#[derive(Debug, Clone)]
pub struct ProxyNode {
    pub source_id: String,
    pub node_id: String,
    pub name: String,
    pub value: Value,
}

#[derive(Debug, Clone)]
pub struct ParsedSubscription {
    pub source_id: String,
    pub config: Value,
    pub nodes: Vec<ProxyNode>,
}

pub fn parse_subscription(source_id: &str, content: &str) -> Result<ParsedSubscription> {
    let source_id = source_id.trim();
    if source_id.is_empty() {
        bail!("subscription source id is empty");
    }

    let value = parse_yaml_or_base64(content)?;
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
    let mut digest = Sha256::new();
    digest.update(source_id.as_bytes());
    digest.update([0]);
    digest.update(serde_yaml_ng::to_string(node)?);
    Ok(digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

pub fn compile_config(
    main_config: &str,
    sources: &[SourceInput],
    state: &OrchestrationState,
) -> Result<String> {
    let mut config: Value = serde_yaml_ng::from_str(main_config).context("main profile is not valid YAML")?;
    let parsed = sources
        .iter()
        .map(|source| parse_subscription(&source.source_id, &source.content))
        .collect::<Result<Vec<_>>>()?;
    compile_value(&mut config, &parsed, state)?;
    crate::utils::yaml_emitter::to_mihomo_config_string(&config).context("failed to serialize generated config")
}

pub fn compile_value(
    config: &mut Value,
    sources: &[ParsedSubscription],
    state: &OrchestrationState,
) -> Result<()> {
    let config_mapping = config.as_mapping_mut().context("main profile is not a YAML mapping")?;
    let node_index = build_node_index(sources);
    let mut injected = BTreeSet::new();

    for binding in state.groups.values() {
        for node_ref in &binding.nodes {
            if let Some(node) = node_index.get(&(node_ref.source_id.clone(), node_ref.node_id.clone())) {
                injected.insert(node_ref.name.clone());
                let mut value = node.value.clone();
                if let Some(mapping) = value.as_mapping_mut() {
                    mapping.insert(Value::String("name".into()), Value::String(node_ref.name.clone()));
                }
                append_proxy(config_mapping, value)?;
            }
        }
    }

    for (group_name, binding) in &state.groups {
        let mode = binding.mode.unwrap_or(state.default_mode);
        if mode == GroupMode::Upstream || state.active_mode == GroupMode::Upstream {
            continue;
        }

        let target_name = binding.upstream_name.as_deref().unwrap_or(group_name);
        let group = find_or_create_group(config_mapping, target_name)?;
        match mode {
            GroupMode::Upstream => {}
            GroupMode::Hybrid => {
                for node_ref in &binding.nodes {
                    if injected.contains(&node_ref.name) {
                        append_group_proxy(group, &node_ref.name);
                    }
                }
            }
            GroupMode::Custom => {
                let custom = binding.custom.as_ref().context("custom group mode requires a custom group")?;
                *group = custom.clone();
                let group = group.as_mapping_mut().context("custom proxy group is not a mapping")?;
                for node_ref in &binding.nodes {
                    if injected.contains(&node_ref.name) {
                        append_group_proxy(group, &node_ref.name);
                    }
                }
            }
        }
    }
    Ok(())
}

fn parse_yaml_or_base64(content: &str) -> Result<Value> {
    if let Ok(value) = serde_yaml_ng::from_str(content) {
        return Ok(value);
    }

    let compact: String = content.chars().filter(|ch| !ch.is_whitespace()).collect();
    let decoded = STANDARD
        .decode(compact)
        .context("subscription is neither valid YAML nor Base64 YAML")?;
    let decoded = String::from_utf8(decoded).context("Base64 subscription is not UTF-8")?;
    serde_yaml_ng::from_str(&decoded).context("decoded subscription is not valid YAML")
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
    if let Some(index) = groups.iter().position(|group| group.get("name").and_then(Value::as_str) == Some(name)) {
        return Ok(&mut groups[index]);
    }
    let mut group = Mapping::new();
    group.insert(Value::String("name".into()), Value::String(name.into()));
    group.insert(Value::String("type".into()), Value::String("select".into()));
    group.insert(Value::String("proxies".into()), Value::Sequence(Vec::new()));
    groups.push(Value::Mapping(group));
    Ok(groups.last_mut().unwrap())
}

fn append_group_proxy(group: &mut Mapping, name: &str) {
    let proxies = group
        .entry(Value::String("proxies".into()))
        .or_insert_with(|| Value::Sequence(Vec::new()));
    let proxies = proxies.as_sequence_mut().expect("proxy group proxies must be a sequence");
    if !proxies.iter().any(|value| value.as_str() == Some(name)) {
        proxies.push(Value::String(name.into()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAIN: &str = "proxies:\n  - name: ytoo-us\n    type: ss\n    server: ytoo.example\n    port: 443\nproxy-groups:\n  - name: US\n    type: select\n    proxies:\n      - ytoo-us\n";

    const EXTERNAL: &str = "proxies:\n  - name: moyu-node\n    type: vless\n    server: xray.example\n    port: 443\n    uuid: test\n    tls: true\n    network: ws\n    ws-opts:\n      path: /test/\n";

    fn state(mode: GroupMode) -> OrchestrationState {
        let node = parse_subscription("moyu", EXTERNAL).unwrap().nodes.remove(0);
        OrchestrationState {
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
                    custom: (mode == GroupMode::Custom).then(|| serde_yaml_ng::from_str("{name: US, type: select, proxies: [ytoo-us]}").unwrap()),
                },
            )]),
        }
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
        let output = compile_config(MAIN, &[SourceInput { source_id: "moyu".into(), content: EXTERNAL.into() }], &state(GroupMode::Hybrid)).unwrap();
        assert!(output.contains("[Moyu] moyu-node"));
        assert!(output.contains("name: US"));
    }

    #[test]
    fn custom_mode_replaces_group_shape_but_keeps_bound_nodes() {
        let output = compile_config(MAIN, &[SourceInput { source_id: "moyu".into(), content: EXTERNAL.into() }], &state(GroupMode::Custom)).unwrap();
        assert!(output.contains("type: select"));
        assert!(output.contains("[Moyu] moyu-node"));
    }
}
