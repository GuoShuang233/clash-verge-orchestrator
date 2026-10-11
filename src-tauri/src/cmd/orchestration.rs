use crate::{
    cmd::{CmdResult, coded_error},
    orchestration::{
        self, NodeSummary, OrchestrationState, SourceContentResult, SourceFetchResult, SourceInput, SubscriptionSource,
    },
    utils::network::{NetworkManager, ProxyType},
};
use smartstring::alias::String as SmartString;

#[tauri::command]
pub async fn preview_orchestration_source(source_id: String, content: String) -> CmdResult<Vec<NodeSummary>> {
    let parsed = orchestration::parse_subscription(&source_id, &content)
        .map_err(|error| coded_error("ORCHESTRATION_SOURCE_PARSE_FAILED", format_args!("{error:#}")))?;
    Ok(orchestration::node_summaries(&parsed))
}

#[tauri::command]
pub async fn fetch_orchestration_source(
    profile_uid: String,
    source: SubscriptionSource,
    use_proxy: bool,
    timeout_seconds: Option<u64>,
) -> CmdResult<SourceFetchResult> {
    let result = if use_proxy {
        orchestration::fetch_source_content(&profile_uid, &source).await
    } else {
        let response = NetworkManager::new()
            .get(
                &source.url,
                ProxyType::None,
                timeout_seconds.or(Some(30)),
                source.user_agent.clone().map(SmartString::from),
                false,
            )
            .await
            .map_err(|error| anyhow::anyhow!("{error:#}"))?;
        if !response.status().is_success() {
            return Err(coded_error(
                "ORCHESTRATION_SOURCE_FETCH_FAILED",
                format_args!("subscription returned HTTP status {}", response.status()),
            ));
        }
        Ok(SourceContentResult {
            content: response.text().to_owned(),
            used_cache: false,
            fetched_at: chrono::Utc::now().timestamp(),
        })
    }
    .map_err(|error| coded_error("ORCHESTRATION_SOURCE_FETCH_FAILED", format_args!("{error:#}")))?;
    let parsed = orchestration::parse_subscription(&source.source_id, &result.content)
        .map_err(|error| coded_error("ORCHESTRATION_SOURCE_PARSE_FAILED", format_args!("{error:#}")))?;
    Ok(SourceFetchResult {
        nodes: orchestration::node_summaries(&parsed),
        used_cache: result.used_cache,
        fetched_at: result.fetched_at,
    })
}

#[tauri::command]
pub async fn get_orchestration_state(profile_uid: String) -> CmdResult<OrchestrationState> {
    orchestration::load_state(&profile_uid)
        .await
        .map_err(|error| coded_error("ORCHESTRATION_STATE_READ_FAILED", format_args!("{error:#}")))
}

#[tauri::command]
pub async fn save_orchestration_state(profile_uid: String, state: OrchestrationState) -> CmdResult {
    orchestration::save_state(&profile_uid, &state)
        .await
        .map_err(|error| coded_error("ORCHESTRATION_STATE_SAVE_FAILED", format_args!("{error:#}")))
}

#[tauri::command]
pub fn preview_orchestration(
    main_config: String,
    sources: Vec<SourceInput>,
    state: OrchestrationState,
) -> CmdResult<String> {
    orchestration::compile_config(&main_config, &sources, &state)
        .map_err(|error| coded_error("ORCHESTRATION_COMPILE_FAILED", format_args!("{error:#}")))
}

