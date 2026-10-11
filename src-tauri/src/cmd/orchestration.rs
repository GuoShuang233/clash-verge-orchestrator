use crate::{
    cmd::{CmdResult, coded_error},
    orchestration::{self, NodeSummary, OrchestrationState, SourceInput, SubscriptionSource},
    utils::network::{NetworkManager, ProxyType},
};

#[tauri::command]
pub async fn preview_orchestration_source(source_id: String, content: String) -> CmdResult<Vec<NodeSummary>> {
    let parsed = orchestration::parse_subscription(&source_id, &content)
        .map_err(|error| coded_error("ORCHESTRATION_SOURCE_PARSE_FAILED", format_args!("{error:#}")))?;
    Ok(orchestration::node_summaries(&parsed))
}

#[tauri::command]
pub async fn fetch_orchestration_source(
    source: SubscriptionSource,
    use_proxy: bool,
    timeout_seconds: Option<u64>,
) -> CmdResult<Vec<NodeSummary>> {
    let response = NetworkManager::new()
        .get(
            &source.url,
            if use_proxy { ProxyType::Localhost } else { ProxyType::None },
            timeout_seconds.or(Some(30)),
            source.user_agent.clone(),
            false,
        )
        .await
        .map_err(|error| coded_error("ORCHESTRATION_SOURCE_FETCH_FAILED", format_args!("{error:#}")))?;
    if !response.status().is_success() {
        return Err(coded_error(
            "ORCHESTRATION_SOURCE_FETCH_FAILED",
            format_args!("subscription returned HTTP status {}", response.status()),
        ));
    }
    let parsed = orchestration::parse_subscription(&source.source_id, response.text())
        .map_err(|error| coded_error("ORCHESTRATION_SOURCE_PARSE_FAILED", format_args!("{error:#}")))?;
    Ok(orchestration::node_summaries(&parsed))
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
