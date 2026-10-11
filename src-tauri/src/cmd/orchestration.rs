use crate::{cmd::{CmdResult, coded_error}, orchestration::{self, OrchestrationState, SourceInput}};

#[tauri::command]
pub fn preview_orchestration(
    main_config: String,
    sources: Vec<SourceInput>,
    state: OrchestrationState,
) -> CmdResult<String> {
    orchestration::compile_config(&main_config, &sources, &state)
        .map_err(|error| coded_error("ORCHESTRATION_COMPILE_FAILED", format_args!("{error:#}")))
}
