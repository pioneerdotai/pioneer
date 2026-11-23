//! Protocol views of persisted threads, composer choices, turns and inputs.

use anyhow::{Context, Result};
use pioneer_crud_access::membership::{
    PersistedThreadAccessClass, persisted_thread_access_class_from_db,
};
use pioneer_crud_access::turn_execution::{
    turn_kind_from_db, turn_origin_from_db, turn_status_from_db,
};
use pioneer_protocol::{
    PromptManifest, PromptManifestProfile, Thread, ThreadVisibility, Turn,
    TurnPermissionProfileSnapshot, TurnPermissionProfileSource, TurnWorkOwner, UserInput,
};

use crate::collaboration::collaboration_from_model;
use crate::thread_metadata::preview_author_from_json;
use crate::turn_writes::{
    TurnPromptManifestColumns, turn_permission_mode_from_db, turn_permission_profile_source_from_db,
};
use crate::values::{
    thread_mode_from_db, thread_origin_kind_from_db, thread_sidebar_visibility_from_db,
    thread_status_from_db,
};

pub fn prompt_manifest_profile_to_db(profile: PromptManifestProfile) -> &'static str {
    match profile {
        PromptManifestProfile::AssistantFull => "assistant_full",
        PromptManifestProfile::AssistantMinimal => "assistant_minimal",
        PromptManifestProfile::AssistantNone => "assistant_none",
        PromptManifestProfile::CliRuntime => "cli_runtime",
    }
}

fn thread_native_context_from_turn(
    model: &pioneer_entity::turn::Model,
    thread: &Thread,
) -> Result<Option<pioneer_protocol::NativeContextBudget>> {
    // CLI/external restoration keeps its pre-existing path. This projection
    // reads only the new Native field, never validates unrelated legacy JSON.
    if model.prompt_profile.as_deref() == Some("cli_runtime")
        || thread.model_provider.starts_with("cli_runtime:")
    {
        return Ok(None);
    }
    let Some(value) = model
        .execution_security_snapshot_json
        .as_deref()
        .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
        .and_then(|mut value| value.get_mut("native_context").map(serde_json::Value::take))
        .filter(|value| !value.is_null())
    else {
        return Ok(None);
    };
    let budget: pioneer_protocol::NativeContextBudget =
        serde_json::from_value(value).context("invalid saved Native model choice")?;
    Ok(Some(budget)
        .filter(|budget| budget.instance == thread.model_provider && budget.model == thread.model))
}

pub fn thread_from_db_model(model: pioneer_entity::thread::Model) -> Result<Option<Thread>> {
    let Some(mode) = thread_mode_from_db(model.mode.as_str()) else {
        return Ok(None);
    };
    let Some(status) = thread_status_from_db(model.status.as_str()) else {
        return Ok(None);
    };
    let Some(origin_kind) = thread_origin_kind_from_db(model.origin_kind.as_str()) else {
        return Ok(None);
    };
    let Some(sidebar_visibility) =
        thread_sidebar_visibility_from_db(model.sidebar_visibility.as_str())
    else {
        return Ok(None);
    };
    let visibility = match persisted_thread_access_class_from_db(model.access_class.as_str())? {
        PersistedThreadAccessClass::Private => Some(ThreadVisibility::Private),
        PersistedThreadAccessClass::Workspace => Some(ThreadVisibility::Workspace),
        PersistedThreadAccessClass::Internal => None,
    };
    let preview_author = preview_author_from_json(model.preview_author_json.as_deref())?;

    Ok(Some(Thread {
        context_window: None,
        service_mode: None,
        context_authority: None,
        workspace_id: model.workspace_id,
        id: model.id,
        name: model.name,
        preview: model.preview,
        preview_author,
        mode,
        model: model.model,
        model_provider: model.model_provider,
        reasoning_effort: None,
        created_at: model.created_at.timestamp(),
        updated_at: model.updated_at.timestamp(),
        status,
        origin_kind,
        sidebar_visibility,
        agent_nickname: model.agent_nickname,
        agent_role: model.agent_role,
        visibility,
        turns: Vec::new(),
    }))
}

pub fn thread_snapshot_turn_from_db_model(
    model: pioneer_entity::turn::Model,
) -> Result<Option<Turn>> {
    decode_turn(model, false)
}

pub fn turn_from_db_model(model: pioneer_entity::turn::Model) -> Result<Option<Turn>> {
    decode_turn(model, true)
}

fn decode_turn(
    model: pioneer_entity::turn::Model,
    read_prompt_manifest: bool,
) -> Result<Option<Turn>> {
    let Some(status) = turn_status_from_db(model.status.as_str()) else {
        return Ok(None);
    };
    let prompt_manifest = if read_prompt_manifest {
        parse_turn_prompt_manifest(&model)?
    } else {
        None
    };
    let permission_profile = parse_turn_permission_profile(&model)?;
    let collaboration = collaboration_from_model(&model)?;

    Ok(Some(Turn {
        work_owner: TurnWorkOwner::from_db_str(&model.work_owner).map_err(anyhow::Error::msg)?,
        id: model.id,
        status,
        turn_kind: turn_kind_from_db(model.turn_kind.as_str()).unwrap_or_default(),
        origin: turn_origin_from_db(model.origin.as_str()).unwrap_or_default(),
        mode: collaboration.mode.effective_mode,
        author: collaboration.author,
        reply_to_turn_id: collaboration.reply_to_turn_id,
        mentions: collaboration.mentions,
        message_revision: collaboration.message_revision,
        message_deleted: collaboration.message_deleted,
        error: model.error,
        prompt_manifest,
        permission_profile,
    }))
}

pub fn decode_turn_input_model(model: pioneer_entity::turn_input::Model) -> Result<UserInput> {
    match serde_json::from_str::<UserInput>(&model.payload) {
        Ok(input) => Ok(input),
        Err(_) if model.input_type == "text" && model.text.is_some() => Ok(UserInput::Text {
            text: model.text.unwrap_or_default(),
            text_elements: Vec::new(),
        }),
        Err(error) => {
            Err(error).with_context(|| format!("failed to decode turn input `{}`", model.id))
        }
    }
}

pub fn parse_turn_prompt_manifest(
    model: &pioneer_entity::turn::Model,
) -> Result<Option<PromptManifest>> {
    let manifest_json = model.prompt_manifest_json.trim();
    if manifest_json.is_empty() || matches!(manifest_json, "{}" | "null") {
        return Ok(None);
    }

    let manifest = serde_json::from_str(manifest_json).with_context(|| {
        format!(
            "failed to decode prompt manifest for turn `{}` in thread `{}`",
            model.id, model.thread_id
        )
    })?;

    Ok(Some(manifest))
}

pub fn parse_turn_permission_profile(
    model: &pioneer_entity::turn::Model,
) -> Result<TurnPermissionProfileSnapshot> {
    if let Some(snapshot_json) = model
        .permission_profile_snapshot_json
        .as_deref()
        .map(str::trim)
        .filter(|value| !matches!(*value, "" | "{}" | "null"))
    {
        return serde_json::from_str(snapshot_json).with_context(|| {
            format!(
                "failed to decode permission profile snapshot for turn `{}` in thread `{}`",
                model.id, model.thread_id
            )
        });
    }

    let Some(mode) = model
        .permission_profile_mode
        .as_deref()
        .and_then(turn_permission_mode_from_db)
    else {
        return Ok(pioneer_protocol::resolve_turn_permission_profile(None));
    };
    let source = model
        .permission_profile_source
        .as_deref()
        .and_then(turn_permission_profile_source_from_db)
        .unwrap_or(TurnPermissionProfileSource::Defaulted);

    Ok(TurnPermissionProfileSnapshot::from_mode(mode, source))
}

pub fn build_turn_prompt_manifest_columns(
    manifest: &PromptManifest,
) -> Result<TurnPromptManifestColumns> {
    Ok(TurnPromptManifestColumns {
        prompt_manifest_json: serde_json::to_string(manifest)
            .context("failed to serialize prompt manifest to json")?,
        prompt_compiler_version: manifest.compiler_version.clone(),
        prompt_profile: prompt_manifest_profile_to_db(manifest.profile).to_owned(),
        prompt_fingerprint_stable: manifest.fingerprint_stable.clone(),
        prompt_fingerprint_dynamic: manifest.fingerprint_dynamic.clone(),
        prompt_fingerprint_full: manifest.fingerprint_full.clone(),
    })
}

pub fn attach_thread_composer_snapshot(
    thread: &mut Thread,
    latest: pioneer_entity::turn::Model,
    conversation: Option<&pioneer_entity::turn::Model>,
) -> Result<()> {
    let reasoning_effort = conversation.and_then(|turn| turn.reasoning_effort.clone());
    let native = conversation
        .map(|turn| thread_native_context_from_turn(turn, thread))
        .transpose()?
        .flatten();
    thread.service_mode = native.as_ref().and_then(|budget| {
        let plan = budget
            .service_mode
            .as_ref()
            .filter(|plan| plan.id != "standard")?;
        Some(pioneer_protocol::NativeServiceModeSelection {
            id: plan.id.clone(),
            authority: pioneer_protocol::native_context_choice_authority(
                budget.authority.as_deref()?,
                &budget.model,
            ),
        })
    });
    thread.context_window = native.as_ref().and_then(|budget| budget.manual_window);
    thread.context_authority = native
        .filter(|budget| budget.manual_window.is_some())
        .and_then(|budget| {
            budget.authority.map(|authority| {
                pioneer_protocol::native_context_choice_authority(&authority, &budget.model)
            })
        });
    if let Some(turn) = thread_snapshot_turn_from_db_model(latest)? {
        thread.reasoning_effort = reasoning_effort;
        thread.turns.push(turn);
    }
    Ok(())
}
