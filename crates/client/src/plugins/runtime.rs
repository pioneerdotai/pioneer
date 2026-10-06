//! Shell-neutral Plugins management. Mobile uses scoped publications; desktop
//! migration: replace its local catalog/action state with these typed intents,
//! retaining direct native Skills/MCP operations and its existing file shell.
use crate::core::{ClientCore, ClientMutationAuthority, ClientScope, ClientTransition};
use pioneer_protocol::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, mpsc},
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PluginIntent {
    Observe {
        plugin_id: Option<String>,
    },
    Close,
    SetEnabled {
        plugin_id: String,
        expected_revision: i64,
        enabled: bool,
    },
    Mutate {
        plugin_id: String,
        expected_revision: i64,
        intent: PluginManagementIntent,
    },
    Install {
        upload_operation: u64,
    },
    ApplyUpdate {
        upload_operation: u64,
        confirm_changes: bool,
    },
    Skill {
        skill_id: SkillId,
        enabled: bool,
        allow_implicit_invocation: bool,
    },
    RemoveSkill {
        skill_id: SkillId,
    },
    ConfigureMcp {
        server_id: String,
        body: String,
    },
    Mcp {
        intent: crate::mcp::operations::McpIntent,
    },
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize)]
pub struct PluginPublication {
    pub connection_id: Option<u64>,
    pub revision: u64,
    pub generation: u64,
    pub plugin_id: Option<String>,
    pub plugins: Vec<PluginItem>,
    pub details: Option<PluginItem>,
    pub skills: Vec<SkillListItem>,
    pub skill_health: Vec<SkillHealthItem>,
    pub mcp: Vec<McpListItem>,
    pub mcp_details: Vec<McpServerDetailsResponse>,
    pub oauth: Vec<PluginOAuthPresentation>,
    pub busy: bool,
    pub managing: bool,
    pub error: Option<String>,
}
/// Secret-free native authorization presentation, shared by management shells.
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct PluginOAuthPresentation {
    pub server_id: String,
    pub flow_id: Option<String>,
    pub state: McpOAuthState,
    pub browser_failed: bool,
}
#[derive(Clone)]
struct Work {
    workspace: String,
    generation: u64,
    epoch: (u64, u64, Option<u64>),
    intent: PluginIntent,
}
#[derive(Default)]
pub(crate) struct PluginController {
    next: u64,
    suspended: std::collections::BTreeSet<String>,
    refresh_requested: std::collections::BTreeSet<String>,
    sender: Option<mpsc::SyncSender<Work>>,
    task: Option<std::thread::JoinHandle<()>>,
    publications: BTreeMap<String, PluginPublication>,
}
impl PluginController {
    pub(crate) fn stop(&mut self) {
        self.sender.take();
        self.publications.clear();
        self.refresh_requested.clear();
        self.suspended.clear();
    }
}
impl Drop for PluginController {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}
impl ClientCore {
    fn publish_plugins(&self, workspace: &str, p: &mut PluginPublication) -> ClientTransition {
        let scope = ClientScope::Plugins {
            workspace_id: workspace.into(),
        };
        p.revision = self
            .snapshot(&scope)
            .map_or(0, |s| s.revisions().scoped().get())
            .saturating_add(1);
        self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            crate::threads::registry::revisions(p.revision),
            Arc::new(p.clone()),
            vec![],
        )
    }
    pub fn plugin_intent(&self, workspace: &str, intent: PluginIntent) -> ClientTransition {
        let managing = self.plugins_management_allowed(workspace);
        let epoch = self.provider_runtime_epoch();
        let mut owner = self.plugins_controller.lock().expect("plugins poisoned");
        let observing = matches!(intent, PluginIntent::Observe { .. });
        if owner.suspended.contains(workspace) && !matches!(intent, PluginIntent::Close) {
            return self.reject_intent();
        }
        if !observing
            && !matches!(intent, PluginIntent::Close)
            && !owner.publications.contains_key(workspace)
        {
            return self.reject_intent();
        }
        owner.next = owner
            .next
            .checked_add(1)
            .expect("plugin client intent identity exhausted");
        let generation = owner.next;
        let p = owner.publications.entry(workspace.into()).or_default();
        if matches!(intent, PluginIntent::Close) {
            p.generation = generation;
            p.busy = false;
            let out = self.publish_plugins(workspace, p);
            owner.publications.remove(workspace);
            return out;
        }
        if self.is_stopped() || (!observing && (p.busy || p.error.is_some() || !managing)) {
            return self.reject_intent();
        }
        match &intent {
            PluginIntent::SetEnabled {
                plugin_id,
                expected_revision,
                ..
            }
            | PluginIntent::Mutate {
                plugin_id,
                expected_revision,
                ..
            } if !p
                .details
                .as_ref()
                .is_some_and(|d| &d.id == plugin_id && d.revision == *expected_revision) =>
            {
                return self.reject_intent();
            }
            PluginIntent::Skill { .. }
            | PluginIntent::RemoveSkill { .. }
            | PluginIntent::ConfigureMcp { .. }
            | PluginIntent::Mcp { .. }
                if p.details.is_none() =>
            {
                return self.reject_intent();
            }
            _ => {}
        }
        // Observing during a mutation cannot supersede its completion. A screen
        // close fences presentation, but never pretends to cancel server work.
        if p.busy {
            return self.reject_intent();
        }
        if let PluginIntent::Observe { plugin_id } = &intent {
            p.plugin_id = plugin_id.clone();
            p.details = None;
            p.skills.clear();
            p.mcp.clear();
            p.mcp_details.clear();
            p.oauth.clear();
        }
        p.connection_id = epoch.2;
        p.generation = generation;
        p.busy = true;
        p.error = None;
        p.managing = managing;
        let work = Work {
            workspace: workspace.into(),
            generation: p.generation,
            epoch,
            intent,
        };
        let out = self.publish_plugins(workspace, p);
        if !owner
            .sender
            .as_ref()
            .is_some_and(|s| s.try_send(work.clone()).is_ok())
        {
            let p = owner.publications.get_mut(workspace).unwrap();
            p.busy = false;
            p.error = Some("Plugin request queue unavailable".into());
            return self.publish_plugins(workspace, p);
        }
        out
    }
    fn plugin_work_current(&self, work: &Work) -> bool {
        !self.is_stopped()
            && self.provider_runtime_epoch() == work.epoch
            && self
                .plugins_controller
                .lock()
                .expect("plugins poisoned")
                .publications
                .get(&work.workspace)
                .is_some_and(|p| p.generation == work.generation && p.busy)
    }
    pub(crate) fn refresh_plugin_publication(&self, workspace: &str) {
        self.refresh_composer_plugins(workspace);
        let id = {
            let mut owner = self.plugins_controller.lock().expect("plugins poisoned");
            let Some(p) = owner.publications.get(workspace) else {
                return;
            };
            if p.busy {
                owner.refresh_requested.insert(workspace.into());
                return;
            }
            p.plugin_id.clone()
        };
        self.plugin_intent(workspace, PluginIntent::Observe { plugin_id: id });
    }
    pub(crate) fn invalidate_plugin_publications(&self) {
        let mut owner = self.plugins_controller.lock().expect("plugins poisoned");
        for (w, p) in &mut owner.publications {
            let generation = p.generation.saturating_add(1);
            *p = PluginPublication {
                generation,
                error: Some("Gateway or permissions changed; refresh required".into()),
                ..Default::default()
            };
            self.publish_plugins(w, p);
        }
    }
    pub(crate) fn plugin_demand_changed(
        &self,
        scope: &ClientScope,
        demand: crate::core::ClientDemand,
    ) {
        let ClientScope::Plugins { workspace_id } = scope else {
            return;
        };
        let mut owner = self.plugins_controller.lock().expect("plugins poisoned");
        if demand == crate::core::ClientDemand::Suspended {
            owner.suspended.insert(workspace_id.clone());
            if let Some(p) = owner.publications.get_mut(workspace_id) {
                let generation = p.generation.saturating_add(1);
                let id = p.plugin_id.clone();
                let connection_id = p.connection_id;
                *p = PluginPublication {
                    generation,
                    plugin_id: id,
                    connection_id,
                    ..Default::default()
                };
                self.publish_plugins(workspace_id, p);
            }
            return;
        }
        if !owner.suspended.remove(workspace_id) {
            return;
        }
        let id = owner
            .publications
            .get(workspace_id)
            .and_then(|p| p.plugin_id.clone());
        drop(owner);
        self.plugin_intent(workspace_id, PluginIntent::Observe { plugin_id: id });
    }
    pub(crate) fn start_plugins_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel::<Work>(16);
        let weak = Arc::downgrade(self);
        let task=std::thread::Builder::new().name("client-plugins".into()).spawn(move||while let Ok(work)=receiver.recv(){
            let Some(core)=weak.upgrade() else{return;}; if !core.plugin_work_current(&work){continue;}
            let current=||weak.upgrade().is_some_and(|c|c.plugin_work_current(&work));
            let result=(||->anyhow::Result<PluginPublication>{
                let connection=work.epoch.2.ok_or_else(||anyhow::anyhow!("gateway_not_connected"))?;
                let bound=core.transport_runtime().ws_command_sender().requests_for_connection(connection);
                let id=core.plugins_controller.lock().expect("plugins poisoned").publications[&work.workspace].plugin_id.clone();
                match &work.intent {
                    PluginIntent::SetEnabled{plugin_id,expected_revision,enabled}=>{crate::transport::ws::command_sender::plugins_set_enabled(&bound,PluginsSetEnabledParams{workspace_id:work.workspace.clone(),plugin_id:plugin_id.clone(),expected_revision:*expected_revision,enabled:*enabled})?;},
                    PluginIntent::Mutate{plugin_id,expected_revision,intent}=>{crate::transport::ws::command_sender::plugins_mutate(&bound,PluginsMutateParams{workspace_id:work.workspace.clone(),plugin_id:plugin_id.clone(),expected_revision:*expected_revision,intent:intent.clone()})?;},
                    PluginIntent::Install{upload_operation}=>{
                        let preview=core.confirm_plugin_upload(&work.workspace,*upload_operation,work.epoch)?;
                        let preview=preview.plugin_preview.ok_or_else(||anyhow::anyhow!("plugin_preview_required"))?;let upload_id=preview.upload_id;let preview=preview.package;
                        crate::transport::ws::command_sender::plugins_install(&bound,PluginsInstallParams{workspace_id:work.workspace.clone(),upload_id,expected_fingerprint:preview.fingerprint})?;
                    },
                    PluginIntent::ApplyUpdate{upload_operation,confirm_changes}=>{
                        let preview=core.confirm_plugin_upload(&work.workspace,*upload_operation,work.epoch)?;
                        let preview=preview.plugin_update_preview.ok_or_else(||anyhow::anyhow!("plugin_preview_required"))?;let upload_id=preview.upload_id;let preview=preview.preview;
                        let crate::skills::operations::SkillUploadTarget::PluginUpdatePreview{plugin_id,expected_revision}=preview_target(&core,&work.workspace,*upload_operation)? else{anyhow::bail!("plugin_update_required");};
                        crate::transport::ws::command_sender::plugins_mutate(&bound,PluginsMutateParams{workspace_id:work.workspace.clone(),plugin_id,expected_revision,intent:PluginManagementIntent::Update{upload_id,expected_fingerprint:preview.package.fingerprint,confirm_changes:*confirm_changes}})?;
                    },
                    _=>{},
                }
                anyhow::ensure!(current(),"plugin_catalog_stale");
                let list=crate::transport::ws::command_sender::plugins_list(&bound,PluginsListParams{workspace_id:work.workspace.clone()})?;
                let details=match id.as_deref().filter(|id|list.plugins.iter().any(|p|p.id==*id)){Some(id)=>Some(crate::transport::ws::command_sender::plugins_details(&bound,PluginsDetailsParams{workspace_id:work.workspace.clone(),plugin_id:id.into()})?),None=>None};
                let mut p=PluginPublication{connection_id:work.epoch.2,plugins:list.plugins,details,plugin_id:id,generation:work.generation,managing:core.plugins_management_allowed(&work.workspace),..Default::default()};
                if let Some(parent)=&p.details {
                    let _management=core.acquire_skills_demand(&work.workspace,None);
                    let skills=core.read_skills_catalog(&work.workspace).wait_while(current)?;
                    anyhow::ensure!(skills.request==crate::skills::store::SkillsLoadState::Ready,"skills_catalog_unavailable");
                    p.skills=skills.catalog.iter().filter(|s|parent.components.iter().any(|c|c.skill_id.as_ref()==Some(&s.skill_id))).map(|s|(**s).clone()).collect();
                    if p.managing && !p.skills.is_empty() {
                        // Existing batched native Health read includes trust gates; a
                        // cached composer catalog need not contain management health.
                        p.skill_health=crate::transport::ws::command_sender::skills_health(&bound,SkillsHealthParams{workspace_id:work.workspace.clone(),skills:p.skills.iter().map(|s|SkillHealthTarget{skill_id:s.skill_id.clone()}).collect(),audit_limit:16})?.skills;
                    }
                    let mcp=core.read_mcp_catalog(&work.workspace).wait_while(current)?;
                    anyhow::ensure!(mcp.request()==crate::mcp::store::McpLoadState::Ready,"mcp_catalog_unavailable");
                    p.mcp=mcp.servers().iter().filter(|s|parent.components.iter().any(|c|c.mcp_installation_id.as_deref()==Some(s.id.as_str()))).map(|s|(**s).clone()).collect();
                    for s in &p.mcp {
                        let d=core.read_mcp_details(&work.workspace,&s.id).wait_while(current)?;
                        let management=d.details().and_then(|details|details.management.as_ref()).and_then(|m|m.oauth_state);
                        if let Some(details)=d.details(){p.mcp_details.push((**details).clone());}
                        if let Some((event,unavailable))=core.mcp_oauth_presentation(&work.workspace,&s.id){
                            let callback_unavailable=event.flow_id.is_some() && event.diagnostic.as_deref()==Some("oauth_callback_unavailable");
                            let state=crate::mcp::oauth::effective_oauth_management_state(Some(event.state),management,callback_unavailable).unwrap_or(event.state);
                            let flow_id=(state==event.state).then_some(event.flow_id).flatten();
                            p.oauth.push(PluginOAuthPresentation{server_id:s.id.clone(),flow_id,state,browser_failed:unavailable});
                        }
                    }
                    match &work.intent {
                        PluginIntent::RemoveSkill{skill_id}=>{
                            anyhow::ensure!(p.skills.iter().any(|s|&s.skill_id==skill_id),"plugin_child_unavailable");
                            core.skills_intent(&work.workspace,crate::skills::operations::SkillsIntent::Remove{skill_id:skill_id.clone()})?;
                        },
                        PluginIntent::ConfigureMcp{server_id,body}=>{
                            let server=p.mcp.iter().find(|s|&s.id==server_id).ok_or_else(||anyhow::anyhow!("plugin_child_unavailable"))?;
                            let config_json=crate::mcp::actions::owned_server_config_for_submit(body,&server.name).ok_or_else(||anyhow::anyhow!("mcp_config_invalid"))?;
                            core.mcp_intent(&work.workspace,crate::mcp::operations::McpIntent::Configure{config_json})?;
                        },
                        PluginIntent::Skill{skill_id,enabled,allow_implicit_invocation}=>{
                            anyhow::ensure!(p.skills.iter().any(|s|&s.skill_id==skill_id),"plugin_child_unavailable");
                            core.skills_intent(&work.workspace,crate::skills::operations::SkillsIntent::Policy{skill_id:skill_id.clone(),enabled:*enabled,allow_implicit_invocation:*allow_implicit_invocation})?;
                        },
                        PluginIntent::Mcp{intent}=>{
                            use crate::mcp::operations::McpIntent::*;
                            let server_id=match intent{SignIn{server_id}|Disconnect{server_id}|CancelAuthorization{server_id,..}|RetryAuthorizationBrowser{server_id}|Policy{server_id,..}|Restart{server_id}|Remove{server_id}=>server_id,Configure{..}=>anyhow::bail!("plugin_child_configure_forbidden")};
                            anyhow::ensure!(p.mcp.iter().any(|s|&s.id==server_id),"plugin_child_unavailable");
                            core.mcp_intent(&work.workspace,intent.clone())?;
                        },_=>{}
                    }
                } else { anyhow::ensure!(!matches!(work.intent,PluginIntent::Skill{..}|PluginIntent::Mcp{..}|PluginIntent::RemoveSkill{..}|PluginIntent::ConfigureMcp{..}),"plugin_child_unavailable"); }
                Ok(p)
            })();
            if !core.plugin_work_current(&work){continue;}
            let mut owner=core.plugins_controller.lock().expect("plugins poisoned");
            if let Some(p)=owner.publications.get_mut(&work.workspace).filter(|p|p.generation==work.generation){
                match result {Ok(next)=>*p=next,Err(error)=>{p.busy=false;p.error=Some(format!("{error:#}"));}}
                core.publish_plugins(&work.workspace,p);
            }
            let refresh=owner.refresh_requested.remove(&work.workspace);
            drop(owner);
            if refresh { core.refresh_plugin_publication(&work.workspace); }
        }).expect("plugins worker start");
        let mut owner = self.plugins_controller.lock().expect("plugins poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
}
fn preview_target(
    core: &ClientCore,
    workspace: &str,
    id: u64,
) -> anyhow::Result<crate::skills::operations::SkillUploadTarget> {
    core.skill_upload_snapshot(workspace, id)
        .map(|p| p.target.clone())
        .ok_or_else(|| anyhow::anyhow!("plugin_preview_required"))
}

#[cfg(test)]
mod tests {
    use super::*;
    // NOT_RUN / NOT_COMPILED. No transport/app/fixture is started.
    #[test]
    fn closed_surface_and_uncertain_projection_cannot_admit_mutations() {
        let core = crate::catalog_test_support::client();
        let params = PluginIntent::SetEnabled {
            plugin_id: "P".repeat(21),
            expected_revision: 7,
            enabled: false,
        };
        assert_eq!(
            core.plugin_intent("workspace", params.clone()).outcome(),
            crate::core::ClientTransitionOutcome::Rejected
        );
        core.plugins_controller.lock().unwrap().publications.insert(
            "workspace".into(),
            PluginPublication {
                error: Some("uncertain".into()),
                ..Default::default()
            },
        );
        assert_eq!(
            core.plugin_intent("workspace", params.clone()).outcome(),
            crate::core::ClientTransitionOutcome::Rejected
        );
        core.plugin_intent("workspace", PluginIntent::Close);
        assert_eq!(
            core.plugin_intent("workspace", params).outcome(),
            crate::core::ClientTransitionOutcome::Rejected
        );
    }
    #[test]
    fn close_then_reopen_cannot_revive_an_old_read_with_the_same_counter() {
        let core = crate::catalog_test_support::client();
        let (sender, receiver) = mpsc::sync_channel(4);
        core.plugins_controller.lock().unwrap().sender = Some(sender);
        core.plugin_intent("workspace", PluginIntent::Observe { plugin_id: None });
        let old = receiver.try_recv().unwrap();
        core.plugin_intent("workspace", PluginIntent::Close);
        core.plugin_intent(
            "workspace",
            PluginIntent::Observe {
                plugin_id: Some("P".repeat(21)),
            },
        );
        let new = receiver.try_recv().unwrap();
        assert_ne!(old.generation, new.generation);
        assert!(!core.plugin_work_current(&old));
        assert!(core.plugin_work_current(&new));
    }
    #[test]
    fn foreign_connection_cannot_close_or_change_current_projection() {
        let core = crate::catalog_test_support::client();
        core.plugins_controller.lock().unwrap().publications.insert(
            "workspace".into(),
            PluginPublication {
                generation: 17,
                ..Default::default()
            },
        );
        let wrong = core.current_auth_ticket().1.map(|id| id + 1).or(Some(99));
        let result = core.dispatch(crate::core::ClientIntent::Plugins {
            workspace_id: "workspace".into(),
            connection_id: wrong,
            intent: PluginIntent::Close,
        });
        assert_eq!(
            result.outcome(),
            crate::core::ClientTransitionOutcome::Rejected
        );
        assert_eq!(
            core.plugins_controller.lock().unwrap().publications["workspace"].generation,
            17
        );
    }
}
