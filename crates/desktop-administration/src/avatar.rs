use crate::{
    administration::AdministrationView, binding::AdministrationBinding,
    ports::AdministrationAvatarPort,
};
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    avatars::{
        AvatarCacheError, AvatarCacheRequest, AvatarCacheResult, AvatarPublication, MemberSummary,
        PrincipalId, avatar_identity_key,
    },
    core::ClientScope,
};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};
use tokio_util::sync::CancellationToken;

struct AvatarDemand {
    request: AvatarCacheRequest,
    cancellation: CancellationToken,
    _task: Task<Result<AvatarCacheResult, AvatarCacheError>>,
}
impl Drop for AvatarDemand {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}
pub(crate) struct AvatarInput {
    pub(crate) cached_image_path: Option<PathBuf>,
}
pub(crate) struct AdministrationAvatars {
    port: Arc<dyn AdministrationAvatarPort>,
    binding: Arc<AdministrationBinding>,
    demand: HashMap<String, AvatarDemand>,
    publications: std::collections::BTreeMap<String, Arc<AvatarPublication>>,
}
impl AdministrationAvatars {
    pub(crate) fn new(
        port: Arc<dyn AdministrationAvatarPort>,
        binding: Arc<AdministrationBinding>,
    ) -> Self {
        Self {
            port,
            binding,
            demand: HashMap::new(),
            publications: Default::default(),
        }
    }
    pub(crate) fn scopes(&self) -> Vec<ClientScope> {
        self.demand
            .keys()
            .map(|key| ClientScope::Avatar {
                principal_id: key.clone(),
            })
            .collect()
    }
    pub(crate) fn reconcile(
        &mut self,
        members: Vec<MemberSummary>,
        visible: bool,
        cx: &mut Context<AdministrationView>,
    ) {
        let requests: Vec<_> = members
            .into_iter()
            .filter(|_| visible)
            .filter_map(|member| {
                member
                    .avatar_revision
                    .map(|avatar_revision| AvatarCacheRequest {
                        principal_id: member.principal_id,
                        avatar_revision,
                    })
            })
            .collect();
        let wanted: HashSet<_> = requests
            .iter()
            .map(|r| avatar_identity_key(r.principal_id.as_str(), &r.avatar_revision))
            .collect();
        self.demand.retain(|key, _| wanted.contains(key));
        for request in requests {
            let key = avatar_identity_key(request.principal_id.as_str(), &request.avatar_revision);
            if self.demand.contains_key(&key) {
                continue;
            }
            let cancellation = CancellationToken::new();
            let task = self.port.resolve(request.clone(), cancellation.clone(), cx);
            self.demand.insert(
                key,
                AvatarDemand {
                    request,
                    cancellation,
                    _task: task,
                },
            );
        }
        self.publications
            .retain(|key, _| self.demand.contains_key(key));
        for key in self.demand.keys() {
            if let Some(publication) = self
                .binding
                .publication(&ClientScope::Avatar {
                    principal_id: key.clone(),
                })
                .and_then(|p| p.snapshot().payload::<AvatarPublication>())
            {
                self.publications.insert(key.clone(), publication);
            } else {
                self.publications.remove(key);
            }
        }
    }
    pub(crate) fn visible_paths(&self) -> Vec<(String, Option<PathBuf>)> {
        self.publications
            .iter()
            .map(|(key, p)| {
                (
                    key.clone(),
                    p.local_path().map(|path| path.as_path().to_owned()),
                )
            })
            .collect()
    }
    pub(crate) fn presentation(&self, principal: &PrincipalId) -> Option<AvatarInput> {
        let (key, demand) = self
            .demand
            .iter()
            .find(|(_, demand)| &demand.request.principal_id == principal)?;
        let avatar = self.publications.get(key)?;
        (avatar.avatar_revision() == demand.request.avatar_revision).then(|| AvatarInput {
            cached_image_path: avatar.local_path().map(|path| path.as_path().to_owned()),
        })
    }
}
