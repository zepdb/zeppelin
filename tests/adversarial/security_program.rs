//! Security-specific model and oracle primitives for the adversarial runner.
//!
//! The functions in this module are intentionally pure. HTTP execution,
//! authoritative S3 reads, and quiet-period choreography stay in `runner.rs`;
//! this module decides whether their observations preserve the Phase 5
//! security invariants.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use zeppelin::index::filter::evaluate_filter;
use zeppelin::security::{
    Action, AuditOutcome, AuditParams, AuditRecord, GrantActions, GrantScope, KeyState,
    PolicyGrant, PolicySnapshot, WriteConstraints,
};
use zeppelin::types::{AttributeValue, Filter};

use super::model::{Model, ModelRecord};
use super::ops::{
    ActorRole, ActorSel, DelegatedTokenSpec, ForbiddenWriteKind, GrantChange, KeySel, Op,
    SecurityGrantSpec, TenantProbeSurface, TokenSel,
};
use super::oracle::ViolationId;

pub const REVOCATION_BOUND_OPS: u64 = 10;
pub const SECURITY_PROGRAM_START_OP: u64 = 15;
pub const SECURITY_AUDIT_BARRIER_OP: u64 = 38;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SecurityPrincipalPlan {
    pub actor: ActorSel,
    pub role: ActorRole,
    pub principal_id: String,
    pub display_name: String,
    pub grants: Vec<SecurityGrantSpec>,
    pub bootstrap_key: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SecurityProgramConfig {
    pub principals: Vec<SecurityPrincipalPlan>,
    pub security_ops: Vec<String>,
    pub revocation_bound_ops: u64,
    #[serde(default)]
    pub protected_assumptions: Vec<String>,
    #[serde(default)]
    pub expiry_scenario: bool,
}

impl SecurityProgramConfig {
    #[must_use]
    pub fn for_seed(prefix: &str, namespaces: &[String]) -> Self {
        Self::for_seed_with_expiry(prefix, namespaces, false)
    }

    #[must_use]
    pub fn for_security_profile(prefix: &str, namespaces: &[String]) -> Self {
        Self::for_seed_with_expiry(prefix, namespaces, true)
    }

    fn for_seed_with_expiry(prefix: &str, namespaces: &[String], expiry_scenario: bool) -> Self {
        assert!(
            namespaces.len() >= 2,
            "security profile requires two tenant namespaces"
        );
        let tenant_a_ns = namespaces[0].clone();
        let tenant_b_ns = namespaces[1].clone();
        let filter_a = json!({"op": "eq", "field": "group", "value": "g0"});
        let filter_b = json!({"op": "eq", "field": "group", "value": "g1"});
        let query_grant = |namespace: &str, filter: &Value| SecurityGrantSpec {
            namespace: Some(namespace.to_string()),
            actions: vec!["Query".to_string()],
            mandatory_filter: Some(filter.clone()),
            write_constraints: None,
        };
        let data_grant = |namespace: &str, filter: &Value, group: &str| SecurityGrantSpec {
            namespace: Some(namespace.to_string()),
            actions: vec![
                "NamespaceRead".to_string(),
                "VectorFetch".to_string(),
                "VectorUpsert".to_string(),
                "VectorDelete".to_string(),
            ],
            mandatory_filter: Some(filter.clone()),
            write_constraints: Some(json!({
                "stamp": {"group": group},
                "forbid_set": ["classification", "group"]
            })),
        };
        let principal = |actor: u8,
                         role: ActorRole,
                         grants: Vec<SecurityGrantSpec>,
                         bootstrap_key: bool| SecurityPrincipalPlan {
            actor: ActorSel(actor),
            role,
            principal_id: format!("service:{prefix}-security-{actor}"),
            display_name: format!("adversarial-{}", ActorSel(actor).label()),
            grants,
            bootstrap_key,
        };
        Self {
            principals: vec![
                principal(
                    1,
                    ActorRole::ReadOnly,
                    vec![SecurityGrantSpec {
                        namespace: Some(tenant_a_ns.clone()),
                        actions: vec!["NamespaceRead".to_string(), "Query".to_string()],
                        mandatory_filter: None,
                        write_constraints: None,
                    }],
                    true,
                ),
                principal(
                    2,
                    ActorRole::TenantA,
                    vec![
                        query_grant(&tenant_a_ns, &filter_a),
                        data_grant(&tenant_a_ns, &filter_a, "g0"),
                        SecurityGrantSpec {
                            namespace: None,
                            actions: vec!["CredentialDelegate".to_string()],
                            mandatory_filter: None,
                            write_constraints: None,
                        },
                    ],
                    true,
                ),
                principal(
                    3,
                    ActorRole::TenantB,
                    vec![
                        query_grant(&tenant_b_ns, &filter_b),
                        data_grant(&tenant_b_ns, &filter_b, "g1"),
                    ],
                    true,
                ),
                principal(
                    4,
                    ActorRole::RevocationTarget,
                    vec![query_grant(&tenant_a_ns, &filter_a)],
                    false,
                ),
                principal(
                    5,
                    ActorRole::SecurityAdmin,
                    vec![SecurityGrantSpec {
                        namespace: None,
                        actions: vec![
                            "SecurityAdminRead".to_string(),
                            "SecurityAdminWrite".to_string(),
                        ],
                        mandatory_filter: None,
                        write_constraints: None,
                    }],
                    true,
                ),
            ],
            security_ops: vec![
                "create_key",
                "rotate_key",
                "revoke_key",
                "publish_grant_change",
                "tenant_boundary_probe",
                "use_revoked_credential",
                "forbidden_write_probe",
                "export_probe",
                "security_admin_probe",
                "audit_barrier",
                "mint_token",
                "use_token",
                "token_exceed_scope_probe",
                "use_expired_token",
                "revoke_parent_then_use_token",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            revocation_bound_ops: REVOCATION_BOUND_OPS,
            protected_assumptions: [
                "authz-bounded-staleness",
                "policy-cas-head",
                "audit-evidence-durable",
                "tenant-isolation",
                "delegation-narrows-parent",
                "delegation-parent-revocation",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            expiry_scenario,
        }
    }

    #[must_use]
    pub fn principal(&self, actor: ActorSel) -> &SecurityPrincipalPlan {
        self.principals
            .iter()
            .find(|principal| principal.actor == actor)
            .unwrap_or_else(|| panic!("security program omitted actor {}", actor.0))
    }

    #[must_use]
    pub fn tenant_namespace(&self, actor: ActorSel) -> Option<&str> {
        self.principal(actor)
            .grants
            .iter()
            .find_map(|grant| grant.namespace.as_deref())
    }

    #[must_use]
    pub fn rewrite_namespace_prefix(&self, old_prefix: &str, new_prefix: &str) -> Self {
        let rewrite = |value: &str| {
            value.strip_prefix(old_prefix).map_or_else(
                || value.to_string(),
                |suffix| format!("{new_prefix}{suffix}"),
            )
        };
        let mut rewritten = self.clone();
        for principal in &mut rewritten.principals {
            principal.principal_id = rewrite(&principal.principal_id);
            for grant in &mut principal.grants {
                if let Some(namespace) = &mut grant.namespace {
                    *namespace = rewrite(namespace);
                }
            }
        }
        rewritten
    }

    #[must_use]
    pub fn scripted_ops(&self) -> Vec<Op> {
        let revocation = self.principal(ActorSel(4));
        let tenant_a = self.principal(ActorSel(2));
        let denied_target = self
            .tenant_namespace(ActorSel(3))
            .expect("tenant B must have one namespace")
            .to_string();
        let tenant_a_ns = self
            .tenant_namespace(ActorSel(2))
            .expect("tenant A must have one namespace")
            .to_string();
        let query_grant = tenant_a
            .grants
            .iter()
            .find(|grant| grant.actions == ["Query"])
            .cloned()
            .expect("tenant A must have an exact Query grant");
        let expiring_token = TokenSel {
            parent: ActorSel(2),
            slot: 0,
        };
        let live_token = TokenSel {
            parent: ActorSel(2),
            slot: 1,
        };
        let narrowed_token = |purpose: &str, expires_after_secs| DelegatedTokenSpec {
            actions: vec!["Query".to_string()],
            namespaces: vec![tenant_a_ns.clone()],
            mandatory_filter: Some(json!({
                "op": "eq",
                "field": "bucket",
                "value": 0
            })),
            purpose: purpose.to_string(),
            expires_after_secs,
        };
        let mut ops = vec![
            Op::CreateKey {
                actor: ActorSel::ADMIN,
                subject: ActorSel(4),
                principal_kind: revocation.role,
                grants: revocation.grants.clone(),
                expires_after_secs: Some(60),
            },
            Op::RotateKey {
                actor: ActorSel::ADMIN,
                key: KeySel {
                    actor: ActorSel(4),
                    retired: 0,
                },
            },
            Op::UseRevokedCredential {
                key: KeySel {
                    actor: ActorSel(4),
                    retired: 1,
                },
            },
            Op::RevokeKey {
                actor: ActorSel::ADMIN,
                key: KeySel {
                    actor: ActorSel(4),
                    retired: 0,
                },
            },
            Op::UseRevokedCredential {
                key: KeySel {
                    actor: ActorSel(4),
                    retired: 0,
                },
            },
            Op::PublishGrantChange {
                actor: ActorSel::ADMIN,
                principal: ActorSel(2),
                grants: vec![query_grant.clone()],
                change: GrantChange::Remove,
            },
            Op::TenantBoundaryProbe {
                actor: ActorSel(2),
                target_ns: tenant_a_ns.clone(),
                surface: TenantProbeSurface::Query,
            },
        ];
        ops.extend(
            TenantProbeSurface::ALL
                .into_iter()
                .map(|surface| Op::TenantBoundaryProbe {
                    actor: ActorSel(2),
                    target_ns: denied_target.clone(),
                    surface,
                }),
        );
        ops.push(Op::PublishGrantChange {
            actor: ActorSel::ADMIN,
            principal: ActorSel(2),
            grants: vec![query_grant.clone()],
            change: GrantChange::Add,
        });
        if self.expiry_scenario {
            ops.push(Op::MintToken {
                actor: ActorSel(2),
                token: expiring_token,
                narrowed: narrowed_token("adversarial-expiry", 60),
            });
        }
        ops.extend([
            Op::ForbiddenWriteProbe {
                actor: ActorSel(2),
                target_ns: tenant_a_ns.clone(),
                kind: ForbiddenWriteKind::StampForgery,
            },
            Op::ForbiddenWriteProbe {
                actor: ActorSel(2),
                target_ns: tenant_a_ns.clone(),
                kind: ForbiddenWriteKind::ForbidSetAttribute,
            },
            Op::ForbiddenWriteProbe {
                actor: ActorSel(2),
                target_ns: tenant_a_ns.clone(),
                kind: ForbiddenWriteKind::CrossScopeDelete,
            },
        ]);
        if self.expiry_scenario {
            ops.push(Op::UseExpiredToken {
                token: expiring_token,
                target_ns: tenant_a_ns.clone(),
            });
        }
        ops.extend([
            Op::ExportProbe {
                actor: ActorSel(1),
                target_ns: tenant_a_ns.clone(),
            },
            Op::SecurityAdminProbe { actor: ActorSel(2) },
            Op::AuditBarrierOp { actor: ActorSel(5) },
            Op::MintToken {
                actor: ActorSel(2),
                token: live_token,
                narrowed: narrowed_token("adversarial-live", 300),
            },
            Op::UseToken {
                token: live_token,
                target_ns: tenant_a_ns.clone(),
            },
            Op::TokenExceedScopeProbe {
                token: live_token,
                target_ns: denied_target,
            },
            Op::PublishGrantChange {
                actor: ActorSel::ADMIN,
                principal: ActorSel(2),
                grants: vec![query_grant.clone()],
                change: GrantChange::Remove,
            },
            Op::RevokeParentThenUseToken {
                token: live_token,
                target_ns: tenant_a_ns.clone(),
            },
            Op::PublishGrantChange {
                actor: ActorSel::ADMIN,
                principal: ActorSel(2),
                grants: vec![query_grant],
                change: GrantChange::Add,
            },
        ]);
        ops
    }
}

impl TenantProbeSurface {
    pub const ALL: [Self; 8] = [
        Self::Query,
        Self::Batch,
        Self::Fetch,
        Self::Paginate,
        Self::Facet,
        Self::Group,
        Self::AsOf,
        Self::Explain,
    ];
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StalenessWindow {
    pub actor: ActorSel,
    pub opened_at: u64,
    pub closes_after: u64,
    pub transition: StalenessTransition,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StalenessTransition {
    /// The old credential may remain accepted until the monotonic refresh
    /// bound closes; the post-refresh state is unauthenticated.
    CredentialRevocation,
    /// Any exact whole-policy snapshot reachable before or after this
    /// publication may authorize the request until absorption completes.
    /// Keeping atomic snapshots avoids inventing cross-principal mixtures that
    /// never existed at one authoritative policy version.
    Policy {
        policy_states: Vec<BTreeMap<ActorSel, Vec<SecurityGrantSpec>>>,
        changed_grants: Vec<SecurityGrantSpec>,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SecurityPolicyModel {
    pub config: Option<SecurityProgramConfig>,
    pub policy_version: u64,
    pub version_history: BTreeMap<u64, u64>,
    pub live_keys: BTreeMap<ActorSel, BTreeSet<KeySel>>,
    pub known_key_ids: BTreeMap<u8, BTreeMap<u8, String>>,
    pub live_grants: BTreeMap<ActorSel, Vec<SecurityGrantSpec>>,
    #[serde(default)]
    pub delegated_tokens: BTreeMap<String, DelegatedTokenModel>,
    #[serde(default)]
    pub policy_branches: Vec<BTreeMap<ActorSel, Vec<SecurityGrantSpec>>>,
    pub staleness_windows: Vec<StalenessWindow>,
    pub revoked_credentials: BTreeSet<KeySel>,
    pub successful_audit_requests: BTreeSet<String>,
    pub indeterminate_mutations: Vec<IndeterminateSecurityMutation>,
    pub resolved_mutations: Vec<SecurityMutationResolution>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegatedTokenModel {
    pub token_id: String,
    pub parent: ActorSel,
    pub narrowed: DelegatedTokenSpec,
    pub minted_at_op: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndeterminateSecurityMutation {
    pub op_index: u64,
    pub op: Op,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SecurityMutationOutcome {
    Applied,
    NotApplied,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SecurityMutationResolution {
    pub op_index: u64,
    pub effect: String,
    pub request_id: String,
    pub resolved: SecurityMutationOutcome,
    pub audit_outcome: Option<String>,
    pub audit_policy_version: Option<u64>,
    pub published_policy_version: Option<u64>,
    pub authoritative_policy_version: u64,
}

impl SecurityPolicyModel {
    pub fn initialize(&mut self, config: SecurityProgramConfig, policy_version: u64) {
        let mut actors = BTreeSet::new();
        for principal in &config.principals {
            assert!(
                actors.insert(principal.actor),
                "security program duplicated actor {}",
                principal.actor.0
            );
            for grant in &principal.grants {
                let _ = parsed_grant_actions(grant);
            }
        }
        self.live_grants = config
            .principals
            .iter()
            .map(|principal| (principal.actor, principal.grants.clone()))
            .collect();
        self.policy_branches = vec![self.live_grants.clone()];
        self.live_keys = config
            .principals
            .iter()
            .filter(|principal| principal.bootstrap_key)
            .map(|principal| {
                (
                    principal.actor,
                    BTreeSet::from([KeySel {
                        actor: principal.actor,
                        retired: 0,
                    }]),
                )
            })
            .collect();
        assert!(self.config.replace(config).is_none());
        self.policy_version = policy_version;
        self.version_history.insert(0, policy_version);
    }

    #[must_use]
    pub fn enabled(&self) -> bool {
        self.config.is_some()
    }

    pub fn register_known_key(&mut self, key: KeySel, key_id: String) {
        if let Some(existing) = self
            .known_key_ids
            .entry(key.actor.0)
            .or_default()
            .insert(key.retired, key_id.clone())
        {
            assert_eq!(existing, key_id, "key selector changed stable key identity");
        }
        self.live_keys.entry(key.actor).or_default().insert(key);
    }

    fn register_response_key(&mut self, key: KeySel, response: &Value) {
        let key_id = response
            .get("key_id")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("successful {} response omitted key_id", key.actor.label()));
        self.register_known_key(key, key_id.to_string());
    }

    fn shift_actor_keys(&mut self, actor: ActorSel) {
        let known = self
            .known_key_ids
            .remove(&actor.0)
            .unwrap_or_default()
            .into_iter()
            .map(|(retired, key_id)| {
                (
                    retired
                        .checked_add(1)
                        .expect("credential retirement depth overflowed u8"),
                    key_id,
                )
            })
            .collect::<BTreeMap<_, _>>();
        if !known.is_empty() {
            self.known_key_ids.insert(actor.0, known);
        }

        if let Some(keys) = self.live_keys.get_mut(&actor) {
            *keys = keys
                .iter()
                .map(|key| KeySel {
                    actor,
                    retired: key
                        .retired
                        .checked_add(1)
                        .expect("live credential retirement depth overflowed u8"),
                })
                .collect();
        }
        self.revoked_credentials = self
            .revoked_credentials
            .iter()
            .map(|key| {
                if key.actor == actor {
                    KeySel {
                        actor,
                        retired: key
                            .retired
                            .checked_add(1)
                            .expect("revoked credential retirement depth overflowed u8"),
                    }
                } else {
                    *key
                }
            })
            .collect();
    }

    /// Replace optimistic security state with the immutable snapshot selected
    /// by the authoritative S3 head after a quiet-period refresh.
    pub fn resolve_authoritative(
        &mut self,
        snapshot: &PolicySnapshot,
        now: DateTime<Utc>,
        op_index: u64,
        durable_audit_records: &BTreeMap<String, AuditRecord>,
    ) -> Result<(), String> {
        let version = snapshot.version().get();
        let mut mutation_resolutions = Vec::with_capacity(self.indeterminate_mutations.len());
        for mutation in &self.indeterminate_mutations {
            let request_id = format!("adv-{}-{}", mutation.op_index, mutation.op.kind());
            let audit = durable_audit_records.get(&request_id);
            let mut published_policy_version = None;
            if let Some(audit) = audit {
                if audit.action != Action::SecurityAdminWrite {
                    return Err(format!(
                        "security ambiguity {request_id} resolved from wrong audit action {}",
                        audit.action.as_str()
                    ));
                }
                if audit.resource != zeppelin::security::ResourceRef::SecurityPolicy {
                    return Err(format!(
                        "security ambiguity {request_id} resolved from a non-policy audit resource"
                    ));
                }
                if audit.policy_version.get() > version {
                    return Err(format!(
                        "security ambiguity {request_id} audit policy version {} exceeds authoritative version {version}",
                        audit.policy_version.get()
                    ));
                }
                if audit.outcome == AuditOutcome::Success {
                    let AuditParams::SecurityPolicyChange {
                        old_version,
                        new_version,
                    } = audit.params
                    else {
                        return Err(format!(
                            "security ambiguity {request_id} success omitted policy version lineage"
                        ));
                    };
                    if old_version != audit.policy_version
                        || new_version.get() <= old_version.get()
                        || new_version.get() > version
                    {
                        return Err(format!(
                            "security ambiguity {request_id} carried invalid policy lineage audit={} old={} new={} authoritative={version}",
                            audit.policy_version.get(),
                            old_version.get(),
                            new_version.get()
                        ));
                    }
                    published_policy_version = Some(new_version.get());
                }
            }
            mutation_resolutions.push(SecurityMutationResolution {
                op_index: mutation.op_index,
                effect: mutation.op.kind().to_string(),
                request_id,
                resolved: if audit.is_some_and(|record| record.outcome == AuditOutcome::Success) {
                    SecurityMutationOutcome::Applied
                } else {
                    SecurityMutationOutcome::NotApplied
                },
                audit_outcome: audit.map(|record| record.outcome.outcome_class().to_string()),
                audit_policy_version: audit.map(|record| record.policy_version.get()),
                published_policy_version,
                authoritative_policy_version: version,
            });
        }

        let config = self
            .config
            .as_ref()
            .expect("authoritative security resolution requires a configured program");
        let mut candidates = config
            .principals
            .iter()
            .map(|principal| (principal.actor, principal.grants.clone()))
            .collect::<BTreeMap<_, _>>();
        for (actor, grants) in &self.live_grants {
            candidates.entry(*actor).or_default().extend(grants.clone());
        }
        for mutation in &self.indeterminate_mutations {
            match &mutation.op {
                Op::CreateKey {
                    subject, grants, ..
                } => candidates
                    .entry(*subject)
                    .or_default()
                    .extend(grants.clone()),
                Op::PublishGrantChange {
                    principal, grants, ..
                } => candidates
                    .entry(*principal)
                    .or_default()
                    .extend(grants.clone()),
                _ => {}
            }
        }
        for grants in candidates.values_mut() {
            grants.sort_by(|left, right| {
                serde_json::to_string(left)
                    .expect("security grant must serialize")
                    .cmp(&serde_json::to_string(right).expect("security grant must serialize"))
            });
            grants.dedup();
        }

        let mut resolved_grants = BTreeMap::new();
        for principal in &config.principals {
            let grants = candidates
                .get(&principal.actor)
                .into_iter()
                .flatten()
                .filter(|candidate| {
                    snapshot.grants().iter().any(|authoritative| {
                        authoritative_grant_matches(
                            authoritative,
                            &principal.principal_id,
                            candidate,
                        )
                    })
                })
                .cloned()
                .collect::<Vec<_>>();
            resolved_grants.insert(principal.actor, grants);
        }

        let mut resolved_keys = BTreeMap::<ActorSel, BTreeSet<KeySel>>::new();
        let mut revoked = BTreeSet::new();
        for (actor, actor_keys) in &self.known_key_ids {
            for (retired, key_id) in actor_keys {
                let selector = KeySel {
                    actor: ActorSel(*actor),
                    retired: *retired,
                };
                let key = snapshot
                    .keys()
                    .iter()
                    .find(|key| key.key_id().as_str() == key_id)
                    .unwrap_or_else(|| {
                        panic!(
                            "authoritative policy omitted known key {} for actor {} retired {}",
                            key_id, actor, retired
                        )
                    });
                resolved_keys
                    .entry(selector.actor)
                    .or_default()
                    .insert(selector);
                let inactive = key.state() == KeyState::Expired
                    || key.expires_at().is_some_and(|expires_at| expires_at <= now)
                    || (key.state() == KeyState::Revoked
                        && key.revokes_at().is_none_or(|revokes_at| revokes_at <= now));
                if inactive {
                    revoked.insert(selector);
                }
            }
        }

        self.live_grants = resolved_grants;
        self.policy_branches = vec![self.live_grants.clone()];
        self.live_keys = resolved_keys;
        self.revoked_credentials = revoked;
        assert!(
            version >= self.policy_version,
            "authoritative security policy regressed from {} to {version}",
            self.policy_version
        );
        self.policy_version = version;
        self.version_history.insert(op_index, version);
        self.indeterminate_mutations.clear();
        self.resolved_mutations.extend(mutation_resolutions);
        Ok(())
    }

    pub fn take_resolved_mutations(&mut self) -> Vec<SecurityMutationResolution> {
        std::mem::take(&mut self.resolved_mutations)
    }

    pub fn observe_applied(&mut self, op: &Op, response: &Value, op_index: u64) {
        if let Some(version) = response
            .get("policy_version")
            .and_then(Value::as_u64)
            .or_else(|| {
                response
                    .pointer("/body/policy_version")
                    .and_then(Value::as_u64)
            })
        {
            assert!(
                version >= self.policy_version,
                "security policy version regressed from {} to {version}",
                self.policy_version
            );
            self.policy_version = version;
            self.version_history.insert(op_index, version);
        }
        if self.operation_requires_durable_audit(op) {
            self.successful_audit_requests
                .insert(format!("adv-{op_index}-{}", op.kind()));
            if let Some(request_id) = response.get("request_id").and_then(Value::as_str) {
                self.successful_audit_requests
                    .insert(request_id.to_string());
            }
        }
        match op {
            Op::CreateKey { subject, .. } => {
                self.shift_actor_keys(*subject);
                self.register_response_key(
                    KeySel {
                        actor: *subject,
                        retired: 0,
                    },
                    response,
                );
            }
            Op::RotateKey { key, .. } => {
                assert_eq!(key.retired, 0, "only current credentials can rotate");
                self.shift_actor_keys(key.actor);
                self.revoked_credentials.insert(KeySel {
                    actor: key.actor,
                    retired: 1,
                });
                self.register_response_key(*key, response);
                self.open_credential_window(key.actor, op_index);
            }
            Op::RevokeKey { key, .. } => {
                self.revoked_credentials.insert(*key);
                self.live_keys.entry(key.actor).or_default().insert(*key);
                self.open_credential_window(key.actor, op_index);
            }
            Op::PublishGrantChange {
                principal,
                grants,
                change,
                ..
            } => {
                let old_grants = self.actor_grants(*principal).to_vec();
                let mut new_grants = old_grants.clone();
                apply_grant_change(&mut new_grants, grants, *change);
                self.open_policy_window(*principal, op_index, *change, grants.clone(), true);
                self.live_grants.insert(*principal, new_grants);
            }
            Op::MintToken {
                actor,
                token,
                narrowed,
            } => {
                assert_eq!(
                    *actor, token.parent,
                    "delegated token selector parent must equal the minting actor"
                );
                let token_id = response
                    .get("token_id")
                    .and_then(Value::as_str)
                    .unwrap_or_else(|| panic!("successful token mint omitted token_id"));
                let previous = self.delegated_tokens.insert(
                    token.artifact_key(),
                    DelegatedTokenModel {
                        token_id: token_id.to_string(),
                        parent: *actor,
                        narrowed: narrowed.clone(),
                        minted_at_op: op_index,
                    },
                );
                assert!(
                    previous.is_none(),
                    "token selector was minted more than once"
                );
            }
            _ => {}
        }
    }

    pub fn observe_ambiguous(&mut self, op: &Op, op_index: u64) {
        if matches!(
            op,
            Op::CreateKey { .. }
                | Op::RotateKey { .. }
                | Op::RevokeKey { .. }
                | Op::PublishGrantChange { .. }
        ) {
            self.indeterminate_mutations
                .push(IndeterminateSecurityMutation {
                    op_index,
                    op: op.clone(),
                });
        }
        match op {
            Op::RotateKey { key, .. } | Op::RevokeKey { key, .. } => {
                self.open_credential_window(key.actor, op_index);
            }
            Op::PublishGrantChange {
                principal,
                grants,
                change,
                ..
            } => {
                self.open_policy_window(*principal, op_index, *change, grants.clone(), false);
            }
            _ => {}
        }
    }

    fn open_credential_window(&mut self, actor: ActorSel, op_index: u64) {
        let closes_after = self.staleness_closes_after(op_index);
        self.staleness_windows.push(StalenessWindow {
            actor,
            opened_at: op_index,
            closes_after,
            transition: StalenessTransition::CredentialRevocation,
        });
    }

    fn open_policy_window(
        &mut self,
        actor: ActorSel,
        op_index: u64,
        change: GrantChange,
        changed_grants: Vec<SecurityGrantSpec>,
        definitely_applied: bool,
    ) {
        let _ = self
            .config
            .as_ref()
            .expect("policy window requires a configured security program")
            .principal(actor);
        if !self
            .staleness_windows
            .iter()
            .any(|window| matches!(&window.transition, StalenessTransition::Policy { .. }))
        {
            self.policy_branches = vec![self.live_grants.clone()];
        }
        assert!(
            !self.policy_branches.is_empty(),
            "policy transition requires at least one reachable whole-policy branch"
        );
        let old_states = self.policy_branches.clone();
        let new_states = old_states
            .iter()
            .map(|policy| policy_after_grant_change(policy, actor, &changed_grants, change))
            .collect::<Vec<_>>();
        let mut policy_states = old_states.clone();
        for state in &new_states {
            push_unique_policy_state(&mut policy_states, state.clone());
        }
        self.policy_branches = if definitely_applied {
            deduplicate_policy_states(new_states)
        } else {
            policy_states.clone()
        };
        let closes_after = self.staleness_closes_after(op_index);
        self.staleness_windows.push(StalenessWindow {
            actor,
            opened_at: op_index,
            closes_after,
            transition: StalenessTransition::Policy {
                policy_states,
                changed_grants,
            },
        });
    }

    fn staleness_closes_after(&self, op_index: u64) -> u64 {
        let bound = self
            .config
            .as_ref()
            .expect("staleness window requires a configured security program")
            .revocation_bound_ops;
        op_index
            .checked_add(bound)
            .expect("staleness window logical bound overflow")
    }

    #[must_use]
    pub fn expected_decision(&self, op: &Op, op_index: u64) -> Option<ExpectedDecision> {
        let active_windows = self
            .staleness_windows
            .iter()
            .filter(|window| {
                op_index <= window.closes_after && self.window_applies_to_op(window, op)
            })
            .collect::<Vec<_>>();
        match op {
            Op::CreateKey { .. }
            | Op::RotateKey { .. }
            | Op::RevokeKey { .. }
            | Op::PublishGrantChange { .. } => Some(self.expected_actor_operation_decision(
                op,
                &active_windows,
                AccessExpectation::Authorized,
            )),
            Op::MintToken { .. } => Some(self.expected_actor_operation_decision(
                op,
                &active_windows,
                AccessExpectation::Allow,
            )),
            Op::UseToken { .. } | Op::RevokeParentThenUseToken { .. } => {
                Some(self.expected_actor_operation_decision(
                    op,
                    &active_windows,
                    AccessExpectation::Allow,
                ))
            }
            Op::TokenExceedScopeProbe { .. } => Some(ExpectedDecision::Forbidden),
            Op::UseExpiredToken { .. } => Some(ExpectedDecision::Unauthorized),
            Op::AuditBarrierOp { .. }
            | Op::TenantBoundaryProbe { .. }
            | Op::SecurityAdminProbe { .. } => Some(self.expected_actor_operation_decision(
                op,
                &active_windows,
                AccessExpectation::Allow,
            )),
            Op::ExportProbe { .. } => Some(self.expected_actor_operation_decision(
                op,
                &active_windows,
                AccessExpectation::Authorized,
            )),
            Op::UseRevokedCredential { .. } => Some(if active_windows.is_empty() {
                ExpectedDecision::Unauthorized
            } else {
                ExpectedDecision::StalenessWindow {
                    allowed: vec![AccessExpectation::Allow, AccessExpectation::Unauthorized],
                }
            }),
            Op::ForbiddenWriteProbe {
                kind: ForbiddenWriteKind::CrossScopeDelete,
                ..
            } => Some(ExpectedDecision::Allow),
            Op::ForbiddenWriteProbe { .. } => Some(ExpectedDecision::Forbidden),
            _ => Some(self.expected_ordinary_decision(op, &active_windows)),
        }
    }

    pub fn close_staleness_windows(&mut self) {
        assert!(
            self.indeterminate_mutations.is_empty(),
            "cannot close security staleness windows before resolving ambiguous mutations"
        );
        self.staleness_windows.clear();
    }

    fn expected_ordinary_decision(
        &self,
        op: &Op,
        active_windows: &[&StalenessWindow],
    ) -> ExpectedDecision {
        self.expected_actor_operation_decision(op, active_windows, AccessExpectation::Authorized)
    }

    fn expected_actor_operation_decision(
        &self,
        op: &Op,
        active_windows: &[&StalenessWindow],
        success: AccessExpectation,
    ) -> ExpectedDecision {
        let mut allowed = self
            .possible_policy_states(active_windows)
            .iter()
            .map(|policy| {
                if self.operation_authorized_in_policy(op, policy) {
                    success
                } else {
                    AccessExpectation::Forbidden
                }
            })
            .collect::<BTreeSet<_>>();
        if active_windows.iter().any(|window| {
            matches!(
                &window.transition,
                StalenessTransition::CredentialRevocation
            )
        }) {
            allowed.insert(AccessExpectation::Unauthorized);
        }
        if allowed.len() == 1 {
            expected_decision_for_access(
                *allowed
                    .first()
                    .expect("one access expectation must be present"),
            )
        } else {
            ExpectedDecision::StalenessWindow {
                allowed: allowed.into_iter().collect(),
            }
        }
    }

    fn possible_policy_states(
        &self,
        active_windows: &[&StalenessWindow],
    ) -> Vec<BTreeMap<ActorSel, Vec<SecurityGrantSpec>>> {
        let config = self
            .config
            .as_ref()
            .expect("policy-state enumeration requires a configured security program");
        for principal in &config.principals {
            assert!(
                self.live_grants.contains_key(&principal.actor),
                "configured actor {} lost its modeled grant state",
                principal.actor.0
            );
        }
        let has_policy_history = self
            .staleness_windows
            .iter()
            .any(|window| matches!(&window.transition, StalenessTransition::Policy { .. }));
        let mut policies = if has_policy_history {
            assert!(
                !self.policy_branches.is_empty(),
                "policy history lost every reachable whole-policy branch"
            );
            self.policy_branches.clone()
        } else {
            vec![self.live_grants.clone()]
        };
        for window in active_windows {
            let StalenessTransition::Policy { policy_states, .. } = &window.transition else {
                continue;
            };
            for state in policy_states {
                push_unique_policy_state(&mut policies, state.clone());
            }
        }
        for policy in &policies {
            for principal in &config.principals {
                assert!(
                    policy.contains_key(&principal.actor),
                    "configured actor {} missing from reachable whole-policy state",
                    principal.actor.0
                );
            }
        }
        policies
    }

    fn operation_authorized_in_policy(
        &self,
        op: &Op,
        policy: &BTreeMap<ActorSel, Vec<SecurityGrantSpec>>,
    ) -> bool {
        let actor_authorized = if op.actor() == ActorSel::ADMIN {
            true
        } else {
            let _ = self
                .config
                .as_ref()
                .expect("operation authorization requires a configured security program")
                .principal(op.actor());
            let grants = policy.get(&op.actor()).unwrap_or_else(|| {
                panic!(
                    "configured actor {} missing from candidate policy",
                    op.actor().0
                )
            });
            match op {
                Op::UseToken { token, target_ns }
                | Op::TokenExceedScopeProbe { token, target_ns }
                | Op::UseExpiredToken { token, target_ns }
                | Op::RevokeParentThenUseToken { token, target_ns } => {
                    self.token_authorized_by_grants(*token, target_ns, grants)
                }
                _ => actor_operation_authorized_by_grants(op, grants),
            }
        };
        actor_authorized
            && match op {
                Op::CloneNamespace { source, target, .. } => {
                    policy_allows_namespace_copy(policy, source, target)
                }
                _ => true,
            }
    }

    fn token_authorized_by_grants(
        &self,
        token: TokenSel,
        namespace: &str,
        parent_grants: &[SecurityGrantSpec],
    ) -> bool {
        let token_model = self
            .delegated_tokens
            .get(&token.artifact_key())
            .unwrap_or_else(|| panic!("token use referenced an unminted selector"));
        token_model.parent == token.parent
            && token_model
                .narrowed
                .actions
                .iter()
                .any(|action| action == "Query")
            && token_model
                .narrowed
                .namespaces
                .iter()
                .any(|candidate| candidate == namespace)
            && grants_satisfy_requirement(
                parent_grants,
                GrantRequirement {
                    action: Action::Query,
                    namespace: Some(namespace),
                    unconstrained: false,
                },
            )
    }

    fn actor_grants(&self, actor: ActorSel) -> &[SecurityGrantSpec] {
        assert_ne!(
            actor,
            ActorSel::ADMIN,
            "implicit admin does not have modeled policy grants"
        );
        let _ = self
            .config
            .as_ref()
            .expect("actor grant lookup requires a configured security program")
            .principal(actor);
        self.live_grants
            .get(&actor)
            .unwrap_or_else(|| panic!("configured actor {} lost its modeled grant state", actor.0))
    }

    fn actor_has_action(&self, actor: ActorSel, namespace: Option<&str>, action: Action) -> bool {
        self.actor_grants(actor)
            .iter()
            .any(|grant| grant_scope_matches(grant, namespace) && grant_has_action(grant, action))
    }

    /// Mirrors the server's successful durable-audit boundary. VectorUpsert
    /// only joins that inventory when the acting principal exercised the
    /// explicit AttributeAdmin privilege; merely constructing an audit
    /// request for constraint handling does not guarantee a durable record.
    fn operation_requires_durable_audit(&self, op: &Op) -> bool {
        matches!(
            op,
            Op::CreateNamespace { .. }
                | Op::DeleteVectors { .. }
                | Op::CompactEndpoint { .. }
                | Op::CreateSnapshot { .. }
                | Op::DeleteSnapshot { .. }
                | Op::CloneNamespace { .. }
                | Op::PatchIndexConfig { .. }
                | Op::Hydrate { .. }
                | Op::DeleteNamespace { .. }
                | Op::CreateKey { .. }
                | Op::RotateKey { .. }
                | Op::RevokeKey { .. }
                | Op::PublishGrantChange { .. }
                | Op::MintToken { .. }
                | Op::ForbiddenWriteProbe { .. }
                | Op::AuditBarrierOp { .. }
        ) || matches!(
            op,
            Op::Upsert { actor, ns, .. }
                if *actor != ActorSel::ADMIN
                    && self.actor_has_action(*actor, Some(ns), Action::AttributeAdmin)
        )
    }

    fn window_applies_to_op(&self, window: &StalenessWindow, op: &Op) -> bool {
        match &window.transition {
            StalenessTransition::CredentialRevocation => window.actor == op.actor(),
            StalenessTransition::Policy { changed_grants, .. } => {
                let affects_actor_decision = window.actor == op.actor()
                    && changed_grants
                        .iter()
                        .any(|grant| grant_can_affect_operation(grant, op));
                let affects_clone_no_widening = match op {
                    Op::CloneNamespace { source, target, .. } => changed_grants
                        .iter()
                        .any(|grant| grant_can_affect_clone_policy(grant, source, target)),
                    _ => false,
                };
                affects_actor_decision || affects_clone_no_widening
            }
        }
    }

    #[must_use]
    pub fn expected_visible_ids(&self, model: &Model, actor: ActorSel) -> BTreeSet<String> {
        let Some(config) = &self.config else {
            return BTreeSet::new();
        };
        let _ = config.principal(actor);
        let Some(grant) = self
            .actor_grants(actor)
            .iter()
            .find(|grant| grant_has_action(grant, Action::Query))
        else {
            return BTreeSet::new();
        };
        let Some(namespace) = grant.namespace.as_deref() else {
            return BTreeSet::new();
        };
        let Some(namespace_model) = model.namespaces.get(namespace) else {
            return BTreeSet::new();
        };
        namespace_model
            .live
            .iter()
            .filter(|(_, record)| record_matches_grant(record, grant))
            .map(|(id, _)| id.clone())
            .collect()
    }

    #[must_use]
    pub fn expected_token_visible_ids(
        &self,
        model: &Model,
        token: TokenSel,
        namespace: &str,
    ) -> BTreeSet<String> {
        let Some(token_model) = self.delegated_tokens.get(&token.artifact_key()) else {
            return BTreeSet::new();
        };
        if !token_model
            .narrowed
            .actions
            .iter()
            .any(|action| action == "Query")
            || !token_model
                .narrowed
                .namespaces
                .iter()
                .any(|candidate| candidate == namespace)
        {
            return BTreeSet::new();
        }
        let Some(namespace_model) = model.namespaces.get(namespace) else {
            return BTreeSet::new();
        };
        let parent_grants = self
            .actor_grants(token_model.parent)
            .iter()
            .filter(|grant| {
                grant_has_action(grant, Action::Query)
                    && grant_scope_matches(grant, Some(namespace))
            })
            .collect::<Vec<_>>();
        if parent_grants.is_empty() {
            return BTreeSet::new();
        }
        namespace_model
            .live
            .iter()
            .filter(|(_, record)| {
                record_matches_all_parent_grants(record, &parent_grants)
                    && token_model
                        .narrowed
                        .mandatory_filter
                        .as_ref()
                        .is_none_or(|filter| record_matches_filter(record, filter))
            })
            .map(|(id, _)| id.clone())
            .collect()
    }
}

fn record_matches_all_parent_grants(record: &ModelRecord, grants: &[&SecurityGrantSpec]) -> bool {
    grants
        .iter()
        .all(|grant| record_matches_grant(record, grant))
}

#[derive(Debug, Clone, Copy)]
struct GrantRequirement<'a> {
    action: Action,
    namespace: Option<&'a str>,
    unconstrained: bool,
}

fn simple_operation_requirement(op: &Op) -> Option<GrantRequirement<'_>> {
    match op {
        Op::CreateNamespace { ns, .. } => Some(GrantRequirement {
            action: Action::NamespaceCreate,
            namespace: Some(ns),
            unconstrained: true,
        }),
        Op::GetNamespace { ns, .. } => Some(GrantRequirement {
            action: Action::NamespaceRead,
            namespace: Some(ns),
            unconstrained: true,
        }),
        Op::Upsert { ns, .. } => Some(GrantRequirement {
            action: Action::VectorUpsert,
            namespace: Some(ns),
            unconstrained: false,
        }),
        Op::DeleteVectors { ns, .. } => Some(GrantRequirement {
            action: Action::VectorDelete,
            namespace: Some(ns),
            unconstrained: false,
        }),
        Op::FetchVectors { ns, .. } => Some(GrantRequirement {
            action: Action::VectorFetch,
            namespace: Some(ns),
            unconstrained: false,
        }),
        Op::Query { ns, .. } | Op::BatchQuery { ns, .. } | Op::PaginateAll { ns, .. } => {
            Some(GrantRequirement {
                action: Action::Query,
                namespace: Some(ns),
                unconstrained: false,
            })
        }
        Op::InvalidProbe { ns, probe, .. } => Some(GrantRequirement {
            action: if probe.is_write_shaped() {
                Action::VectorUpsert
            } else {
                Action::Query
            },
            namespace: Some(ns),
            unconstrained: false,
        }),
        Op::CompactEndpoint { ns, .. } => Some(GrantRequirement {
            action: Action::CompactionTrigger,
            namespace: Some(ns),
            unconstrained: true,
        }),
        Op::CreateSnapshot { ns, .. } => Some(GrantRequirement {
            action: Action::SnapshotWrite,
            namespace: Some(ns),
            unconstrained: true,
        }),
        Op::GetSnapshot { ns, .. } | Op::ListSnapshots { ns, .. } => Some(GrantRequirement {
            action: Action::SnapshotRead,
            namespace: Some(ns),
            unconstrained: true,
        }),
        Op::DeleteSnapshot { ns, .. } => Some(GrantRequirement {
            action: Action::SnapshotDelete,
            namespace: Some(ns),
            unconstrained: true,
        }),
        Op::PatchIndexConfig { ns, .. } => Some(GrantRequirement {
            action: Action::IndexConfigWrite,
            namespace: Some(ns),
            unconstrained: true,
        }),
        Op::Hydrate { ns, .. } => Some(GrantRequirement {
            action: Action::HydrationTrigger,
            namespace: Some(ns),
            unconstrained: true,
        }),
        Op::DeleteNamespace { ns, .. } => Some(GrantRequirement {
            action: Action::NamespaceDelete,
            namespace: Some(ns),
            unconstrained: true,
        }),
        Op::TenantBoundaryProbe {
            target_ns, surface, ..
        } => Some(GrantRequirement {
            action: if *surface == TenantProbeSurface::Fetch {
                Action::VectorFetch
            } else {
                Action::Query
            },
            namespace: Some(target_ns),
            unconstrained: false,
        }),
        Op::ForbiddenWriteProbe {
            target_ns,
            kind: ForbiddenWriteKind::CrossScopeDelete,
            ..
        } => Some(GrantRequirement {
            action: Action::VectorDelete,
            namespace: Some(target_ns),
            unconstrained: false,
        }),
        Op::ForbiddenWriteProbe { target_ns, .. } => Some(GrantRequirement {
            action: Action::VectorUpsert,
            namespace: Some(target_ns),
            unconstrained: false,
        }),
        Op::SecurityAdminProbe { .. } | Op::AuditBarrierOp { .. } => Some(GrantRequirement {
            action: Action::SecurityAdminRead,
            namespace: None,
            unconstrained: true,
        }),
        Op::CreateKey { .. }
        | Op::RotateKey { .. }
        | Op::RevokeKey { .. }
        | Op::PublishGrantChange { .. } => Some(GrantRequirement {
            action: Action::SecurityAdminWrite,
            namespace: None,
            unconstrained: true,
        }),
        Op::MintToken { .. } => Some(GrantRequirement {
            action: Action::CredentialDelegate,
            namespace: None,
            unconstrained: true,
        }),
        Op::UseToken { target_ns, .. }
        | Op::TokenExceedScopeProbe { target_ns, .. }
        | Op::UseExpiredToken { target_ns, .. }
        | Op::RevokeParentThenUseToken { target_ns, .. } => Some(GrantRequirement {
            action: Action::Query,
            namespace: Some(target_ns),
            unconstrained: false,
        }),
        Op::GcCycle { .. }
        | Op::ProbeSandwich { .. }
        | Op::CompactInline { .. }
        | Op::CloneNamespace { .. }
        | Op::UseRevokedCredential { .. }
        | Op::ExportProbe { .. } => None,
    }
}

fn actor_operation_authorized_by_grants(op: &Op, grants: &[SecurityGrantSpec]) -> bool {
    match op {
        Op::CloneNamespace { source, target, .. } => [
            GrantRequirement {
                action: Action::NamespaceClone,
                namespace: Some(source),
                unconstrained: true,
            },
            GrantRequirement {
                action: Action::NamespaceRead,
                namespace: Some(source),
                unconstrained: true,
            },
            GrantRequirement {
                action: Action::NamespaceCreate,
                namespace: Some(target),
                unconstrained: true,
            },
        ]
        .into_iter()
        .all(|requirement| grants_satisfy_requirement(grants, requirement)),
        Op::ExportProbe { target_ns, .. } => [
            GrantRequirement {
                action: Action::VectorFetch,
                namespace: Some(target_ns),
                unconstrained: false,
            },
            GrantRequirement {
                action: Action::SnapshotRead,
                namespace: Some(target_ns),
                unconstrained: true,
            },
        ]
        .into_iter()
        .any(|requirement| grants_satisfy_requirement(grants, requirement)),
        _ => simple_operation_requirement(op)
            .is_some_and(|requirement| grants_satisfy_requirement(grants, requirement)),
    }
}

fn grants_satisfy_requirement(
    grants: &[SecurityGrantSpec],
    requirement: GrantRequirement<'_>,
) -> bool {
    let applicable = grants.iter().filter(|grant| {
        grant_has_action(grant, requirement.action)
            && grant_scope_matches(grant, requirement.namespace)
    });
    let mut matched = false;
    for grant in applicable {
        matched = true;
        if requirement.unconstrained && !grant_is_unconstrained(grant) {
            return false;
        }
    }
    matched
}

fn grant_scope_matches(grant: &SecurityGrantSpec, namespace: Option<&str>) -> bool {
    match namespace {
        Some(namespace) => {
            grant.namespace.is_none() || grant.namespace.as_deref() == Some(namespace)
        }
        None => grant.namespace.is_none(),
    }
}

fn grant_is_unconstrained(grant: &SecurityGrantSpec) -> bool {
    grant.mandatory_filter.is_none() && grant.write_constraints.as_ref().is_none_or(Value::is_null)
}

fn grant_has_action(grant: &SecurityGrantSpec, expected: Action) -> bool {
    parsed_grant_actions(grant).contains(&expected)
}

fn parsed_grant_actions(grant: &SecurityGrantSpec) -> BTreeSet<Action> {
    assert!(
        !grant.actions.is_empty(),
        "modeled grant actions must not be empty"
    );
    let mut parsed = BTreeSet::new();
    for raw in &grant.actions {
        let action = raw
            .parse::<Action>()
            .unwrap_or_else(|error| panic!("invalid modeled grant action {raw}: {error}"));
        assert!(
            parsed.insert(action),
            "modeled grant duplicated action {raw}"
        );
    }
    parsed
}

const MODELED_DERIVED_NAMESPACE_ACTIONS: [Action; 14] = [
    Action::NamespaceRead,
    Action::NamespaceDelete,
    Action::SnapshotRead,
    Action::SnapshotWrite,
    Action::SnapshotDelete,
    Action::IndexConfigWrite,
    Action::CompactionTrigger,
    Action::CompactionStatusRead,
    Action::HydrationTrigger,
    Action::VectorFetch,
    Action::VectorUpsert,
    Action::VectorDelete,
    Action::Query,
    Action::AttributeAdmin,
];

#[derive(Debug, Default)]
struct ModeledNamespaceScope {
    filter_conjuncts: BTreeSet<String>,
    stamps: BTreeMap<String, Value>,
    forbidden_fields: BTreeSet<String>,
}

fn policy_allows_namespace_copy(
    policy: &BTreeMap<ActorSel, Vec<SecurityGrantSpec>>,
    source: &str,
    target: &str,
) -> bool {
    for grants in policy.values() {
        for action in MODELED_DERIVED_NAMESPACE_ACTIONS {
            let target_scope = modeled_namespace_scope(grants, action, target);
            let Some(target_scope) = target_scope else {
                continue;
            };
            let Some(source_scope) = modeled_namespace_scope(grants, action, source) else {
                return false;
            };
            if !source_scope
                .filter_conjuncts
                .is_subset(&target_scope.filter_conjuncts)
                || !source_scope
                    .stamps
                    .iter()
                    .all(|(field, value)| target_scope.stamps.get(field) == Some(value))
                || !source_scope
                    .forbidden_fields
                    .is_subset(&target_scope.forbidden_fields)
            {
                return false;
            }
        }
    }
    true
}

fn modeled_namespace_scope(
    grants: &[SecurityGrantSpec],
    action: Action,
    namespace: &str,
) -> Option<ModeledNamespaceScope> {
    let mut matched = false;
    let mut scope = ModeledNamespaceScope::default();
    for grant in grants.iter().filter(|grant| {
        grant_scope_matches(grant, Some(namespace)) && grant_has_action(grant, action)
    }) {
        matched = true;
        if let Some(filter) = &grant.mandatory_filter {
            let filter = serde_json::from_value::<Filter>(filter.clone()).unwrap_or_else(|error| {
                panic!("invalid modeled mandatory filter for {action:?}: {error}")
            });
            collect_modeled_filter_conjuncts(&filter, &mut scope.filter_conjuncts);
        }
        if let Some(raw_constraints) = grant
            .write_constraints
            .as_ref()
            .filter(|constraints| !constraints.is_null())
        {
            let constraints = serde_json::from_value::<WriteConstraints>(raw_constraints.clone())
                .unwrap_or_else(|error| {
                    panic!("invalid modeled write constraints for {action:?}: {error}")
                });
            for (field, value) in constraints.stamp() {
                let value = serde_json::to_value(value)
                    .expect("modeled write-constraint stamp value must serialize");
                if let Some(existing) = scope.stamps.insert(field.clone(), value.clone()) {
                    assert_eq!(
                        existing, value,
                        "validated modeled policy carried conflicting stamp for {field}"
                    );
                }
            }
            scope
                .forbidden_fields
                .extend(constraints.forbidden_fields().iter().cloned());
        }
    }
    if grants.iter().any(|grant| {
        grant_scope_matches(grant, Some(namespace))
            && grant_has_action(grant, Action::AttributeAdmin)
    }) {
        scope.forbidden_fields.clear();
    }
    matched.then_some(scope)
}

fn collect_modeled_filter_conjuncts(filter: &Filter, output: &mut BTreeSet<String>) {
    if let Filter::And { filters } = filter {
        for filter in filters {
            collect_modeled_filter_conjuncts(filter, output);
        }
        return;
    }
    output.insert(serde_json::to_string(filter).expect("modeled mandatory filter must serialize"));
}

fn grant_can_affect_clone_policy(grant: &SecurityGrantSpec, source: &str, target: &str) -> bool {
    (grant_scope_matches(grant, Some(source)) || grant_scope_matches(grant, Some(target)))
        && MODELED_DERIVED_NAMESPACE_ACTIONS
            .into_iter()
            .any(|action| grant_has_action(grant, action))
}

fn grant_can_affect_operation(grant: &SecurityGrantSpec, op: &Op) -> bool {
    let matches = |requirement: GrantRequirement<'_>| {
        grant_has_action(grant, requirement.action)
            && grant_scope_matches(grant, requirement.namespace)
    };
    match op {
        Op::CloneNamespace { source, target, .. } => [
            GrantRequirement {
                action: Action::NamespaceClone,
                namespace: Some(source),
                unconstrained: true,
            },
            GrantRequirement {
                action: Action::NamespaceRead,
                namespace: Some(source),
                unconstrained: true,
            },
            GrantRequirement {
                action: Action::NamespaceCreate,
                namespace: Some(target),
                unconstrained: true,
            },
        ]
        .into_iter()
        .any(matches),
        Op::ExportProbe { target_ns, .. } => [
            GrantRequirement {
                action: Action::VectorFetch,
                namespace: Some(target_ns),
                unconstrained: false,
            },
            GrantRequirement {
                action: Action::SnapshotRead,
                namespace: Some(target_ns),
                unconstrained: true,
            },
        ]
        .into_iter()
        .any(matches),
        _ => simple_operation_requirement(op).is_some_and(matches),
    }
}

fn apply_grant_change(
    current: &mut Vec<SecurityGrantSpec>,
    changed: &[SecurityGrantSpec],
    change: GrantChange,
) {
    for grant in changed {
        let _ = parsed_grant_actions(grant);
    }
    match change {
        GrantChange::Add => {
            for grant in changed {
                if !current
                    .iter()
                    .any(|existing| same_grant_identity(existing, grant))
                {
                    current.push(grant.clone());
                }
            }
        }
        GrantChange::Remove => current.retain(|existing| {
            !changed
                .iter()
                .any(|removed| same_grant_identity(existing, removed))
        }),
    }
}

fn policy_after_grant_change(
    policy: &BTreeMap<ActorSel, Vec<SecurityGrantSpec>>,
    actor: ActorSel,
    changed: &[SecurityGrantSpec],
    change: GrantChange,
) -> BTreeMap<ActorSel, Vec<SecurityGrantSpec>> {
    let mut next = policy.clone();
    let grants = next
        .get_mut(&actor)
        .unwrap_or_else(|| panic!("policy transition omitted configured actor {}", actor.0));
    apply_grant_change(grants, changed, change);
    next
}

fn push_unique_policy_state(
    states: &mut Vec<BTreeMap<ActorSel, Vec<SecurityGrantSpec>>>,
    candidate: BTreeMap<ActorSel, Vec<SecurityGrantSpec>>,
) {
    if !states.contains(&candidate) {
        states.push(candidate);
    }
}

fn deduplicate_policy_states(
    states: Vec<BTreeMap<ActorSel, Vec<SecurityGrantSpec>>>,
) -> Vec<BTreeMap<ActorSel, Vec<SecurityGrantSpec>>> {
    let mut unique = Vec::with_capacity(states.len());
    for state in states {
        push_unique_policy_state(&mut unique, state);
    }
    unique
}

fn expected_decision_for_access(access: AccessExpectation) -> ExpectedDecision {
    match access {
        AccessExpectation::Authorized => ExpectedDecision::Authorized,
        AccessExpectation::Allow => ExpectedDecision::Allow,
        AccessExpectation::Unauthorized => ExpectedDecision::Unauthorized,
        AccessExpectation::Forbidden => ExpectedDecision::Forbidden,
    }
}

fn same_grant_identity(left: &SecurityGrantSpec, right: &SecurityGrantSpec) -> bool {
    left.namespace == right.namespace && parsed_grant_actions(left) == parsed_grant_actions(right)
}

fn authoritative_grant_matches(
    authoritative: &PolicyGrant,
    principal_id: &str,
    candidate: &SecurityGrantSpec,
) -> bool {
    if authoritative.principal_id().as_str() != principal_id || authoritative.field_mask().is_some()
    {
        return false;
    }
    let scope_matches = match authoritative.scope() {
        GrantScope::Global => candidate.namespace.is_none(),
        GrantScope::Namespace { namespace } => {
            candidate.namespace.as_deref() == Some(namespace.as_str())
        }
    };
    if !scope_matches {
        return false;
    }
    let actual_actions = match authoritative.actions() {
        GrantActions::All => return false,
        GrantActions::Selected { actions } => actions
            .iter()
            .map(|action| action.as_str().to_string())
            .collect::<BTreeSet<_>>(),
    };
    let expected_actions = candidate.actions.iter().cloned().collect::<BTreeSet<_>>();
    actual_actions == expected_actions
        && serde_json::to_value(authoritative.mandatory_filter())
            .expect("authoritative grant filter must serialize")
            == candidate
                .mandatory_filter
                .as_ref()
                .map_or(Value::Null, Clone::clone)
        && serde_json::to_value(authoritative.write_constraints())
            .expect("authoritative write constraints must serialize")
            == candidate
                .write_constraints
                .as_ref()
                .map_or(Value::Null, Clone::clone)
}

fn record_matches_grant(record: &ModelRecord, grant: &SecurityGrantSpec) -> bool {
    let Some(filter) = &grant.mandatory_filter else {
        return true;
    };
    let Some(field) = filter.get("field").and_then(Value::as_str) else {
        return false;
    };
    let Some(expected) = filter.get("value") else {
        return false;
    };
    let Some(attributes) = &record.attributes else {
        return false;
    };
    attributes.get(field).is_some_and(|actual| match actual {
        zeppelin::types::AttributeValue::String(value) => expected.as_str() == Some(value),
        zeppelin::types::AttributeValue::Integer(value) => expected.as_i64() == Some(*value),
        zeppelin::types::AttributeValue::Bool(value) => expected.as_bool() == Some(*value),
        _ => false,
    })
}

fn record_matches_filter(record: &ModelRecord, raw_filter: &Value) -> bool {
    let Some(attributes) = &record.attributes else {
        return false;
    };
    let filter = serde_json::from_value::<Filter>(raw_filter.clone())
        .unwrap_or_else(|error| panic!("invalid modeled delegated filter: {error}"));
    evaluate_filter(&filter, attributes)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpectedDecision {
    /// The actor is permitted to reach product validation/execution. The
    /// operation may still return a non-authz status such as 400, 404, or 410.
    Authorized,
    Allow,
    Unauthorized,
    Forbidden,
    /// Both the pre-publication and post-publication decisions are legal until
    /// the monotonic refresh bound closes.
    StalenessWindow {
        allowed: Vec<AccessExpectation>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AccessExpectation {
    /// Authorization passed; downstream product errors remain outside I22.
    Authorized,
    /// This probe itself must complete successfully.
    Allow,
    Unauthorized,
    Forbidden,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SecurityFinding {
    pub id: ViolationId,
    pub detail: String,
    pub evidence: Value,
}

#[derive(Debug, Clone)]
pub struct SecurityStateObservation {
    pub head_parsed: bool,
    pub checksum_valid: bool,
    pub observed_version: u64,
    pub minimum_version: u64,
    pub leaked_secret_locations: Vec<String>,
}

#[must_use]
pub fn check_i22_authz_decision(
    expected: ExpectedDecision,
    observed_status: u16,
) -> Option<SecurityFinding> {
    let accepted = match &expected {
        ExpectedDecision::Authorized => !matches!(observed_status, 401 | 403),
        ExpectedDecision::Allow => (200..300).contains(&observed_status),
        ExpectedDecision::Unauthorized => observed_status == 401,
        ExpectedDecision::Forbidden => observed_status == 403,
        ExpectedDecision::StalenessWindow { allowed } => allowed
            .iter()
            .any(|expected| access_expectation_accepts(*expected, observed_status)),
    };
    (!accepted).then(|| SecurityFinding {
        id: ViolationId::I22AuthzDecision,
        detail: "observed status disagreed with the policy model".to_string(),
        evidence: serde_json::json!({
            "expected": format!("{expected:?}"),
            "observed_status": observed_status,
        }),
    })
}

fn access_expectation_accepts(expected: AccessExpectation, observed_status: u16) -> bool {
    match expected {
        AccessExpectation::Authorized => !matches!(observed_status, 401 | 403),
        AccessExpectation::Allow => (200..300).contains(&observed_status),
        AccessExpectation::Unauthorized => observed_status == 401,
        AccessExpectation::Forbidden => observed_status == 403,
    }
}

#[must_use]
pub fn check_i23_tenant_leak(
    visible: &BTreeSet<String>,
    observed: &BTreeSet<String>,
) -> Option<SecurityFinding> {
    let leaked = observed.difference(visible).cloned().collect::<Vec<_>>();
    (!leaked.is_empty()).then(|| SecurityFinding {
        id: ViolationId::I23TenantLeak,
        detail: "response exposed ids outside the actor's modeled visible set".to_string(),
        evidence: serde_json::json!({
            "visible": visible,
            "observed": observed,
            "leaked": leaked,
        }),
    })
}

#[must_use]
pub fn check_i24_revocation_freshness(
    refresh_complete: bool,
    observed_status: u16,
) -> Option<SecurityFinding> {
    (refresh_complete && observed_status != 401).then(|| SecurityFinding {
        id: ViolationId::I24RevocationFreshness,
        detail: "revoked or expired credential was accepted after security refresh".to_string(),
        evidence: serde_json::json!({
            "refresh_complete": refresh_complete,
            "observed_status": observed_status,
            "expected_status": 401,
        }),
    })
}

#[must_use]
pub fn check_i25_audit_evidence(
    successful_request_ids: &BTreeSet<String>,
    durable_request_ids: &BTreeSet<String>,
) -> Option<SecurityFinding> {
    let missing = successful_request_ids
        .difference(durable_request_ids)
        .cloned()
        .collect::<Vec<_>>();
    (!missing.is_empty()).then(|| SecurityFinding {
        id: ViolationId::I25AuditEvidence,
        detail: "successful must-audit operations lacked durable evidence".to_string(),
        evidence: serde_json::json!({
            "successful_request_ids": successful_request_ids,
            "durable_request_ids": durable_request_ids,
            "missing": missing,
        }),
    })
}

#[must_use]
pub fn check_i26_security_state(observation: &SecurityStateObservation) -> Option<SecurityFinding> {
    let sane = observation.head_parsed
        && observation.checksum_valid
        && observation.observed_version >= observation.minimum_version
        && observation.leaked_secret_locations.is_empty();
    (!sane).then(|| SecurityFinding {
        id: ViolationId::I26SecurityStateSanity,
        detail:
            "authoritative security state failed parse, integrity, lineage, or redaction checks"
                .to_string(),
        evidence: serde_json::json!({
            "head_parsed": observation.head_parsed,
            "checksum_valid": observation.checksum_valid,
            "observed_version": observation.observed_version,
            "minimum_version": observation.minimum_version,
            "leaked_secret_locations": observation.leaked_secret_locations,
        }),
    })
}

#[must_use]
pub fn check_i27_constraint_drop(
    expected_visible: &BTreeSet<String>,
    observed_visible: &BTreeSet<String>,
) -> Option<SecurityFinding> {
    (expected_visible != observed_visible).then(|| {
        let missing = expected_visible
            .difference(observed_visible)
            .cloned()
            .collect::<Vec<_>>();
        let extra = observed_visible
            .difference(expected_visible)
            .cloned()
            .collect::<Vec<_>>();
        SecurityFinding {
            id: ViolationId::I27ConstraintDrop,
            detail: "quiet-period exhaustive visibility differed from the policy model".to_string(),
            evidence: serde_json::json!({
                "expected": expected_visible,
                "observed": observed_visible,
                "missing": missing,
                "extra": extra,
            }),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn i22_rejects_an_allowed_response_for_a_forbidden_operation() {
        let finding = check_i22_authz_decision(ExpectedDecision::Forbidden, 200)
            .expect("allowed forbidden operation must be detected");
        assert_eq!(finding.id, ViolationId::I22AuthzDecision);
    }

    #[test]
    fn i22_checks_ordinary_actor_carrying_operations_without_overriding_product_errors() {
        assert!(check_i22_authz_decision(ExpectedDecision::Authorized, 404).is_none());
        assert!(check_i22_authz_decision(ExpectedDecision::Authorized, 401).is_some());
        let op = Op::GetNamespace {
            actor: ActorSel::ADMIN,
            ns: "tenant-a".to_string(),
        };
        let mut model = SecurityPolicyModel::default();
        model.initialize(
            SecurityProgramConfig::for_seed(
                "ordinary-i22",
                &["tenant-a".to_string(), "tenant-b".to_string()],
            ),
            1,
        );
        assert_eq!(
            model.expected_decision(&op, 1),
            Some(ExpectedDecision::Authorized)
        );

        let permitted = Op::GetNamespace {
            actor: ActorSel(1),
            ns: "tenant-a".to_string(),
        };
        let cross_tenant = Op::GetNamespace {
            actor: ActorSel(1),
            ns: "tenant-b".to_string(),
        };
        assert_eq!(
            model.expected_decision(&permitted, 1),
            Some(ExpectedDecision::Authorized)
        );
        assert_eq!(
            model.expected_decision(&cross_tenant, 1),
            Some(ExpectedDecision::Forbidden)
        );
    }

    #[test]
    fn i22_models_clone_as_compound_unconstrained_authorization() {
        let mut model = SecurityPolicyModel::default();
        model.initialize(
            SecurityProgramConfig::for_seed(
                "clone-authz",
                &["tenant-a".to_string(), "tenant-b".to_string()],
            ),
            1,
        );
        let clone = Op::CloneNamespace {
            actor: ActorSel(1),
            source: "tenant-a".to_string(),
            target: "tenant-b".to_string(),
            as_of: super::super::ops::AsOfTarget::Generation(1),
        };
        let grant = |namespace: Option<&str>, actions: &[&str]| SecurityGrantSpec {
            namespace: namespace.map(str::to_string),
            actions: actions.iter().map(|action| (*action).to_string()).collect(),
            mandatory_filter: None,
            write_constraints: None,
        };

        model.live_grants.insert(
            ActorSel(1),
            vec![
                grant(Some("tenant-a"), &["NamespaceClone"]),
                grant(Some("tenant-b"), &["NamespaceCreate"]),
            ],
        );
        assert_eq!(
            model.expected_decision(&clone, 1),
            Some(ExpectedDecision::Forbidden),
            "clone requires source NamespaceRead in addition to NamespaceClone"
        );

        model.live_grants.insert(
            ActorSel(1),
            vec![
                grant(Some("tenant-a"), &["NamespaceClone"]),
                grant(Some("tenant-a"), &["NamespaceRead"]),
                grant(Some("tenant-c"), &["NamespaceCreate"]),
            ],
        );
        assert_eq!(
            model.expected_decision(&clone, 1),
            Some(ExpectedDecision::Forbidden),
            "clone requires NamespaceCreate authority on the exact target"
        );

        let mut constrained_clone = grant(Some("tenant-a"), &["NamespaceClone"]);
        constrained_clone.mandatory_filter = Some(json!({
            "op": "eq",
            "field": "group",
            "value": "g0"
        }));
        model.live_grants.insert(
            ActorSel(1),
            vec![
                constrained_clone,
                grant(Some("tenant-a"), &["NamespaceRead"]),
                grant(None, &["NamespaceCreate"]),
            ],
        );
        assert_eq!(
            model.expected_decision(&clone, 1),
            Some(ExpectedDecision::Forbidden),
            "every clone control decision must be unconstrained"
        );

        model.live_grants.insert(
            ActorSel(1),
            vec![
                grant(Some("tenant-a"), &["NamespaceClone"]),
                grant(Some("tenant-a"), &["NamespaceRead"]),
                grant(None, &["NamespaceCreate"]),
            ],
        );
        assert_eq!(
            model.expected_decision(&clone, 1),
            Some(ExpectedDecision::Forbidden),
            "target-only authority held by another principal fails no-widening"
        );

        let safe_clone = Op::CloneNamespace {
            actor: ActorSel(1),
            source: "tenant-a".to_string(),
            target: "tenant-c".to_string(),
            as_of: super::super::ops::AsOfTarget::Generation(1),
        };
        assert_eq!(
            model.expected_decision(&safe_clone, 1),
            Some(ExpectedDecision::Authorized)
        );
    }

    #[test]
    fn i22_staleness_accepts_the_authorized_status_family() {
        let mut model = SecurityPolicyModel::default();
        model.initialize(
            SecurityProgramConfig::for_seed(
                "stale-status",
                &["tenant-a".to_string(), "tenant-b".to_string()],
            ),
            1,
        );
        let create_grant = SecurityGrantSpec {
            namespace: None,
            actions: vec!["NamespaceCreate".to_string()],
            mandatory_filter: None,
            write_constraints: None,
        };
        model
            .live_grants
            .insert(ActorSel(1), vec![create_grant.clone()]);
        model.observe_applied(
            &Op::PublishGrantChange {
                actor: ActorSel::ADMIN,
                principal: ActorSel(1),
                grants: vec![create_grant],
                change: GrantChange::Remove,
            },
            &json!({"policy_version": 2}),
            10,
        );
        let create = Op::CreateNamespace {
            actor: ActorSel(1),
            ns: "tenant-new".to_string(),
            spec: super::super::ops::NamespaceSpec {
                dims: 2,
                metric: zeppelin::types::DistanceMetric::Euclidean,
                quantization: zeppelin::index::quantization::QuantizationType::None,
                num_centroids: 1,
                fts_fields: Vec::new(),
                bitmap: false,
            },
        };
        let expected = model
            .expected_decision(&create, 11)
            .expect("ordinary create must have an authz expectation");

        assert!(
            check_i22_authz_decision(expected.clone(), 201).is_none(),
            "old authorized policy may return Created during absorption"
        );
        assert!(
            check_i22_authz_decision(expected, 403).is_none(),
            "new revoked policy may return Forbidden during absorption"
        );
    }

    #[test]
    fn i22_composes_overlapping_policy_and_credential_windows() {
        let config = SecurityProgramConfig::for_seed(
            "overlap",
            &["tenant-a".to_string(), "tenant-b".to_string()],
        );
        let query_grant = config.principal(ActorSel(2)).grants[0].clone();
        let query = Op::TenantBoundaryProbe {
            actor: ActorSel(2),
            target_ns: "tenant-a".to_string(),
            surface: TenantProbeSurface::Query,
        };
        let mut model = SecurityPolicyModel::default();
        model.initialize(config, 1);
        model.observe_ambiguous(
            &Op::PublishGrantChange {
                actor: ActorSel::ADMIN,
                principal: ActorSel(2),
                grants: vec![query_grant.clone()],
                change: GrantChange::Remove,
            },
            10,
        );
        model.observe_ambiguous(
            &Op::PublishGrantChange {
                actor: ActorSel::ADMIN,
                principal: ActorSel(2),
                grants: vec![query_grant.clone()],
                change: GrantChange::Add,
            },
            11,
        );
        let expected = model.expected_decision(&query, 12).unwrap();
        assert!(
            check_i22_authz_decision(expected, 403).is_none(),
            "the earlier cached removed-policy state remains legal"
        );

        model.observe_ambiguous(
            &Op::RevokeKey {
                actor: ActorSel::ADMIN,
                key: KeySel {
                    actor: ActorSel(2),
                    retired: 0,
                },
            },
            13,
        );
        model.observe_ambiguous(
            &Op::PublishGrantChange {
                actor: ActorSel::ADMIN,
                principal: ActorSel(2),
                grants: vec![query_grant],
                change: GrantChange::Add,
            },
            14,
        );
        let expected = model.expected_decision(&query, 15).unwrap();
        assert!(
            check_i22_authz_decision(expected, 401).is_none(),
            "a later policy window must not hide credential revocation"
        );
    }

    #[test]
    fn i22_composes_same_actor_policy_transitions() {
        let mut model = SecurityPolicyModel::default();
        model.initialize(
            SecurityProgramConfig::for_seed(
                "same-actor-overlap",
                &["tenant-a".to_string(), "tenant-b".to_string()],
            ),
            1,
        );
        let grant = |action: &str| SecurityGrantSpec {
            namespace: Some("tenant-a".to_string()),
            actions: vec![action.to_string()],
            mandatory_filter: None,
            write_constraints: None,
        };
        let fetch = grant("VectorFetch");
        let snapshot = grant("SnapshotRead");
        model
            .live_grants
            .insert(ActorSel(1), vec![fetch.clone(), snapshot.clone()]);
        for (op_index, removed) in [(10, fetch), (11, snapshot)] {
            model.observe_ambiguous(
                &Op::PublishGrantChange {
                    actor: ActorSel::ADMIN,
                    principal: ActorSel(1),
                    grants: vec![removed],
                    change: GrantChange::Remove,
                },
                op_index,
            );
        }
        let export = Op::ExportProbe {
            actor: ActorSel(1),
            target_ns: "tenant-a".to_string(),
        };

        assert!(
            check_i22_authz_decision(model.expected_decision(&export, 12).unwrap(), 403).is_none(),
            "both independently absorbed removals can leave no export surface authorized"
        );
    }

    #[test]
    fn i22_never_mixes_actors_from_different_whole_policy_versions() {
        let mut model = SecurityPolicyModel::default();
        model.initialize(
            SecurityProgramConfig::for_seed(
                "whole-policy",
                &["tenant-a".to_string(), "tenant-b".to_string()],
            ),
            1,
        );
        for grants in model.live_grants.values_mut() {
            grants.clear();
        }
        let grant = |actor: ActorSel, namespace: &str| {
            (
                actor,
                SecurityGrantSpec {
                    namespace: Some(namespace.to_string()),
                    actions: vec!["Query".to_string()],
                    mandatory_filter: None,
                    write_constraints: None,
                },
            )
        };
        let (actor_one, actor_one_target) = grant(ActorSel(1), "tenant-b");
        model.live_grants.insert(actor_one, vec![actor_one_target]);

        let (actor_two, actor_two_target) = grant(ActorSel(2), "tenant-b");
        model.observe_applied(
            &Op::PublishGrantChange {
                actor: ActorSel::ADMIN,
                principal: actor_two,
                grants: vec![actor_two_target],
                change: GrantChange::Add,
            },
            &json!({"policy_version": 2}),
            10,
        );
        let (_, actor_one_source) = grant(actor_one, "tenant-a");
        model.observe_applied(
            &Op::PublishGrantChange {
                actor: ActorSel::ADMIN,
                principal: actor_one,
                grants: vec![actor_one_source],
                change: GrantChange::Add,
            },
            &json!({"policy_version": 3}),
            11,
        );
        let clone = Op::CloneNamespace {
            actor: ActorSel::ADMIN,
            source: "tenant-a".to_string(),
            target: "tenant-b".to_string(),
            as_of: super::super::ops::AsOfTarget::Generation(1),
        };

        assert_eq!(
            model.expected_decision(&clone, 12),
            Some(ExpectedDecision::Forbidden),
            "no reachable whole-policy version makes every principal clone-safe"
        );
    }

    #[test]
    fn staleness_windows_use_the_configured_logical_bound() {
        let mut config = SecurityProgramConfig::for_seed(
            "configured-bound",
            &["tenant-a".to_string(), "tenant-b".to_string()],
        );
        config.revocation_bound_ops = 2;
        let mut model = SecurityPolicyModel::default();
        model.initialize(config, 1);
        let key = KeySel {
            actor: ActorSel(4),
            retired: 0,
        };
        model.observe_ambiguous(
            &Op::RevokeKey {
                actor: ActorSel::ADMIN,
                key,
            },
            10,
        );
        let probe = Op::UseRevokedCredential { key };

        assert!(matches!(
            model.expected_decision(&probe, 12),
            Some(ExpectedDecision::StalenessWindow { .. })
        ));
        assert_eq!(
            model.expected_decision(&probe, 13),
            Some(ExpectedDecision::Unauthorized)
        );
    }

    #[test]
    #[should_panic(expected = "staleness window logical bound overflow")]
    fn staleness_window_bound_overflow_fails_loudly() {
        let mut config = SecurityProgramConfig::for_seed(
            "bound-overflow",
            &["tenant-a".to_string(), "tenant-b".to_string()],
        );
        config.revocation_bound_ops = u64::MAX;
        let mut model = SecurityPolicyModel::default();
        model.initialize(config, 1);
        model.observe_ambiguous(
            &Op::RevokeKey {
                actor: ActorSel::ADMIN,
                key: KeySel {
                    actor: ActorSel(4),
                    retired: 0,
                },
            },
            1,
        );
    }

    #[test]
    fn i22_authorizes_security_ops_from_the_acting_principals_grants() {
        let mut model = SecurityPolicyModel::default();
        model.initialize(
            SecurityProgramConfig::for_seed(
                "security-admin-authz",
                &["tenant-a".to_string(), "tenant-b".to_string()],
            ),
            1,
        );
        let grant = model.actor_grants(ActorSel(2))[0].clone();
        let publish = |actor| Op::PublishGrantChange {
            actor,
            principal: ActorSel(2),
            grants: vec![grant.clone()],
            change: GrantChange::Remove,
        };

        assert_eq!(
            model.expected_decision(&publish(ActorSel(1)), 1),
            Some(ExpectedDecision::Forbidden)
        );
        assert_eq!(
            model.expected_decision(&publish(ActorSel::ADMIN), 1),
            Some(ExpectedDecision::Authorized)
        );
        assert_eq!(
            model.expected_decision(&Op::AuditBarrierOp { actor: ActorSel(1) }, 1),
            Some(ExpectedDecision::Forbidden)
        );
        assert_eq!(
            model.expected_decision(&Op::AuditBarrierOp { actor: ActorSel(5) }, 1),
            Some(ExpectedDecision::Allow)
        );
    }

    #[test]
    fn grant_identity_uses_a_canonical_typed_action_set() {
        let grant = |actions: &[&str]| SecurityGrantSpec {
            namespace: Some("tenant-a".to_string()),
            actions: actions.iter().map(|action| (*action).to_string()).collect(),
            mandatory_filter: None,
            write_constraints: None,
        };

        assert!(same_grant_identity(
            &grant(&["Query", "VectorFetch"]),
            &grant(&["VectorFetch", "Query"]),
        ));
    }

    #[test]
    fn i22_export_probe_mirrors_any_surface_observation() {
        let mut model = SecurityPolicyModel::default();
        model.initialize(
            SecurityProgramConfig::for_seed(
                "partial-export",
                &["tenant-a".to_string(), "tenant-b".to_string()],
            ),
            1,
        );
        let export = Op::ExportProbe {
            actor: ActorSel(1),
            target_ns: "tenant-a".to_string(),
        };
        let partial = |action: &str| SecurityGrantSpec {
            namespace: Some("tenant-a".to_string()),
            actions: vec![action.to_string()],
            mandatory_filter: None,
            write_constraints: None,
        };

        model
            .live_grants
            .insert(ActorSel(1), vec![partial("VectorFetch")]);
        assert_eq!(
            model.expected_decision(&export, 1),
            Some(ExpectedDecision::Authorized)
        );

        model
            .live_grants
            .insert(ActorSel(1), vec![partial("SnapshotRead")]);
        let expected = model.expected_decision(&export, 1).unwrap();
        assert_eq!(expected, ExpectedDecision::Authorized);
        assert!(
            check_i22_authz_decision(expected, 404).is_none(),
            "an authorized read of the deliberately absent snapshot reaches downstream 404"
        );
    }

    #[test]
    #[should_panic(expected = "invalid modeled grant action invalid-action")]
    fn policy_model_validates_every_configured_action_eagerly() {
        let mut config = SecurityProgramConfig::for_seed(
            "invalid-action",
            &["tenant-a".to_string(), "tenant-b".to_string()],
        );
        config.principals[0].grants[0].actions =
            vec!["Query".to_string(), "invalid-action".to_string()];
        SecurityPolicyModel::default().initialize(config, 1);
    }

    #[test]
    #[should_panic(expected = "security program duplicated actor 1")]
    fn policy_model_rejects_duplicate_actor_selectors() {
        let mut config = SecurityProgramConfig::for_seed(
            "duplicate-actor",
            &["tenant-a".to_string(), "tenant-b".to_string()],
        );
        config.principals.push(config.principals[0].clone());
        SecurityPolicyModel::default().initialize(config, 1);
    }

    #[test]
    #[should_panic(expected = "security program omitted actor 99")]
    fn policy_model_fails_loudly_for_an_unconfigured_actor() {
        let mut model = SecurityPolicyModel::default();
        model.initialize(
            SecurityProgramConfig::for_seed(
                "unknown-actor",
                &["tenant-a".to_string(), "tenant-b".to_string()],
            ),
            1,
        );
        let op = Op::GetNamespace {
            actor: ActorSel(99),
            ns: "tenant-a".to_string(),
        };
        let _ = model.expected_decision(&op, 1);
    }

    #[test]
    fn i23_rejects_any_id_outside_the_actor_visible_set() {
        let finding = check_i23_tenant_leak(&ids(&["tenant-a"]), &ids(&["tenant-a", "tenant-b"]))
            .expect("cross-tenant id must be detected");
        assert_eq!(finding.id, ViolationId::I23TenantLeak);
    }

    #[test]
    fn i24_rejects_a_credential_after_the_refresh_barrier() {
        let finding = check_i24_revocation_freshness(true, 200)
            .expect("accepted revoked credential must be detected");
        assert_eq!(finding.id, ViolationId::I24RevocationFreshness);
    }

    #[test]
    fn i25_rejects_a_success_without_durable_audit_evidence() {
        let finding = check_i25_audit_evidence(&ids(&["request-1"]), &BTreeSet::new())
            .expect("missing durable audit record must be detected");
        assert_eq!(finding.id, ViolationId::I25AuditEvidence);
    }

    #[test]
    fn i25_models_successful_ordinary_mutations_with_the_workload_request_id() {
        let mut model = SecurityPolicyModel::default();
        model.initialize(
            SecurityProgramConfig::for_seed(
                "ordinary-audit",
                &["tenant-a".to_string(), "tenant-b".to_string()],
            ),
            1,
        );
        model.observe_applied(
            &Op::DeleteVectors {
                actor: ActorSel::ADMIN,
                ns: "tenant-a".to_string(),
                ids: vec!["row-1".to_string()],
            },
            &json!({}),
            9,
        );
        assert!(model
            .successful_audit_requests
            .contains("adv-9-delete_vectors"));

        model.observe_applied(
            &Op::Upsert {
                actor: ActorSel::ADMIN,
                ns: "tenant-a".to_string(),
                vectors: Vec::new(),
            },
            &json!({}),
            10,
        );
        assert!(
            !model.successful_audit_requests.contains("adv-10-upsert"),
            "ordinary successful upserts do not carry a DurableAudit obligation"
        );

        model
            .live_grants
            .get_mut(&ActorSel(2))
            .expect("tenant actor must have grants")
            .push(SecurityGrantSpec {
                namespace: Some("tenant-a".to_string()),
                actions: vec!["AttributeAdmin".to_string()],
                mandatory_filter: None,
                write_constraints: None,
            });
        model.observe_applied(
            &Op::Upsert {
                actor: ActorSel(2),
                ns: "tenant-a".to_string(),
                vectors: Vec::new(),
            },
            &json!({}),
            11,
        );
        assert!(model.successful_audit_requests.contains("adv-11-upsert"));
    }

    #[test]
    fn delegated_visibility_intersects_every_matching_parent_grant() {
        let record = ModelRecord {
            values: vec![1.0, 0.0],
            attributes: Some(std::collections::HashMap::from([
                (
                    "group".to_string(),
                    AttributeValue::String("g0".to_string()),
                ),
                ("bucket".to_string(), AttributeValue::Integer(1)),
            ])),
        };
        let group_grant = SecurityGrantSpec {
            namespace: Some("tenant-a".to_string()),
            actions: vec!["Query".to_string()],
            mandatory_filter: Some(json!({"field": "group", "value": "g0"})),
            write_constraints: None,
        };
        let wrong_bucket_grant = SecurityGrantSpec {
            namespace: Some("tenant-a".to_string()),
            actions: vec!["Query".to_string()],
            mandatory_filter: Some(json!({"field": "bucket", "value": 0})),
            write_constraints: None,
        };
        let matching_bucket_grant = SecurityGrantSpec {
            mandatory_filter: Some(json!({"field": "bucket", "value": 1})),
            ..wrong_bucket_grant.clone()
        };

        assert!(!record_matches_all_parent_grants(
            &record,
            &[&group_grant, &wrong_bucket_grant]
        ));
        assert!(record_matches_all_parent_grants(
            &record,
            &[&group_grant, &matching_bucket_grant]
        ));
    }

    fn empty_policy_snapshot(version: u64) -> PolicySnapshot {
        serde_json::from_value(json!({
            "version": version,
            "created_at": "2026-07-14T12:00:00Z",
            "created_by": "system:test",
            "checksum": "0000000000000000000000000000000000000000000000000000000000000000",
            "principals": [],
            "keys": [],
            "grants": []
        }))
        .expect("policy snapshot fixture must decode")
    }

    fn security_mutation_audit(
        request_id: &str,
        action: Action,
        old_version: u64,
        new_version: u64,
    ) -> AuditRecord {
        use std::net::{IpAddr, Ipv4Addr};
        use zeppelin::security::{ApiKeyId, PolicyVersion, Principal, PrincipalId, ResourceRef};

        let principal = Principal::api_key(
            PrincipalId::new("service:test-admin").expect("principal fixture must validate"),
            ApiKeyId::new("zpk1_test_admin").expect("key fixture must validate"),
            "test admin".to_string(),
            None,
        );
        let old_version =
            PolicyVersion::persisted(old_version).expect("old policy version must validate");
        let new_version =
            PolicyVersion::persisted(new_version).expect("new policy version must validate");
        AuditRecord::new(
            "2026-07-14T12:00:00Z"
                .parse()
                .expect("audit timestamp fixture must parse"),
            request_id,
            None,
            &principal,
            action,
            ResourceRef::SecurityPolicy,
            old_version,
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)),
            AuditOutcome::Success,
            AuditParams::SecurityPolicyChange {
                old_version,
                new_version,
            },
            "node-test",
        )
    }

    #[test]
    fn authoritative_resolution_classifies_each_ambiguous_security_mutation() {
        let config = SecurityProgramConfig::for_seed(
            "resolution",
            &["tenant-a".to_string(), "tenant-b".to_string()],
        );
        let grant = config.principal(ActorSel(2)).grants[0].clone();
        let mut model = SecurityPolicyModel::default();
        model.initialize(config, 1);
        model.observe_ambiguous(
            &Op::PublishGrantChange {
                actor: ActorSel::ADMIN,
                principal: ActorSel(2),
                grants: vec![grant.clone()],
                change: GrantChange::Remove,
            },
            20,
        );
        model.observe_ambiguous(
            &Op::PublishGrantChange {
                actor: ActorSel::ADMIN,
                principal: ActorSel(2),
                grants: vec![grant],
                change: GrantChange::Add,
            },
            30,
        );
        let records = BTreeMap::from([(
            "adv-30-publish_grant_change".to_string(),
            security_mutation_audit(
                "adv-30-publish_grant_change",
                Action::SecurityAdminWrite,
                1,
                2,
            ),
        )]);

        model
            .resolve_authoritative(
                &empty_policy_snapshot(2),
                "2026-07-14T12:00:00Z".parse().unwrap(),
                40,
                &records,
            )
            .expect("per-request audit lineage must resolve both mutations");

        assert!(model.indeterminate_mutations.is_empty());
        assert_eq!(
            model.take_resolved_mutations(),
            vec![
                SecurityMutationResolution {
                    op_index: 20,
                    effect: "publish_grant_change".to_string(),
                    request_id: "adv-20-publish_grant_change".to_string(),
                    resolved: SecurityMutationOutcome::NotApplied,
                    audit_outcome: None,
                    audit_policy_version: None,
                    published_policy_version: None,
                    authoritative_policy_version: 2,
                },
                SecurityMutationResolution {
                    op_index: 30,
                    effect: "publish_grant_change".to_string(),
                    request_id: "adv-30-publish_grant_change".to_string(),
                    resolved: SecurityMutationOutcome::Applied,
                    audit_outcome: Some("success".to_string()),
                    audit_policy_version: Some(1),
                    published_policy_version: Some(2),
                    authoritative_policy_version: 2,
                },
            ]
        );
    }

    #[test]
    fn failed_security_resolution_preserves_pending_mutations() {
        let config = SecurityProgramConfig::for_seed(
            "resolution-failure",
            &["tenant-a".to_string(), "tenant-b".to_string()],
        );
        let grant = config.principal(ActorSel(2)).grants[0].clone();
        let mut model = SecurityPolicyModel::default();
        model.initialize(config, 1);
        model.observe_ambiguous(
            &Op::PublishGrantChange {
                actor: ActorSel::ADMIN,
                principal: ActorSel(2),
                grants: vec![grant],
                change: GrantChange::Remove,
            },
            20,
        );
        let records = BTreeMap::from([(
            "adv-20-publish_grant_change".to_string(),
            security_mutation_audit("adv-20-publish_grant_change", Action::Query, 1, 2),
        )]);

        let error = model
            .resolve_authoritative(
                &empty_policy_snapshot(2),
                "2026-07-14T12:00:00Z".parse().unwrap(),
                40,
                &records,
            )
            .expect_err("wrong-action audit evidence must fail closed");
        assert!(error.contains("wrong audit action"));
        assert_eq!(model.indeterminate_mutations.len(), 1);
        assert!(model.resolved_mutations.is_empty());
    }

    #[test]
    fn i26_rejects_secret_material_in_security_or_audit_objects() {
        let finding = check_i26_security_state(&SecurityStateObservation {
            head_parsed: true,
            checksum_valid: true,
            observed_version: 8,
            minimum_version: 8,
            leaked_secret_locations: vec!["_audit/node.jsonl".to_string()],
        })
        .expect("plaintext credential material must be detected");
        assert_eq!(finding.id, ViolationId::I26SecurityStateSanity);
    }

    #[test]
    fn i27_rejects_a_quiet_period_constraint_drop() {
        let finding = check_i27_constraint_drop(&ids(&["tenant-a"]), &ids(&["tenant-b"]))
            .expect("quiet-period visible-set drift must be detected");
        assert_eq!(finding.id, ViolationId::I27ConstraintDrop);
    }

    #[test]
    fn policy_model_versions_grants_and_closes_bounded_staleness() {
        let tenant_a = "tenant-a".to_string();
        let tenant_b = "tenant-b".to_string();
        let config = SecurityProgramConfig::for_seed("policy-model", &[tenant_a.clone(), tenant_b]);
        let query_grant = config
            .principal(ActorSel(2))
            .grants
            .iter()
            .find(|grant| grant.actions == ["Query"])
            .cloned()
            .unwrap();
        let remove = Op::PublishGrantChange {
            actor: ActorSel::ADMIN,
            principal: ActorSel(2),
            grants: vec![query_grant.clone()],
            change: GrantChange::Remove,
        };
        let probe = Op::TenantBoundaryProbe {
            actor: ActorSel(2),
            target_ns: tenant_a,
            surface: TenantProbeSurface::Query,
        };
        let mut model = SecurityPolicyModel::default();
        model.initialize(config, 4);

        model.observe_applied(
            &remove,
            &json!({"policy_version": 5, "request_id": "grant-remove"}),
            20,
        );
        assert_eq!(model.version_history.get(&20), Some(&5));
        assert!(model.successful_audit_requests.contains("grant-remove"));
        assert_eq!(
            model.expected_decision(&probe, 21),
            Some(ExpectedDecision::StalenessWindow {
                allowed: vec![AccessExpectation::Allow, AccessExpectation::Forbidden],
            })
        );

        model.close_staleness_windows();
        assert_eq!(
            model.expected_decision(&probe, 40),
            Some(ExpectedDecision::Forbidden)
        );

        model.observe_applied(
            &Op::PublishGrantChange {
                actor: ActorSel::ADMIN,
                principal: ActorSel(2),
                grants: vec![query_grant],
                change: GrantChange::Add,
            },
            &json!({"policy_version": 6, "request_id": "grant-add"}),
            41,
        );
        model.close_staleness_windows();
        assert_eq!(
            model.expected_decision(&probe, 60),
            Some(ExpectedDecision::Allow)
        );
    }

    #[test]
    fn security_profile_script_exercises_wall_clock_expiry_after_the_clock_fault() {
        let config = SecurityProgramConfig::for_security_profile(
            "expiry",
            &["tenant-a".to_string(), "tenant-b".to_string()],
        );
        let ops = config.scripted_ops();
        assert!(matches!(
            &ops[usize::try_from(31 - SECURITY_PROGRAM_START_OP).unwrap()],
            Op::MintToken {
                token: TokenSel {
                    parent: ActorSel(2),
                    slot: 0,
                },
                narrowed: DelegatedTokenSpec {
                    expires_after_secs: 60,
                    ..
                },
                ..
            }
        ));
        assert!(matches!(
            &ops[usize::try_from(35 - SECURITY_PROGRAM_START_OP).unwrap()],
            Op::UseExpiredToken {
                token: TokenSel {
                    parent: ActorSel(2),
                    slot: 0,
                },
                ..
            }
        ));
        assert!(matches!(
            &ops[usize::try_from(SECURITY_AUDIT_BARRIER_OP - SECURITY_PROGRAM_START_OP).unwrap()],
            Op::AuditBarrierOp { .. }
        ));
    }

    #[test]
    fn delegated_token_model_intersects_narrowing_with_current_parent_grants() {
        let config = SecurityProgramConfig::for_seed(
            "delegation-model",
            &["tenant-a".to_string(), "tenant-b".to_string()],
        );
        let query_grant = config.principal(ActorSel(2)).grants[0].clone();
        let token = TokenSel {
            parent: ActorSel(2),
            slot: 7,
        };
        let narrowed = DelegatedTokenSpec {
            actions: vec!["Query".to_string()],
            namespaces: vec!["tenant-a".to_string()],
            mandatory_filter: Some(json!({
                "op": "eq",
                "field": "bucket",
                "value": 0
            })),
            purpose: "model-test".to_string(),
            expires_after_secs: 300,
        };
        let mint = Op::MintToken {
            actor: ActorSel(2),
            token,
            narrowed: narrowed.clone(),
        };
        let use_token = Op::UseToken {
            token,
            target_ns: "tenant-a".to_string(),
        };
        let exceed = Op::TokenExceedScopeProbe {
            token,
            target_ns: "tenant-b".to_string(),
        };
        let mut model = SecurityPolicyModel::default();
        model.initialize(config, 1);

        assert_eq!(
            model.expected_decision(&mint, 1),
            Some(ExpectedDecision::Allow)
        );
        model.observe_applied(&mint, &json!({"token_id": "token-model-7"}), 1);
        assert_eq!(
            model.expected_decision(&use_token, 2),
            Some(ExpectedDecision::Allow)
        );
        assert_eq!(
            model.expected_decision(&exceed, 2),
            Some(ExpectedDecision::Forbidden)
        );

        model.observe_applied(
            &Op::PublishGrantChange {
                actor: ActorSel::ADMIN,
                principal: ActorSel(2),
                grants: vec![query_grant],
                change: GrantChange::Remove,
            },
            &json!({"policy_version": 2}),
            3,
        );
        let revoked_use = Op::RevokeParentThenUseToken {
            token,
            target_ns: "tenant-a".to_string(),
        };
        assert_eq!(
            model.expected_decision(&revoked_use, 4),
            Some(ExpectedDecision::StalenessWindow {
                allowed: vec![AccessExpectation::Allow, AccessExpectation::Forbidden],
            })
        );
        model.close_staleness_windows();
        assert_eq!(
            model.expected_decision(&revoked_use, 20),
            Some(ExpectedDecision::Forbidden)
        );
    }
}
