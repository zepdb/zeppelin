//! Mechanically enumerable mapping from Axum routes to security actions.

use axum::http::Method;

use super::Action;

/// Security classification of one method and normalized route template.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteClass {
    /// Explicitly anonymous route.
    Public,
    /// Protected route requiring the attached action.
    Protected(Action),
}

/// One entry in the central route-to-action inventory.
#[derive(Debug, Clone)]
pub struct RouteAction {
    /// HTTP method registered by Axum.
    pub method: Method,
    /// Axum `MatchedPath` template, never a caller-controlled raw path.
    pub path: &'static str,
    /// Public classification or required action.
    pub class: RouteClass,
}

/// Complete phase-3 route/action inventory in router declaration order.
pub static ROUTE_ACTIONS: &[RouteAction] = &[
    RouteAction {
        method: Method::GET,
        path: "/healthz",
        class: RouteClass::Public,
    },
    RouteAction {
        method: Method::GET,
        path: "/readyz",
        class: RouteClass::Protected(Action::SystemRead),
    },
    RouteAction {
        method: Method::GET,
        path: "/metrics",
        class: RouteClass::Protected(Action::MetricsRead),
    },
    #[cfg(feature = "profiling")]
    RouteAction {
        method: Method::GET,
        path: "/debug/pprof/cpu",
        class: RouteClass::Protected(Action::MetricsRead),
    },
    RouteAction {
        method: Method::GET,
        path: "/v1/config/query",
        class: RouteClass::Protected(Action::RuntimeConfigRead),
    },
    RouteAction {
        method: Method::PATCH,
        path: "/v1/config/query",
        class: RouteClass::Protected(Action::RuntimeConfigWrite),
    },
    RouteAction {
        method: Method::PUT,
        path: "/v1/config/query",
        class: RouteClass::Protected(Action::RuntimeConfigWrite),
    },
    RouteAction {
        method: Method::GET,
        path: "/v1/security/principals",
        class: RouteClass::Protected(Action::SecurityAdminRead),
    },
    RouteAction {
        method: Method::POST,
        path: "/v1/security/principals",
        class: RouteClass::Protected(Action::SecurityAdminWrite),
    },
    RouteAction {
        method: Method::GET,
        path: "/v1/security/keys",
        class: RouteClass::Protected(Action::SecurityAdminRead),
    },
    RouteAction {
        method: Method::POST,
        path: "/v1/security/keys",
        class: RouteClass::Protected(Action::SecurityAdminWrite),
    },
    RouteAction {
        method: Method::POST,
        path: "/v1/security/keys/:key_id/rotate",
        class: RouteClass::Protected(Action::SecurityAdminWrite),
    },
    RouteAction {
        method: Method::DELETE,
        path: "/v1/security/keys/:key_id",
        class: RouteClass::Protected(Action::SecurityAdminWrite),
    },
    RouteAction {
        method: Method::GET,
        path: "/v1/security/grants",
        class: RouteClass::Protected(Action::SecurityAdminRead),
    },
    RouteAction {
        method: Method::POST,
        path: "/v1/security/grants",
        class: RouteClass::Protected(Action::SecurityAdminWrite),
    },
    RouteAction {
        method: Method::DELETE,
        path: "/v1/security/grants",
        class: RouteClass::Protected(Action::SecurityAdminWrite),
    },
    RouteAction {
        method: Method::GET,
        path: "/v1/security/policy",
        class: RouteClass::Protected(Action::SecurityAdminRead),
    },
    RouteAction {
        method: Method::POST,
        path: "/v1/security/tokens",
        class: RouteClass::Protected(Action::CredentialDelegate),
    },
    RouteAction {
        method: Method::GET,
        path: "/v1/security/preservation",
        class: RouteClass::Protected(Action::PreservationAdmin),
    },
    RouteAction {
        method: Method::POST,
        path: "/v1/security/preservation",
        class: RouteClass::Protected(Action::PreservationAdmin),
    },
    RouteAction {
        method: Method::POST,
        path: "/v1/security/preservation/:lock_id/release",
        class: RouteClass::Protected(Action::PreservationRelease),
    },
    RouteAction {
        method: Method::POST,
        path: "/v1/namespaces",
        class: RouteClass::Protected(Action::NamespaceCreate),
    },
    RouteAction {
        method: Method::GET,
        path: "/v1/namespaces/:ns",
        class: RouteClass::Protected(Action::NamespaceRead),
    },
    RouteAction {
        method: Method::DELETE,
        path: "/v1/namespaces/:ns",
        class: RouteClass::Protected(Action::NamespaceDelete),
    },
    RouteAction {
        method: Method::GET,
        path: "/v1/namespaces/:ns/snapshots",
        class: RouteClass::Protected(Action::SnapshotRead),
    },
    RouteAction {
        method: Method::GET,
        path: "/v1/namespaces/:ns/snapshots/:name",
        class: RouteClass::Protected(Action::SnapshotRead),
    },
    RouteAction {
        method: Method::PUT,
        path: "/v1/namespaces/:ns/snapshots/:name",
        class: RouteClass::Protected(Action::SnapshotWrite),
    },
    RouteAction {
        method: Method::DELETE,
        path: "/v1/namespaces/:ns/snapshots/:name",
        class: RouteClass::Protected(Action::SnapshotDelete),
    },
    RouteAction {
        method: Method::POST,
        path: "/v1/namespaces/:ns/clone",
        class: RouteClass::Protected(Action::NamespaceClone),
    },
    RouteAction {
        method: Method::PATCH,
        path: "/v1/namespaces/:ns/index_config",
        class: RouteClass::Protected(Action::IndexConfigWrite),
    },
    RouteAction {
        method: Method::POST,
        path: "/v1/namespaces/:ns/compact",
        class: RouteClass::Protected(Action::CompactionTrigger),
    },
    RouteAction {
        method: Method::GET,
        path: "/v1/namespaces/:ns/compact/status",
        class: RouteClass::Protected(Action::CompactionStatusRead),
    },
    RouteAction {
        method: Method::POST,
        path: "/v1/namespaces/:ns/hydrate",
        class: RouteClass::Protected(Action::HydrationTrigger),
    },
    RouteAction {
        method: Method::POST,
        path: "/v1/namespaces/:ns/vectors/get",
        class: RouteClass::Protected(Action::VectorFetch),
    },
    RouteAction {
        method: Method::POST,
        path: "/v1/namespaces/:ns/vectors",
        class: RouteClass::Protected(Action::VectorUpsert),
    },
    RouteAction {
        method: Method::DELETE,
        path: "/v1/namespaces/:ns/vectors",
        class: RouteClass::Protected(Action::VectorDelete),
    },
    RouteAction {
        method: Method::POST,
        path: "/v1/namespaces/:ns/query",
        class: RouteClass::Protected(Action::Query),
    },
    RouteAction {
        method: Method::POST,
        path: "/v1/namespaces/:ns/query/batch",
        class: RouteClass::Protected(Action::Query),
    },
];

/// Classify a registered route, applying the configurable readiness exception.
#[must_use]
pub fn classify_route(
    method: &Method,
    matched_path: &str,
    readyz_public: bool,
) -> Option<RouteClass> {
    // Axum 0.7 dispatches HEAD through a registered GET endpoint and strips the
    // response body. Inherit the exact GET authorization class so this implicit
    // framework route cannot escape the central map or fail as "unmapped".
    let mapped_method = if method == Method::HEAD {
        &Method::GET
    } else {
        method
    };
    let class = ROUTE_ACTIONS
        .iter()
        .find(|entry| entry.method == *mapped_method && entry.path == matched_path)
        .map(|entry| entry.class)?;
    if readyz_public && mapped_method == Method::GET && matched_path == "/readyz" {
        Some(RouteClass::Public)
    } else {
        Some(class)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{classify_route, RouteClass, ROUTE_ACTIONS};
    use crate::security::Action;

    #[test]
    fn route_inventory_has_no_duplicate_method_path_pairs() {
        let mut seen = HashSet::new();
        for entry in ROUTE_ACTIONS {
            assert!(seen.insert((entry.method.clone(), entry.path)));
        }
    }

    #[test]
    fn readiness_public_override_is_explicit() {
        assert_eq!(
            classify_route(&axum::http::Method::GET, "/readyz", false),
            Some(RouteClass::Protected(Action::SystemRead))
        );
        assert_eq!(
            classify_route(&axum::http::Method::GET, "/readyz", true),
            Some(RouteClass::Public)
        );
    }

    #[test]
    fn security_route_inventory_is_exact_through_phase_eight() {
        let routes = ROUTE_ACTIONS
            .iter()
            .filter(|entry| entry.path.starts_with("/v1/security"))
            .map(|entry| {
                let RouteClass::Protected(action) = entry.class else {
                    panic!("security administration routes must never be public");
                };
                (entry.method.as_str(), entry.path, action.as_str())
            })
            .collect::<Vec<_>>();

        assert_eq!(
            routes,
            vec![
                ("GET", "/v1/security/principals", "SecurityAdminRead"),
                ("POST", "/v1/security/principals", "SecurityAdminWrite"),
                ("GET", "/v1/security/keys", "SecurityAdminRead"),
                ("POST", "/v1/security/keys", "SecurityAdminWrite"),
                (
                    "POST",
                    "/v1/security/keys/:key_id/rotate",
                    "SecurityAdminWrite"
                ),
                ("DELETE", "/v1/security/keys/:key_id", "SecurityAdminWrite"),
                ("GET", "/v1/security/grants", "SecurityAdminRead"),
                ("POST", "/v1/security/grants", "SecurityAdminWrite"),
                ("DELETE", "/v1/security/grants", "SecurityAdminWrite"),
                ("GET", "/v1/security/policy", "SecurityAdminRead"),
                ("POST", "/v1/security/tokens", "CredentialDelegate"),
                ("GET", "/v1/security/preservation", "PreservationAdmin"),
                ("POST", "/v1/security/preservation", "PreservationAdmin"),
                (
                    "POST",
                    "/v1/security/preservation/:lock_id/release",
                    "PreservationRelease"
                ),
            ]
        );
    }

    #[test]
    fn implicit_head_inherits_every_get_classification() {
        for entry in ROUTE_ACTIONS
            .iter()
            .filter(|entry| entry.method == axum::http::Method::GET)
        {
            assert_eq!(
                classify_route(&axum::http::Method::HEAD, entry.path, false),
                Some(entry.class),
                "HEAD {} must inherit its GET security class",
                entry.path
            );
        }
        assert_eq!(
            classify_route(&axum::http::Method::HEAD, "/readyz", true),
            Some(RouteClass::Public)
        );
    }
}
