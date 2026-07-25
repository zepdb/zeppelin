//! Mechanically enumerable mapping from Axum routes to security actions.
//!
//! This file owns the single, auditable table that answers one question: *given
//! an HTTP method and the route template axum matched, is this endpoint
//! anonymous, or which [`Action`] does it require?* It is the bridge between the
//! transport layer in `src/server/` and the security vocabulary in
//! `src/security/`, and it is deliberately a flat `static` so the entire
//! authorization surface of the server can be read top to bottom as a document.
//!
//! It deliberately does **not** own:
//!
//! - *route registration* — the axum `Router` is built in `src/server/mod.rs`;
//!   this table mirrors it and a test proves the mirror is exact;
//! - *resource extraction* — turning `/v1/namespaces/:ns/query` into a typed
//!   [`Resource`](crate::security::Resource) is `route_resource` in `src/server/mod.rs`;
//! - *the decision* — whether the authenticated principal actually holds the
//!   action is [`SecurityKernel`](crate::security::SecurityKernel)'s job;
//! - *licensing* — a route naming `Action::NamespaceFork` says nothing about
//!   whether `Feature::Branching` is entitled; that check belongs in the kernel;
//! - *rate limiting* — `rate_limit_class` in `src/server/mod.rs` classifies
//!   independently.
//!
//! ## Where this sits
//!
//! ```text
//!   axum Router  --matches-->  MatchedPath ("/v1/namespaces/:ns/query")
//!                                    |
//!                                    v
//!   crate::server::authenticate --> classify_route(method, matched_path, readyz_public)
//!                                    |
//!            +-----------------------+-----------------------+
//!            |                       |                       |
//!            v                       v                       v
//!       Some(Public)        Some(Protected(action))         None
//!            |                       |                       |
//!   anonymous principal      authenticate, then       SecurityError::UnmappedRoute
//!   inserted; continue       crate::server::authorize      (fail closed)
//!                            -> SecurityKernel -> Decision
//! ```
//!
//! ## Reading map
//!
//! 1. [`RouteClass`] — the two possible classifications. `Public` is explicit;
//!    there is no implicit third state.
//! 2. [`ROUTE_ACTIONS`] — the inventory itself, ordered to mirror router
//!    declaration order.
//! 3. [`classify_route`] — the lookup, plus the two rules that are not visible
//!    in the table: implicit `HEAD` and the configurable `/readyz` exception.
//!
//! ## State and artifacts
//!
//! None. This module performs no I/O, holds no cache, and allocates nothing per
//! request. It is pure data plus one lookup function, which is precisely why it
//! can be trusted as the authorization index.
//!
//! ## Invariants
//!
//! - **Completeness is mechanically enforced.** `route_map_complete` in
//!   `tests/security_api_tests.rs` parses the router registrations out of
//!   `src/server/mod.rs` and asserts set equality with [`ROUTE_ACTIONS`]. Adding
//!   a route without adding an entry fails the test suite. At runtime a matched
//!   path with no entry yields `None`, which both middlewares turn into
//!   [`SecurityError::UnmappedRoute`](crate::security::SecurityError::UnmappedRoute) — an unmapped route is
//!   rejected, never allowed by default.
//! - **Classification is driven by the matched template, never the raw URI.**
//!   [`RouteAction::path`] is an axum `MatchedPath` template containing `:ns`,
//!   `:name`, `:key_id`, or `:lock_id` placeholders. A caller controls the
//!   concrete path but not which template axum matched, so path trickery cannot
//!   move a request into a weaker class.
//! - **Axum 0.7 parameter syntax.** These templates use `:param`, not `{param}`.
//!   Axum 0.8 changed the spelling; mixing them makes parameterized routes fail
//!   to match while static routes keep working, and here that failure surfaces as
//!   `UnmappedRoute` rather than as an obvious 404.
//! - **`HEAD` inherits its `GET` class.** Axum 0.7 dispatches `HEAD` through the
//!   registered `GET` handler and strips the body. That implicit route is never
//!   written in the table, so [`classify_route`] maps `HEAD` onto `GET` before
//!   lookup; otherwise every `HEAD` would be unmapped.
//!   `implicit_head_inherits_every_get_classification` pins this for every `GET`
//!   entry.
//! - **`Public` is rare, explicit, and configurable in exactly one place.** Only
//!   `/healthz` is unconditionally public. `/readyz` is
//!   `Protected(Action::SystemRead)` by default and downgrades to `Public` only
//!   when `security.readyz_public` is set, and only for `GET`/`HEAD` on that
//!   exact path. The override is applied *after* the table lookup, so it cannot
//!   introduce a route that is not otherwise mapped.
//! - **No duplicate `(method, path)` pairs.** [`classify_route`] takes the first
//!   match, so a duplicate entry could shadow a stricter one.
//!   `route_inventory_has_no_duplicate_method_path_pairs` forbids it.
//! - **Every `/v1/security/*` route is protected.** The exact method, path, and
//!   action triple for all fourteen security-administration routes is pinned by
//!   `security_route_inventory_is_exact_through_phase_ten`, and the test panics
//!   if any of them is ever classified `Public`.
//! - **Read and write are separated per resource.**
//!   `GET /v1/namespaces/:ns/branches` requires `Action::NamespaceRead` while
//!   `POST` on the same path requires the destructive, delegatable
//!   `Action::NamespaceFork`; the same split appears for snapshots, runtime
//!   config, and security administration. `Action::AttributeAdmin` appears
//!   nowhere here on purpose — it is a kernel-evaluated capability with no route
//!   of its own.
//! - **Conditional compilation is part of the surface.** The `/debug/pprof/cpu`
//!   entry exists only under the `profiling` feature and inherits
//!   `Action::MetricsRead`. Compiling that feature in without the entry would make
//!   the endpoint unmapped, not open.
//!
//! ## Rust concepts used here
//!
//! **A `static` table of `&'static str`.** [`ROUTE_ACTIONS`] is baked into the
//! binary: no lazy initialization, no lock, no allocation, and no reflective scan
//! of annotations as a Java framework would perform at startup. A C engineer can
//! read it as a `const` array of structs in `.rodata`. Because the paths are
//! `&'static str`, every entry outlives every request and can be borrowed freely
//! across `.await` points in the middleware.
//!
//! **`Copy` on [`RouteClass`], `Clone` on [`RouteAction`].** [`RouteClass`] is two
//! words and derives `Copy`, so `find(...).map(|entry| entry.class)` copies the
//! classification out and immediately ends the borrow of the static table. The
//! surrounding [`RouteAction`] is only `Clone` because [`Method`] owns a possible
//! heap-allocated extension method name; nothing on the request path clones one.
//!
//! **`Option<RouteClass>` as a fail-closed return.** [`classify_route`] returns
//! `None` for "not in the map" rather than a default class. Rust forces the caller
//! to destructure it, and both call sites in `src/server/mod.rs` do so with
//! `let ... else` and return an error. There is no `orElse(Public)` to write by
//! accident.
//!
//! **Comparison against a borrowed [`Method`].** The parameter is `&Method` and
//! the `HEAD` remapping produces `&Method::GET` — a reference to a `const`
//! constant — so the normalization step neither clones nor allocates, which
//! matters because it runs on every request including the query hot path.

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
        method: Method::GET,
        path: "/v1/namespaces/:ns/branches",
        class: RouteClass::Protected(Action::NamespaceRead),
    },
    RouteAction {
        method: Method::POST,
        path: "/v1/namespaces/:ns/branches",
        class: RouteClass::Protected(Action::NamespaceFork),
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
    RouteAction {
        method: Method::POST,
        path: "/v1/verify",
        class: RouteClass::Protected(Action::ReceiptVerify),
    },
    RouteAction {
        method: Method::GET,
        path: "/v1/namespaces/:ns/manifest/root",
        class: RouteClass::Protected(Action::NamespaceRead),
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
    fn security_route_inventory_is_exact_through_phase_ten() {
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
