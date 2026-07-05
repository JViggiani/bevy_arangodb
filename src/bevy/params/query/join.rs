use bevy::ecs::query::{IterQueryData, QueryData, QueryFilter, QueryState, SingleEntityQueryData};
use bevy::ecs::system::QueryLens;
use bevy::prelude::{Entity, World};

use super::cache::CachePolicy;
use super::persistence_query_system_param::PersistentQuery;
use super::presence_spec::{FilterSupported, ToPresenceSpec, collect_presence_components};
use super::query_data_to_components::QueryDataToComponents;
use super::query_thread_local::{
    take_filter, take_relationship_load_spec,
};

impl<'w, 's, Q, F> PersistentQuery<'w, 's, Q, F>
where
    Q: QueryData<ReadOnly = Q> + IterQueryData + QueryDataToComponents,
    F: QueryFilter + ToPresenceSpec + FilterSupported,
{
    /// Smart join between two PersistentQuery params:
    /// - Builds a combined presence/value spec (intersection) and performs a single DB load.
    /// - Returns a Bevy QueryLens that views the world-only intersection of both queries.
    /// Note: World mutations are applied in PostUpdate (PreCommit); schedule this system accordingly.
    pub fn join_filtered<'a, 'w2, 's2, Q2, F2, NewD, NewF>(
        &'a mut self,
        other: &'a mut PersistentQuery<'w2, 's2, Q2, F2>,
    ) -> QueryLens<'a, NewD, NewF>
    where
        Q2: QueryData<ReadOnly = Q2> + IterQueryData + QueryDataToComponents,
        F2: QueryFilter + ToPresenceSpec + FilterSupported,
        NewD: SingleEntityQueryData,
        NewF: bevy::ecs::query::QueryFilter,
    {
        bevy::log::debug!(
            "PQ::join_filtered enter: lhs_type={} rhs_type={}",
            std::any::type_name::<Q>(),
            std::any::type_name::<Q2>(),
        );

        // Collect fetch names from both query data types
        let mut fetch_names: Vec<&'static str> = Vec::new();
        Q::push_names(&mut fetch_names);
        Q2::push_names(&mut fetch_names);

        // Presence specs from both filters (type-driven)
        let p1 = <F as ToPresenceSpec>::to_presence_spec();
        let p2 = <F2 as ToPresenceSpec>::to_presence_spec();

        let mut presence_with: Vec<&'static str> = Vec::new();
        let mut presence_without: Vec<&'static str> = Vec::new();
        presence_with.extend(p1.withs().iter().copied());
        presence_with.extend(p2.withs().iter().copied());
        presence_without.extend(p1.withouts().iter().copied());
        presence_without.extend(p2.withouts().iter().copied());

        // Collect components referenced by presence expressions so we fetch them too
        if let Some(expr) = p1.expr() {
            collect_presence_components(expr, &mut fetch_names);
        }
        if let Some(expr) = p2.expr() {
            collect_presence_components(expr, &mut fetch_names);
        }

        // Ensure we gate presence by all components being joined (true intersection)
        presence_with.extend(fetch_names.iter().copied());

        // Include presence-gated components in fetch list
        for &n in &presence_with {
            if !fetch_names.contains(&n) {
                fetch_names.push(n);
            }
        }

        // Dedup lists
        fn sort_dedup<T: Ord>(v: &mut Vec<T>) {
            v.sort_unstable();
            v.dedup();
        }
        sort_dedup(&mut fetch_names);
        sort_dedup(&mut presence_with);
        sort_dedup(&mut presence_without);

        // Combine presence/value expressions: (p1.expr AND p2.expr) AND (TLS filter, if any)
        let presence_expr = match (p1.expr().cloned(), p2.expr().cloned()) {
            (Some(a), Some(b)) => Some(a.and(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        let tls_expr = take_filter();
        let relationship_spec = take_relationship_load_spec();
        let store = super::query_thread_local::take_store()
            .unwrap_or_else(|| self.config.default_store.clone());
        let combined_expr = match (presence_expr, tls_expr) {
            (Some(a), Some(b)) => Some(a.and(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };

        bevy::log::debug!(
            "PQ::join_filtered spec: presence_with={:?} presence_without={:?} fetch_only={:?} expr={:?}",
            presence_with,
            presence_without,
            fetch_names,
            combined_expr
        );

        // For joined inline loads, always bypass cache to ensure materialization happens now.
        let cache_policy: CachePolicy = CachePolicy::ForceRefresh;
        bevy::log::trace!("PQ::join_filtered: forcing DB refresh for joined load");

        // Single DB load on the LHS with full docs; this materializes both sides' components.
        self.execute_combined_load(
            cache_policy,
            presence_with.clone(),
            presence_without.clone(),
            fetch_names.clone(),
            combined_expr.clone(),
            &[std::any::type_name::<Q2>()],
            true,
            store.clone(),
            relationship_spec,
        );

        // Warm-up using fresh QueryStates so counts reflect immediate inserts this tick.
        if let Some(ptr_res) = &self.world_ptr {
            let world: &World = ptr_res.as_world();
            let (lhs_cnt, rhs_cnt, joined_cnt) = {
                if let (Some(mut lhs_state), Some(mut rhs_state)) = (
                    QueryState::<(Entity, Q), F>::try_new(world),
                    QueryState::<(Entity, Q2), F2>::try_new(world),
                ) {
                    let lc = lhs_state.iter(world).count();
                    let rc = rhs_state.iter(world).count();
                    let mut joined_state: QueryState<NewD, NewF> =
                        lhs_state.join_filtered(world, &rhs_state);
                    let jc = joined_state.query(world).iter().count();
                    (lc, rc, jc)
                } else {
                    (0, 0, 0)
                }
            };
            bevy::log::trace!(
                "PQ::join_filtered warm-up: lhs_iter={} rhs_iter={} joined_preview={}",
                lhs_cnt,
                rhs_cnt,
                joined_cnt
            );
        }

        // Return a world-only joined view via the inner Bevy Query
        let lens = self.query.join_filtered(&mut other.query);
        bevy::log::debug!("PQ::join_filtered: returning lens");
        lens
    }
}
