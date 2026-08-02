//! Despawn / removal tracking for persisted entities and resources.

use bevy::prelude::*;

use crate::{bevy::components::Guid, core::session::PersistenceSession};

/// Detects removal of persisted resources and marks them for deletion.
pub(crate) fn auto_despawn_tracking_resource_system(ecs: &mut World) {
    let presence_snapshot = {
        let session = ecs.resource::<PersistenceSession>();
        session.resource_presence_snapshot(ecs)
    };

    let mut session = ecs.resource_mut::<PersistenceSession>();
    for (type_id, is_present) in presence_snapshot {
        let was_present = session.update_resource_presence(type_id, is_present);
        if was_present && !is_present {
            session.mark_resource_despawned_type_id(type_id);
        }
    }
}

/// Automatically marks despawned entities as needing deletion.
pub(crate) fn auto_despawn_tracking_system(
    mut session: ResMut<PersistenceSession>,
    mut removed: RemovedComponents<Guid>,
) {
    for entity in removed.read() {
        session.mark_despawned(entity);
    }
}
