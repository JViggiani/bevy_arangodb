use bevy::prelude::{Resource, World};

/// Unsafe world pointer published each frame to enable immediate application of DB results.
#[derive(Resource, Clone, Copy)]
pub struct ImmediateWorldPtr {
    pub(crate) ptr: *mut World,
}

impl ImmediateWorldPtr {
    #[inline]
    pub fn new(ptr: *mut World) -> Self {
        Self { ptr }
    }

    #[inline]
    pub fn set(&mut self, ptr: *mut World) {
        self.ptr = ptr;
    }

    #[inline]
    pub fn as_world(&self) -> &World {
        // Main thread only; pointer provided by exclusive systems
        unsafe { &*self.ptr }
    }

    #[inline]
    pub fn as_world_mut(&self) -> &mut World {
        // Main thread only; pointer provided by exclusive systems
        unsafe { &mut *self.ptr }
    }

    #[inline]
    pub fn publish(world: &mut World) {
        let ptr: *mut World = world as *mut World;
        if world.contains_resource::<ImmediateWorldPtr>() {
            world.resource_mut::<ImmediateWorldPtr>().set(ptr);
        } else {
            world.insert_resource(ImmediateWorldPtr::new(ptr));
        }
    }
}

// Safety: used only on the main thread in exclusive systems; we uphold Bevy's aliasing rules.
unsafe impl Send for ImmediateWorldPtr {}
unsafe impl Sync for ImmediateWorldPtr {}
