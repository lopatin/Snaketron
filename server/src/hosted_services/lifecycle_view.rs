//! Adapts the host's [`TaskLifecycle`] to the narrow [`LifecycleView`] a
//! service sees, so the full lifecycle API stays internal.

use async_trait::async_trait;
use snaketron_service_api::deps::LifecycleView;

use crate::lifecycle::TaskLifecycle;

pub struct TaskLifecycleView {
    lifecycle: TaskLifecycle,
}

impl TaskLifecycleView {
    pub fn new(lifecycle: TaskLifecycle) -> Self {
        Self { lifecycle }
    }
}

#[async_trait]
impl LifecycleView for TaskLifecycleView {
    async fn on_drain(&self) {
        // Subscribe BEFORE the level check, then check: draining may already
        // have been announced, in which case the broadcast edge is gone
        // forever and a subscribe-then-wait would hang until shutdown. Doing
        // it in this order means neither the level nor the edge can be missed.
        let mut drains = self.lifecycle.subscribe_to_drain();
        if self.lifecycle.current_drain_notice().is_some() {
            return;
        }
        // A closed channel means the task is going away, which is as good a
        // reason to flush as an explicit drain.
        let _ = drains.recv().await;
    }

    fn is_draining(&self) -> bool {
        self.lifecycle.current_drain_notice().is_some()
    }
}
