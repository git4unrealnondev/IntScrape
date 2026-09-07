use std::{
    fmt::Debug,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use governor::{DefaultDirectRateLimiter, Quota};
use tokio::sync::Notify;

pub struct RatelimitManager {
    ratelimit: DefaultDirectRateLimiter,
    priority_queue: Arc<Mutex<Vec<(u64, u64)>>>,
    counter: AtomicU64,
    notify: Arc<Notify>,
}

impl Debug for RatelimitManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.priority_queue.lock().map(|q| q.len()).unwrap_or(0);
        f.debug_struct("RatelimitManager")
            .field("queue_len", &len)
            .finish()
    }
}

/// Sets up default ratelimiter
impl Default for RatelimitManager {
    fn default() -> Self {
        RatelimitManager::new(1, Duration::from_secs(1))
    }
}

impl RatelimitManager {
    /// Creates a new ratelimiter with user input
    pub fn new(regen_tokens: u32, time_per_regen: Duration) -> Self {
        let hits_nonzero = std::num::NonZeroU32::new(regen_tokens)
            .unwrap_or(std::num::NonZeroU32::new(1).unwrap());

        let cell_replenish_interval = time_per_regen / hits_nonzero.get();

        let quota = Quota::with_period(cell_replenish_interval)
            .unwrap()
            .allow_burst(hits_nonzero);

        Self {
            ratelimit: governor::RateLimiter::direct(quota),
            priority_queue: Arc::new(Mutex::new(Vec::new())),
            counter: AtomicU64::new(0),
            notify: Arc::new(Notify::new()),
        }
    }

    pub async fn wait(&self, priority: u64) {
        let numberinsert = self.counter.fetch_add(1, Ordering::Relaxed);

        // 1. Insert into the priority queue in sorted order using partition_point
        {
            let mut queue = self.priority_queue.lock().unwrap();
            let item = (priority, numberinsert);

            // Sort order: Higher priority first (descending), then smaller numberinsert first (FIFO ties)
            let pos = queue.partition_point(|existing| {
                existing.0 > priority || (existing.0 == priority && existing.1 < numberinsert)
            });

            queue.insert(pos, item);
        }

        // Wake up waiters to evaluate the new queue head
        self.notify.notify_waiters();

        // Drop guard to guarantee cancellation safety
        struct Guard {
            queue: Arc<Mutex<Vec<(u64, u64)>>>,
            notify: Arc<Notify>,
            numberinsert: u64,
            completed: bool,
        }

        impl Drop for Guard {
            fn drop(&mut self) {
                if !self.completed
                    && let Ok(mut queue) = self.queue.lock()
                    && let Some(pos) = queue.iter().position(|item| item.1 == self.numberinsert)
                {
                    queue.remove(pos);
                    self.notify.notify_waiters();
                }
            }
        }

        let mut guard = Guard {
            queue: Arc::clone(&self.priority_queue),
            notify: Arc::clone(&self.notify),
            numberinsert,
            completed: false,
        };

        // 2. Wait until we are at the head of the queue and acquire a rate-limit token
        loop {
            let is_head = {
                let queue = self.priority_queue.lock().unwrap();
                queue.first().is_some_and(|item| item.1 == numberinsert)
            };

            if is_head {
                // Use tokio::select! to listen for rate-limit readiness *and* queue preemption notifications
                tokio::select! {
                    _ = self.ratelimit.until_ready() => {
                        // Re-verify head status after acquiring the token
                        let mut queue = self.priority_queue.lock().unwrap();
                        if queue.first().is_some_and(|item| item.1 == numberinsert) {
                            if let Some(pos) = queue.iter().position(|item| item.1 == numberinsert) {
                                queue.remove(pos);
                            }
                            guard.completed = true;
                            drop(queue);

                            // Notify the next task in line
                            self.notify.notify_waiters();
                            return;
                        }
                    }
                    _ = self.notify.notified() => {
                        // The queue changed (e.g. a higher priority task arrived), re-evaluate head status
                    }
                }
            } else {
                // Wait efficiently for a notification rather than polling with a timer
                self.notify.notified().await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_high_and_low_priority_tasks() {
        // Create a ratelimiter with a 50ms replenishment rate
        let manager = Arc::new(RatelimitManager::new(1, Duration::from_millis(50)));

        // Consume the initial available token so the bucket is empty
        manager.wait(1).await;

        let execution_order = Arc::new(Mutex::new(Vec::new()));

        // 1. Spawn a low priority task (it will enter the queue and block waiting for a token)
        let manager_low = Arc::clone(&manager);
        let order_low = Arc::clone(&execution_order);
        let low_task = tokio::spawn(async move {
            manager_low.wait(1).await; // Low priority = 1
            order_low.lock().unwrap().push("low");
        });

        // Yield briefly to ensure the low-priority task is safely queued and waiting
        tokio::time::sleep(Duration::from_millis(10)).await;

        // 2. Spawn a high priority task (it will jump ahead and preempt the low-priority task)
        let manager_high = Arc::clone(&manager);
        let order_high = Arc::clone(&execution_order);
        let high_task = tokio::spawn(async move {
            manager_high.wait(10).await; // High priority = 10
            order_high.lock().unwrap().push("high");
        });

        // Wait for both tasks to complete
        let _ = tokio::try_join!(low_task, high_task);

        // Verify that the high priority task completed before the low priority task
        let order = execution_order.lock().unwrap();
        assert_eq!(*order, vec!["high", "low"]);
    }
}
