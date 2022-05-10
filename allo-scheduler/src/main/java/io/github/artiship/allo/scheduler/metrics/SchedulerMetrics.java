package io.github.artiship.allo.scheduler.metrics;

import io.github.artiship.allo.scheduler.core.DependencyScheduler;
import io.github.artiship.allo.scheduler.core.JobStateStore;
import io.github.artiship.allo.scheduler.core.TaskDispatcher;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SchedulerMetrics {

    @Bean
    public Gauge taskSchedulerTaskQueuedGauge(MeterRegistry registry, DependencyScheduler dependencyScheduler) {
        return Gauge.builder("task.scheduler.queued.total", dependencyScheduler::queuedTaskCount)
                    .description("Tasks queued in task scheduler")
                    .register(registry);
    }

    @Bean
    public Gauge taskDispatcherTaskQueuedGauge(MeterRegistry registry, TaskDispatcher taskDispatcher) {
        return Gauge.builder("task.dispatcher.queued.total", taskDispatcher::queuedTaskCount)
                    .description("Tasks queued in task dispatcher")
                    .register(registry);
    }

    @Bean
    public Gauge taskDispatcherLimitedTaskQueuedGauge(MeterRegistry registry, TaskDispatcher taskDispatcher) {
        return Gauge.builder("task.dispatcher.queued.limited.total", taskDispatcher::limitedQueuedTaskCount)
                    .description("Limited tasks queued in task dispatcher")
                    .register(registry);
    }

    @Bean
    public Gauge taskStateTaskRunningGauge(MeterRegistry registry, JobStateStore jobStateStore) {
        return Gauge.builder("task.state.running.total", jobStateStore::runningTaskCounts)
                    .description("Tasks queued in task dispatcher")
                    .register(registry);
    }
}
