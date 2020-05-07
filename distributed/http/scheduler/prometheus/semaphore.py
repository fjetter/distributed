class SemaphoreMetricExtension:
    def __init__(self, dask_server):
        self.server = dask_server

    def collect(self):
        from prometheus_client.core import (
            GaugeMetricFamily,
            CounterMetricFamily,
        )

        sem_ext = self.server.extensions["semaphores"]

        semaphore_max_leases_family = GaugeMetricFamily(
            "semaphore_max_leases",
            "Maximum leases allowed per semaphore, this will be constant for each semaphore during its lifetime.",
            labels=["name"],
        )
        for semaphore_name, max_leases in sem_ext.max_leases.items():
            semaphore_max_leases_family.add_metric([semaphore_name], max_leases)
        yield semaphore_max_leases_family

        semaphore_active_leases_family = GaugeMetricFamily(
            "semaphore_active_leases",
            "Amount of currently active leases per semaphore.",
            labels=["name"],
        )
        for semaphore_name, semaphore_leases in sem_ext.leases.items():
            semaphore_active_leases_family.add_metric(
                [semaphore_name], len(semaphore_leases)
            )
        yield semaphore_active_leases_family

        semaphore_pending_leases = GaugeMetricFamily(
            "semaphore_pending_leases",
            "Amount of currently pending leases per semaphore.",
            labels=["name"],
        )
        for semaphore_name, count in sem_ext.metrics["pending"].items():
            semaphore_pending_leases.add_metric([semaphore_name], count)
        yield semaphore_pending_leases

        semaphore_acquire_total = CounterMetricFamily(
            "semaphore_acquire_total",
            "Total number of leases acquired per semaphore.",
            labels=["name"],
        )
        for semaphore_name, count in sem_ext.metrics["acquire_total"].items():
            semaphore_acquire_total.add_metric([semaphore_name], count)
        yield semaphore_acquire_total

        semaphore_release_total = CounterMetricFamily(
            "semaphore_release_total",
            "Total number of leases released per semaphore.\n"
            "Note: if a semaphore is closed while there are still leases active, this count will not equal "
            "`semaphore_acquired_total` after execution.",
            labels=["name"],
        )
        for semaphore_name, count in sem_ext.metrics["release_total"].items():
            semaphore_release_total.add_metric([semaphore_name], count)
        yield semaphore_release_total

        semaphore_average_pending_lease_time = GaugeMetricFamily(
            "semaphore_average_pending_lease_time",
            "Exponential moving average of the time it took to acquire a lease per semaphore.\n"
            "Note: this only includes time spent on scheduler side, "
            "it does"
            " not include time spent on communication.\n"
            "Note: this average is calculated based on order of leases instead of time of lease acquisition.",
            labels=["name"],
            unit="s",
        )
        for semaphore_name, moving_average in sem_ext.metrics[
            "average_pending_lease_time"
        ].items():
            semaphore_average_pending_lease_time.add_metric(
                [semaphore_name], moving_average
            )
        yield semaphore_average_pending_lease_time
