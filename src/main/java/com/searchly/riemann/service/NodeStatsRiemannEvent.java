package com.searchly.riemann.service;

import com.aphyr.riemann.client.RiemannClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.monitor.fs.FsStats;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.OsStats;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ferhat
 *
 * TODO: Expand metrics recorded? http://radar.oreilly.com/2015/04/10-elasticsearch-metrics-to-watch.html
 */
public class NodeStatsRiemannEvent {

    private RiemannClient riemannClient;
    private String hostDefinition;
    private Settings settings;
    private static NodeStatsRiemannEvent nodeStatsRiemannEvent;
    private Map<String, Long> deltaMap;
    private String[] tags;
    private Map<String, String> attributes;

    public static NodeStatsRiemannEvent getNodeStatsRiemannEvent(RiemannClient riemannClient,
                                                                 Settings settings, String hostDefinition, String clusterName, String[] tags, Map<String, String> attributes) {
        if (nodeStatsRiemannEvent == null) {
            nodeStatsRiemannEvent = new NodeStatsRiemannEvent(riemannClient, settings, hostDefinition, clusterName, tags, attributes);
        }
        return nodeStatsRiemannEvent;
    }

    private NodeStatsRiemannEvent(RiemannClient riemannClient, Settings settings, String hostDefinition, String clusterName, String[] tags, Map<String, String> attributes) {
        this.riemannClient = riemannClient;
        this.hostDefinition = hostDefinition;
        this.settings = settings;
        this.tags = tags;
        this.attributes = attributes;
        this.deltaMap = new HashMap<>();
        // init required delta instead of null check
        deltaMap.put("index_rate", 0L);
        deltaMap.put("query_rate", 0L);
        deltaMap.put("fetch_rate", 0L);
        deltaMap.put("query_time", 0L);

    }

    public void sendEvents(MonitorService monitorService, NodeIndicesStats nodeIndicesStats) {

        //JVM
        //mem
        if (settings.getAsBoolean("metrics.riemann.heap_ratio.enabled", true)) {
            long ok = settings.getAsLong("metrics.riemann.heap_ratio.ok", 85L);
            long warning = settings.getAsLong("metrics.riemann.heap_ratio.warning", 95L);
            heapRatio(monitorService.jvmService().stats(), ok, warning);
        }
        if (settings.getAsBoolean("metrics.riemann.heap.enabled", true)) {
            heap(monitorService.jvmService().stats());
        }
        if (settings.getAsBoolean("metrics.riemann.non_heap.enabled", true)) {
            nonHeap(monitorService.jvmService().stats());
        }

        if (settings.getAsBoolean("metrics.riemann.current_query_rate.enabled", true)) {
            long ok = settings.getAsLong("metrics.riemann.current_query_rate.ok", 50L);
            long warning = settings.getAsLong("metrics.riemann.current_query_rate.warning", 70L);
            currentQueryRate(nodeIndicesStats, ok, warning);
        }
        if (settings.getAsBoolean("metrics.riemann.query_time.enabled", true)) {
            queryTime(nodeIndicesStats);
        }

        if (settings.getAsBoolean("metrics.riemann.current_fetch_rate.enabled", true)) {
            long ok = settings.getAsLong("metrics.riemann.current_fetch_rate.ok", 50L);
            long warning = settings.getAsLong("metrics.riemann.current_fetch_rate.warning", 70L);
            currentFetchRate(nodeIndicesStats, ok, warning);
        }

        if (settings.getAsBoolean("metrics.riemann.current_indexing_rate.enabled", true)) {
            long ok = settings.getAsLong("metrics.riemann.current_indexing_rate.ok", 300L);
            long warning = settings.getAsLong("metrics.riemann.current_indexing_rate.warning", 1000L);
            currentIndexingRate(nodeIndicesStats, ok, warning);
        }

        if (settings.getAsBoolean("metrics.riemann.field_data.enabled", true)) {
            fieldDataCache(nodeIndicesStats);
        }
        if (settings.getAsBoolean("metrics.riemann.filter_cache.enabled", true)) {
            filterCache(nodeIndicesStats);
        }

        if (settings.getAsBoolean("metrics.riemann.total_thread_count.enabled", true)) {
            long ok = settings.getAsLong("metrics.riemann.total_thread_count.ok", 150L);
            long warning = settings.getAsLong("metrics.riemann.total_thread_count.warning", 200L);
            totalThreadCount(monitorService.jvmService().stats(), ok, warning);
        }

        if (settings.getAsBoolean("metrics.riemann.system_load.enabled", true)) {
            long ok = settings.getAsLong("metrics.riemann.system_load.ok", 2L);
            long warning = settings.getAsLong("metrics.riemann.system_load.warning", 5L);
            systemLoadOne(monitorService.osService().stats(), ok, warning);
        }

        if (settings.getAsBoolean("metrics.riemann.system_load_5m.enabled", true)) {
            long ok = settings.getAsLong("metrics.riemann.system_load_5m.ok", 2L);
            long warning = settings.getAsLong("metrics.riemann.system_load_5m.warning", 5L);
            systemLoadFive(monitorService.osService().stats(), ok, warning);
        }

        if (settings.getAsBoolean("metrics.riemann.system_load_15m.enabled", true)) {
            long ok = settings.getAsLong("metrics.riemann.system_load_15m.ok", 2L);
            long warning = settings.getAsLong("metrics.riemann.system_load_15m.warning", 5L);
            systemLoadFifteen(monitorService.osService().stats(), ok, warning);
        }

        if (settings.getAsBoolean("metrics.riemann.system_memory_usage.enabled", true)) {
            long ok = settings.getAsLong("metrics.riemann.system_memory_usage.ok", 80L);
            long warning = settings.getAsLong("metrics.riemann.system_memory_usage.warning", 90L);
            systemMemory(monitorService.osService().stats(), ok, warning);
        }
        if (settings.getAsBoolean("metrics.riemann.system_cpu.enabled", true)) {
            systemCpu(monitorService.osService().stats());
        }

        if (settings.getAsBoolean("metrics.riemann.disk_usage.enabled", true)) {
            long ok = settings.getAsLong("metrics.riemann.disk_usage.ok", 80L);
            long warning = settings.getAsLong("metrics.riemann.disk_usage.warning", 90L);
            systemFile(monitorService.fsService().stats(), ok, warning);
        }

    }

    private void currentIndexingRate(NodeIndicesStats nodeIndicesStats, long ok, long warning) {
        long indexCount = nodeIndicesStats.getIndexing() == null ? 0 : nodeIndicesStats.getIndexing().getTotal().getIndexCount();
        long delta = deltaMap.get("index_rate");
        long indexingCurrent = indexCount - delta;
        deltaMap.put("index_rate", indexCount);
        riemannClient.event()
                .host(hostDefinition)
                .service("elasticsearch node statistics current indexing rate")
                .description("Elastic Search current indexing rate")
                .tags(tags)
                .attributes(attributes)
                .attribute("component", "node statistics")
                .attribute("measurement", "current_indexing_rate")
                .state(RiemannUtils.getState(indexingCurrent, ok, warning)).metric(indexingCurrent).send();
    }

    private void heapRatio(JvmStats jvmStats, long ok, long warning) {
        long heapUsed = jvmStats.getMem().getHeapUsed().getBytes();
        long heapCommitted = jvmStats.getMem().getHeapCommitted().getBytes();
        long heapRatio = (heapUsed * 100) / heapCommitted;
        riemannClient.event()
                .host(hostDefinition)
                .service("elasticsearch JVM statistics heap usage ratio")
                .description("Elastic Search heap usage ratio")
                .tags(tags)
                .attributes(attributes)
                .attribute("component", "JVM statistics")
                .attribute("measurement", "heap_usage_ratio")
                .state(RiemannUtils.getState(heapRatio, ok, warning)).metric(heapRatio).send();

        riemannClient.event()
                .host(hostDefinition)
                .service("elasticsearch JVM statistics heap used percentage")
                .description("Elastic Search heap used percentage")
                .tags(tags)
                .attributes(attributes)
                .attribute("component", "JVM statistics")
                .attribute("measurement", "heap_used_percentage")
                .state(RiemannUtils.getState(heapRatio, ok, warning)).metric(jvmStats.getMem().getHeapUsedPrecent()).send();
    }

    private void heap(JvmStats jvmStats) {
        double heapUsed = jvmStats.getMem().getHeapUsed().getGbFrac();
        double heapCommitted = jvmStats.getMem().getHeapCommitted().getGbFrac();
        double heapMax = jvmStats.getMem().getHeapMax().getGbFrac();
        riemannClient.event()
                .host(hostDefinition)
                .service("elasticsearch JVM statistics heap used")
                .description("Elastic Search heap used")
                .tags(tags)
                .attributes(attributes)
                .attribute("component", "JVM statistics")
                .attribute("measurement", "heap_used")
                .state("ok")
                .metric(heapUsed).send();
        riemannClient.event()
                .host(hostDefinition)
                .service("elasticsearch JVM statistics heap committed")
                .description("Elastic Search heap committed")
                .tags(tags)
                .attributes(attributes)
                .attribute("component", "JVM statistics")
                .attribute("measurement", "heap_committed")
                .state("ok")
                .metric(heapCommitted).send();
        riemannClient.event()
                .host(hostDefinition)
                .service("elasticsearch JVM statistics heap max")
                .description("Elastic Search heap max")
                .tags(tags)
                .attributes(attributes)
                .attribute("component", "JVM statistics")
                .attribute("measurement", "heap_max")
                .state("ok")
                .metric(heapMax).send();
    }

    private void nonHeap(JvmStats jvmStats) {
        double used = jvmStats.getMem().getNonHeapUsed().getGbFrac();
        double committed = jvmStats.getMem().getNonHeapCommitted().getGbFrac();
        riemannClient.event()
                .host(hostDefinition)
                .service("elasticsearch JVM statistics non-heap used")
                .description("Elastic Search non-heap used")
                .tags(tags)
                .attributes(attributes)
                .attribute("component", "JVM statistics")
                .attribute("measurement", "non_heap_used")
                .state("ok")
                .metric(used).send();
        riemannClient.event()
                .host(hostDefinition)
                .service("elasticsearch JVM statistics non-heap committed")
                .description("Elastic Search non-heap committed")
                .tags(tags)
                .attributes(attributes)
                .attribute("component", "JVM statistics")
                .attribute("measurement", "non_heap_committed")
                .state("ok")
                .metric(committed).send();
    }

    private void currentQueryRate(NodeIndicesStats nodeIndicesStats, long ok, long warning) {
        long queryCount = nodeIndicesStats.getSearch() == null ? 0 : nodeIndicesStats.getSearch().getTotal().getQueryCount();

        long delta = deltaMap.get("query_rate");
        long queryCurrent = queryCount - delta;
        deltaMap.put("query_rate", queryCount);

        riemannClient.event()
                .host(hostDefinition)
                .service("elasticsearch node statistics current query rate")
                .description("Elastic Search current query rate")
                .tags(tags)
                .attributes(attributes)
                .attribute("component", "node statistics")
                .attribute("measurement", "current_query_rate")
                .state(RiemannUtils.getState(queryCurrent, ok, warning)).metric(queryCurrent).send();
    }

    private void queryTime(NodeIndicesStats nodeIndicesStats) {
        long queryTime = nodeIndicesStats.getSearch() == null ? 0 : nodeIndicesStats.getSearch().getTotal().getQueryTimeInMillis();
        long delta = deltaMap.get("query_time");
        long queryTimeCurrent = queryTime - delta;
        deltaMap.put("query_time", queryTime);
        riemannClient.event()
                .host(hostDefinition)
                .service("elasticsearch node statistics current query time")
                .description("Elastic Search current query time (ms)")
                .tags(tags)
                .attributes(attributes)
                .attribute("component", "node statistics")
                .attribute("measurement", "current_query_time_ms")
                .state("ok")
                .metric(queryTimeCurrent).send();
    }

    private void filterCache(NodeIndicesStats nodeIndicesStats) {
        long evictions = nodeIndicesStats.getFilterCache() == null ? 0 : nodeIndicesStats.getFilterCache().getEvictions();
        long size = nodeIndicesStats.getFilterCache() == null ? 0 : nodeIndicesStats.getFilterCache().getMemorySizeInBytes();

        riemannClient.event()
                .host(hostDefinition)
                .service("elasticsearch node statistics filter cache size")
                .description("Elastic Search filter cache size")
                .tags(tags)
                .attributes(attributes)
                .attribute("component", "node statistics")
                .attribute("measurement", "filter cache size")
                .state("ok")
                .metric(size).send();

        riemannClient.event()
                .host(hostDefinition)
                .service("elasticsearch node statistics filter cache evictions")
                .description("Elastic Search filter cache evictions")
                .tags(tags)
                .attributes(attributes)
                .attribute("component", "node statistics")
                .attribute("measurement", "filter cache evictions")
                .state("ok")
                .metric(evictions).send();
    }

    private void fieldDataCache(NodeIndicesStats nodeIndicesStats) {
        long evictions = nodeIndicesStats.getFieldData() == null ? 0 : nodeIndicesStats.getFieldData().getEvictions();
        long size = nodeIndicesStats.getFieldData() == null ? 0 : nodeIndicesStats.getFieldData().getMemorySizeInBytes();

        riemannClient.event()
                .host(hostDefinition)
                .service("elasticsearch node statistics field data cache size")
                .description("Elastic Search field data cache size")
                .tags(tags)
                .attributes(attributes)
                .attribute("component", "node statistics")
                .attribute("measurement", "field data cache size")
                .state("ok")
                .metric(size).send();

        riemannClient.event()
                .host(hostDefinition)
                .service("elasticsearch node statistics field cache evictions")
                .description("Elastic Search field cache evictions")
                .tags(tags)
                .attributes(attributes)
                .attribute("component", "node statistics")
                .attribute("measurement", "field cache evictions")
                .state("ok")
                .metric(evictions).send();
    }

    private void currentFetchRate(NodeIndicesStats nodeIndicesStats, long ok, long warning) {
        long fetchCount = nodeIndicesStats.getSearch() == null ? 0 : nodeIndicesStats.getSearch().getTotal().getFetchCount();
        long delta = deltaMap.get("fetch_rate");
        long fetchCurrent = fetchCount - delta;
        deltaMap.put("fetch_rate", fetchCount);
        riemannClient.event()
                .host(hostDefinition)
                .service("elasticsearch node statistics current fetch rate")
                .description("Elastic Search current fetch rate")
                .tags(tags)
                .attributes(attributes)
                .attribute("component", "node statistics")
                .attribute("measurement", "current_fetch_rate")
                .state(RiemannUtils.getState(fetchCurrent, ok, warning)).metric(fetchCurrent).send();
    }

    private void totalThreadCount(JvmStats jvmStats, long ok, long warning) {
        int threadCount = jvmStats.getThreads().getCount();
        riemannClient.event()
                .host(hostDefinition)
                .service("elasticsearch JVM statistics total thread count")
                .description("Elastic Search total thread count")
                .tags(tags)
                .attributes(attributes)
                .attribute("component", "JVM statistics")
                .attribute("measurement", "total_thread_count")
                .state(RiemannUtils.getState(threadCount, ok, warning)).metric(threadCount).send();
    }

    private void systemLoadOne(OsStats osStats, long ok, long warning) {
        double[] systemLoad = osStats.getLoadAverage();
        riemannClient.event()
                .host(hostDefinition)
                .service("elasticsearch system statistics system load (1m)")
                .description("Elastic Search 1m system load")
                .tags(tags)
                .attributes(attributes)
                .attribute("component", "system statistics")
                .attribute("measurement", "system_load_1m")
                .state(RiemannUtils.getState((long) systemLoad[0], ok, warning)).metric(systemLoad[0]).send();
    }

    private void systemLoadFive(OsStats osStats, long ok, long warning) {
        double[] systemLoad = osStats.getLoadAverage();
        riemannClient.event()
                .host(hostDefinition)
                .service("elasticsearch system statistics system load (5m)")
                .description("Elastic Search 5m system load")
                .tags(tags)
                .attributes(attributes)
                .attribute("component", "system statistics")
                .attribute("measurement", "system_load_5m")
                .state(RiemannUtils.getState((long) systemLoad[1], ok, warning)).metric(systemLoad[1]).send();
    }

    private void systemLoadFifteen(OsStats osStats, long ok, long warning) {
        double[] systemLoad = osStats.getLoadAverage();
        riemannClient.event()
                .host(hostDefinition)
                .service("elasticsearch system statistics system load (15m)")
                .description("Elastic Search 15m system load")
                .tags(tags)
                .attributes(attributes)
                .attribute("component", "system statistics")
                .attribute("measurement", "system_load_15m")
                .state(RiemannUtils.getState((long) systemLoad[2], ok, warning)).metric(systemLoad[2]).send();
    }

    private void systemMemory(OsStats osStats, long ok, long warning) {
        short memoryUsedPercentage = osStats.getMem().getUsedPercent();
        riemannClient.event()
                .host(hostDefinition)
                .service("elasticsearch system statistics system memory usage")
                .description("Elastic Search system memory usage")
                .tags(tags)
                .attributes(attributes)
                .attribute("component", "system statistics")
                .attribute("measurement", "system_memory_usage")
                .state(RiemannUtils.getState(memoryUsedPercentage, ok, warning)).metric(memoryUsedPercentage).send();

    }

    private void systemCpu(OsStats osStats) {
        short user = osStats.getCpu().getUser();
        short sys = osStats.getCpu().getSys();
        short idle = osStats.getCpu().getIdle();
        short stolen = osStats.getCpu().getStolen();
        riemannClient.event()
                .host(hostDefinition)
                .service("elasticsearch system statistics user CPU")
                .description("Elastic Search system user CPU")
                .tags(tags)
                .attributes(attributes)
                .attribute("component", "system statistics")
                .attribute("measurement", "user_cpu")
                .state("ok")
                .metric(user).send();

        riemannClient.event()
                .host(hostDefinition)
                .service("elasticsearch system statistics system CPU")
                .description("Elastic Search system CPU")
                .tags(tags)
                .attributes(attributes)
                .attribute("component", "system statistics")
                .attribute("measurement", "system_cpu")
                .state("ok")
                .metric(sys).send();

        riemannClient.event()
                .host(hostDefinition)
                .service("elasticsearch system statistics idle CPU")
                .description("Elastic Search system idle CPU")
                .tags(tags)
                .attributes(attributes)
                .attribute("component", "system statistics")
                .attribute("measurement", "idle_cpu")
                .state("ok")
                .metric(idle).send();

        riemannClient.event()
                .host(hostDefinition)
                .service("elasticsearch system statistics stolen CPU")
                .description("Elastic Search system stolen CPU")
                .tags(tags)
                .attributes(attributes)
                .attribute("component", "system statistics")
                .attribute("measurement", "stolen_cpu")
                .state("ok")
                .metric(stolen).send();

    }

    private void systemFile(FsStats fsStats, long ok, long warning) {
        for (FsStats.Info info : fsStats) {
            long free = info.getFree().getBytes();
            long total = info.getTotal().getBytes();
            long usageRatio = ((total - free) * 100) / total;
            riemannClient.event()
                    .host(hostDefinition)
                    .service("elasticsearch system statistics disk usage")
                    .description("Elastic Search system disk usage")
                    .tags(tags)
                    .attributes(attributes)
                    .attribute("component", "system statistics")
                    .attribute("measurement", "system_disk_usage")
                    .state(RiemannUtils.getState(usageRatio, ok, warning)).metric(usageRatio).send();
        }
    }
}
