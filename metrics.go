package gokv

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	writesTotal        prometheus.Counter
	readsTotal         prometheus.Counter
	readLatency        prometheus.Histogram
	writeLatency       prometheus.Histogram
	memtableSize       prometheus.Gauge
	sstableCount       prometheus.Gauge
	compactionDuration prometheus.Histogram
	bloomHits          prometheus.Counter
	bloomMisses        prometheus.Counter
}

func newMetrics(reg prometheus.Registerer) *metrics {
	factory := promauto.With(reg)

	return &metrics{
		writesTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "gokv_writes_total",
			Help: "Total number of write operations.",
		}),
		readsTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "gokv_reads_total",
			Help: "Total number of read operations.",
		}),
		readLatency: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "gokv_read_latency_seconds",
			Help:    "Latency of read operations in seconds.",
			Buckets: prometheus.DefBuckets,
		}),
		writeLatency: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "gokv_write_latency_seconds",
			Help:    "Latency of write operations in seconds.",
			Buckets: prometheus.DefBuckets,
		}),
		memtableSize: factory.NewGauge(prometheus.GaugeOpts{
			Name: "gokv_memtable_size_bytes",
			Help: "Current memtable size in bytes.",
		}),
		sstableCount: factory.NewGauge(prometheus.GaugeOpts{
			Name: "gokv_sstable_count",
			Help: "Current number of SSTable files.",
		}),
		compactionDuration: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "gokv_compaction_duration_seconds",
			Help:    "Duration of compaction operations in seconds.",
			Buckets: prometheus.DefBuckets,
		}),
		bloomHits: factory.NewCounter(prometheus.CounterOpts{
			Name: "gokv_bloom_filter_hits_total",
			Help: "Number of times bloom filter confirmed key might exist.",
		}),
		bloomMisses: factory.NewCounter(prometheus.CounterOpts{
			Name: "gokv_bloom_filter_misses_total",
			Help: "Number of times bloom filter ruled out a key.",
		}),
	}
}