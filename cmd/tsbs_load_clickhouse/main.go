// tsbs_load_clickhouse loads a ClickHouse instance with data from stdin.
//
// If the database exists beforehand, it will be *DROPPED*.
package main

import (
	"bufio"
	"flag"
	"log"
	"time"

	"github.com/timescale/tsbs/load"
)

const (
	dbType       = "clickhouse"
	timeValueIdx = "TIME-VALUE"
	valueTimeIdx = "VALUE-TIME"
)

// Program option vars:
var (
	host            string
	user            string
	password        string

	logBatches    bool
	inTableTag    bool
	hashWorkers   bool

	numberPartitions int
	chunkTime        time.Duration

	timeIndex          bool
	timePartitionIndex bool
	partitionIndex     bool
	fieldIndex         string
	fieldIndexCount    int

	profileFile        string

	debug int
)

// String values of tags and fields to insert - string representation
type insertData struct {
	tags   string	// hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,rack=67,os=Ubuntu16.10,arch=x86,team=NYC,service=7,service_version=0,service_environment=production
	fields string	// 1451606400000000000,58,2,24,61,22,63,6,44,80,38
}

// Global vars
var (
	loader    *load.BenchmarkRunner
	tableCols map[string][]string
)

// allows for testing
var fatal = log.Fatalf

// Parse args:
func init() {
	loader = load.GetBenchmarkRunner()

	flag.StringVar(&host, "host", "localhost", "Hostname of ClickHouse instance")
	flag.StringVar(&user, "user", "default", "User to connect to ClickHouse as")
	flag.StringVar(&password, "password", "", "Password to connect to ClickHouse")

	flag.BoolVar(&logBatches, "log-batches", false, "Whether to time individual batches.")

	// TODO - This flag could potentially be done as a string/enum with other options besides no-hash, round-robin, etc
	flag.BoolVar(&hashWorkers, "hash-workers", false, "Whether to consistently hash insert data to the same workers (i.e., the data for a particular host always goes to the same worker)")

	flag.IntVar(&numberPartitions, "partitions", 1, "Number of partitions")
	flag.DurationVar(&chunkTime, "chunk-time", 12*time.Hour, "Duration that each chunk should represent, e.g., 12h")

	flag.BoolVar(&timeIndex, "time-index", true, "Whether to build an index on the time dimension")
	flag.BoolVar(&timePartitionIndex, "time-partition-index", false, "Whether to build an index on the time dimension, compounded with partition")
	flag.BoolVar(&partitionIndex, "partition-index", true, "Whether to build an index on the partition key")
	flag.StringVar(&fieldIndex, "field-index", valueTimeIdx, "index types for tags (comma deliminated)")
	flag.IntVar(&fieldIndexCount, "field-index-count", 0, "Number of indexed fields (-1 for all)")

	flag.StringVar(&profileFile, "write-profile", "", "File to output CPU/memory profile to")

	flag.IntVar(&debug, "debug", 0, "Debug printing (choices: 0, 1, 2). (default 0)")

	flag.Parse()
	tableCols = make(map[string][]string)
}

// loader.Benchmark interface implementation
type benchmark struct{}

// loader.Benchmark interface implementation
func (b *benchmark) GetPointDecoder(br *bufio.Reader) load.PointDecoder {
	return &decoder{
		scanner: bufio.NewScanner(br),
	}
}

// loader.Benchmark interface implementation
func (b *benchmark) GetBatchFactory() load.BatchFactory {
	return &factory{}
}

// loader.Benchmark interface implementation
func (b *benchmark) GetPointIndexer(maxPartitions uint) load.PointIndexer {
	if hashWorkers {
		return &hostnameIndexer{
			partitions: maxPartitions,
		}
	}
	return &load.ConstantIndexer{}
}

// loader.Benchmark interface implementation
func (b *benchmark) GetProcessor() load.Processor {
	return &processor{}
}

// loader.Benchmark interface implementation
func (b *benchmark) GetDBCreator() load.DBCreator {
	return &dbCreator{}
}

func main() {
	// If specified, generate a performance profile
	if len(profileFile) > 0 {
		go profileCPUAndMem(profileFile)
	}

	if hashWorkers {
		loader.RunBenchmark(&benchmark{}, load.WorkerPerQueue)
	} else {
		loader.RunBenchmark(&benchmark{}, load.SingleQueue)
	}
}
