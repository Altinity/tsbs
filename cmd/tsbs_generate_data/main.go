// tsbs_generate_data generates time series data from pre-specified use cases.
//
// Supported formats:
// Cassandra CSV format
// InfluxDB bulk load format
// MongoDB BSON format
// TimescaleDB pseudo-CSV format

// Supported use cases:
// devops: scale-var is the number of hosts to simulate, with log messages
//         every log-interval seconds.
// cpu-only: same as `devops` but only generate metrics for CPU
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"sort"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
	"io"
	"math/rand"
)

const (
	// Output data format choices (alphabetical order)
	formatCassandra   = "cassandra"
	formatClickhouse  = "clickhouse"
	formatInflux      = "influx"
	formatMongo       = "mongo"
	formatTimescaleDB = "timescaledb"

	// Use case choices (make sure to update TestGetConfig if adding a new one)
	useCaseCPUOnly   = "cpu-only"
	useCaseCPUSingle = "cpu-single"
	useCaseDevops    = "devops"

	defaultWriteSize  = 4 << 20 // 4 MB
)

// semi-constants
var (
	formatChoices = []string{
		formatCassandra,
		formatClickhouse,
		formatInflux,
		formatMongo,
		formatTimescaleDB,
	}
	// allows for testing
	fatal = log.Fatalf
)

// Program option vars:
var (
	format      string
	useCase     string
	profileFile string

	initialScale uint64
	scale        uint64
	seed         int64
	debug        int

	timestampStart time.Time
	timestampEnd   time.Time

	interleavedGenerationGroupID uint
	interleavedGenerationGroups  uint

	logInterval time.Duration
	limit       uint64
	fileName    string
)

// Parse args:
func init() {
	var timestampStartStr string
	var timestampEndStr string
	flag.StringVar(&format, "format", "", fmt.Sprintf("Format to emit. (choices: %s)", strings.Join(formatChoices, ", ")))

	flag.StringVar(&useCase, "use-case", "", "Use case to model. (choices: devops, cpu-only)")

	flag.Uint64Var(&initialScale, "initial-scale", 0, "Initial scaling specific to the use case (e.g., devices in 'devops'). 0 means to use -scale value")
	flag.Uint64Var(&scale, "scale", 1, "Scaling variable specific to the use case (e.g., devices in 'devops').")

	flag.StringVar(&timestampStartStr, "timestamp-start", "2016-01-01T00:00:00Z", "Beginning timestamp (RFC3339).")
	flag.StringVar(&timestampEndStr, "timestamp-end", "2016-01-02T06:00:00Z", "Ending timestamp (RFC3339).")

	flag.Int64Var(&seed, "seed", 0, "PRNG seed (0 uses the current timestamp). (default 0)")
	flag.IntVar(&debug, "debug", 0, "Debug printing (choices: 0, 1, 2). (default 0)")

	flag.UintVar(&interleavedGenerationGroupID, "interleaved-generation-group-id", 0, "Group (0-indexed) to perform round-robin serialization within. Use this to scale up data generation to multiple processes.")
	flag.UintVar(&interleavedGenerationGroups, "interleaved-generation-groups", 1, "The number of round-robin serialization groups. Use this to scale up data generation to multiple processes.")
	flag.StringVar(&profileFile, "profile-file", "", "File to which to write go profiling data")

	flag.DurationVar(&logInterval, "log-interval", 10*time.Second, "Duration between host data points")
	flag.Uint64Var(&limit, "limit", 0, "Limit the number of data point to generate, 0 = no limit")
	flag.StringVar(&fileName, "file", "", "File name to write generated data to")

	flag.Parse()

	if !(interleavedGenerationGroupID < interleavedGenerationGroups) {
		log.Fatal("incorrect interleaved groups configuration")
	}

	if initialScale == 0 {
		initialScale = scale
	}

	// the default seed is the current timestamp:
	if seed == 0 {
		seed = int64(time.Now().Nanosecond())
	}
	fmt.Fprintf(os.Stderr, "using random seed %d\n", seed)

	// Parse timestamps:
	var err error
	timestampStart, err = time.Parse(time.RFC3339, timestampStartStr)
	if err != nil {
		log.Fatal(err)
	}
	timestampStart = timestampStart.UTC()
	timestampEnd, err = time.Parse(time.RFC3339, timestampEndStr)
	if err != nil {
		log.Fatal(err)
	}
	timestampEnd = timestampEnd.UTC()
}

func main() {
	if !validateFormat(format) {
		log.Fatal("invalid format specifier")
	}

	if len(profileFile) > 0 {
		defer startMemoryProfile(profileFile)()
	}

	rand.Seed(seed)

	// Prepare output file/STDOUT
	var out  *bufio.Writer
	var file io.Writer
	if len(fileName) > 0 {
		// Write output to file
		file, err := os.Create(fileName)
		if err != nil {
			panic("cannot open file " + fileName)
		}
		defer file.Close()
	} else {
		// Write output to STDOUT
		file = os.Stdout
	}
	// Set up output buffering:
	out = bufio.NewWriterSize(file, defaultWriteSize)
	defer out.Flush()

	cfg := getConfig(useCase)
	sim := cfg.NewSimulator(logInterval, limit)
	serializer := getSerializer(sim, format, out)

	currentInterleavedGroup := uint(0)
	point := serialize.NewPoint()
	for !sim.Finished() {
		write := sim.Next(point)
		if !write {
			point.Reset()
			continue
		}

		// in the default case this is always true
		if currentInterleavedGroup == interleavedGenerationGroupID {
			err := serializer.Serialize(point, out)
			if err != nil {
				log.Fatal(err)
			}
		}
		point.Reset()

		currentInterleavedGroup++
		if currentInterleavedGroup == interleavedGenerationGroups {
			currentInterleavedGroup = 0
		}
	}

	err := out.Flush()
	if err != nil {
		log.Fatal(err.Error())
	}
}

func validateFormat(format string) bool {
	for _, s := range formatChoices {
		if s == format {
			return true
		}
	}
	return false
}

func getConfig(useCase string) common.SimulatorConfig {
	switch useCase {
	case useCaseDevops:
		return &devops.DevopsSimulatorConfig{
			Start: timestampStart,
			End:   timestampEnd,

			InitHostCount:   initialScale,
			HostCount:       scale,
			HostConstructor: devops.NewHost,
		}
	case useCaseCPUOnly:
		return &devops.CPUOnlySimulatorConfig{
			Start: timestampStart,
			End:   timestampEnd,

			InitHostCount:   initialScale,
			HostCount:       scale,
			HostConstructor: devops.NewHostCPUOnly,
		}
	case useCaseCPUSingle:
		return &devops.CPUOnlySimulatorConfig{
			Start: timestampStart,
			End:   timestampEnd,

			InitHostCount:   initialScale,
			HostCount:       scale,
			HostConstructor: devops.NewHostCPUSingle,
		}
	default:
		fatal("unknown use case: '%s'", useCase)
		return nil
	}
}

func getSerializer(sim common.Simulator, format string, out *bufio.Writer) serialize.PointSerializer {
	switch format {
	case formatCassandra:
		return &serialize.CassandraSerializer{}

	case formatInflux:
		return &serialize.InfluxSerializer{}

	case formatMongo:
		return &serialize.MongoSerializer{}

	case formatClickhouse:
		fallthrough

	case formatTimescaleDB:
		out.WriteString("tags")
		for _, key := range devops.MachineTagKeys {
			out.WriteString(",")
			out.Write(key)
		}
		out.WriteString("\n")
		// sort the keys so the header is deterministic
		keys := make([]string, 0)
		fields := sim.Fields()
		for k := range fields {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, measurementName := range keys {
			out.WriteString(measurementName)
			for _, field := range fields[measurementName] {
				out.WriteString(",")
				out.Write(field)

			}
			out.WriteString("\n")
		}
		out.WriteString("\n")

		return &serialize.TimescaleDBSerializer{}
	}

	fatal("unknown format: '%s'", format)
	return nil
}

// startMemoryProfile sets up memory profiling to be written to profileFile. It
// returns a function to cleanup/write that should be deferred by the caller
func startMemoryProfile(profileFile string) func() {
	f, err := os.Create(profileFile)
	if err != nil {
		log.Fatal("could not create memory profile: ", err)
	}

	stop := func() {
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		f.Close()
	}

	// Catches ctrl+c signals
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		<-c

		fmt.Fprintln(os.Stderr, "\ncaught interrupt, stopping profile")
		stop()

		os.Exit(0)
	}()

	return stop
}
