package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/kshvakov/clickhouse"
	"github.com/timescale/tsbs/load"
	"github.com/lib/pq"
)

type syncCSI struct {
	m     map[string]int64
	mutex *sync.RWMutex
}

func newSyncCSI() *syncCSI {
	return &syncCSI{
		m:     make(map[string]int64),
		mutex: &sync.RWMutex{},
	}
}

// globalSyncCSI is used when data is not hashed by some function to a worker consistently so
// therefore all workers need to know about the same map from hostname -> tags_id
var globalSyncCSI = newSyncCSI()

// subsystemTagsToJSON converts equations as
// a=b
// c=d
// into JSON STRING '{"a": "b", "c": "d"}'
func subsystemTagsToJSON(tags []string) string {
	json := "{"
	for i, t := range tags {
		args := strings.Split(t, "=")
		if i > 0 {
			json += ","
		}
		json += fmt.Sprintf("\"%s\": \"%s\"", args[0], args[1])
	}
	json += "}"
	return json
}

// insertTags fills tags table with values
func insertTags(db *sqlx.DB, tagRows [][]string, returnResults bool) map[string]int64 {
	tagCols := tableCols["tags"]
	cols := tagCols

	// reflect tags table structure which is
	// CREATE TABLE tags(
	//	 created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	//	 id,
	//   %s
	// ) engine=MergeTree(created_at, (%s), 8192)

	// build insert-multiple-rows INSERT statement like:
	// INSERT INTO tags (
	//   ... list of column names ...
	// ) VALUES
	// ( ... row 1 values ... ),
	// ( ... row 2 values ... ),
	// ...
	// ( ... row N values ... ),

	values := make([]string, 0)
	commonTagsLen := len(tagCols)
	id := 0
	for _, val := range tagRows {
		id++
		values = append(values, fmt.Sprintf("(" + string(id) + ", '%s')", strings.Join(val[:commonTagsLen], "','")))
	}

	tx := db.MustBegin()
	defer tx.Commit()

	sql := fmt.Sprintf(`
		INSERT INTO tags(
			%s
		) VALUES 
			%s
		`,
		strings.Join(cols, ","),
		strings.Join(values, ","))
	if debug > 0 {
		fmt.Printf(sql)
	}
	_, err := tx.Query(sql)
	if err != nil {
		panic(err)
	}

	// Results will be used to make a Golang index for faster inserts
	//if returnResults {
	//	resCols, _ := res.Columns()
	//	resVals := make([]interface{}, len(resCols))
	//	resValsPtrs := make([]interface{}, len(resCols))
	//	for i := range resVals {
	//		resValsPtrs[i] = &resVals[i]
	//	}
	//	ret := make(map[string]int64)
	//	for res.Next() {
	//		err = res.Scan(resValsPtrs...)
	//		if err != nil {
	//			panic(err)
	//		}
	//		ret[fmt.Sprintf("%v", resVals[1])] = resVals[0].(int64)
	//	}
	//	res.Close()
	//	return ret
	//}
	return nil
}

func (p *processor) processCSI(tableName string, rows []*insertData) uint64 {
	tagRows := make([][]string, 0, len(rows))
	dataRows := make([][]interface{}, 0, len(rows))
	ret := uint64(0)
	commonTagsLen := len(tableCols["tags"])

	colLen := len(tableCols[tableName]) + 2
	if inTableTag {
		colLen++
	}

	for _, data := range rows {
		// Split the tags into individual common tags and
		// an extra bit leftover for non-common tags that need to be added separately.
		// For each of the common tags, remove everything after = in the form <label>=<val>
		// since we won't need it.
		// tags line ex.:
		// hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,rack=67,os=Ubuntu16.10,arch=x86,team=NYC,service=7,service_version=0,service_environment=production
		tags := strings.SplitN(data.tags, ",", commonTagsLen+1)
		for i := 0; i < commonTagsLen; i++ {
			// extract value of the tag
			tags[i] = strings.Split(tags[i], "=")[1]
		}
		var json interface{} = nil
		if len(tags) > commonTagsLen {
			json = subsystemTagsToJSON(strings.Split(tags[commonTagsLen], ","))
		}

		// fields line ex.:
		// 1451606400000000000,58,2,24,61,22,63,6,44,80,38
		metrics := strings.Split(data.fields, ",")
		ret += uint64(len(metrics) - 1) // 1-st field is timestamp

		// convert time from 1451606400000000000 to '2006-01-02 15:04:05.999999 -0700'
		timeInt, err := strconv.ParseInt(metrics[0], 10, 64)
		if err != nil {
			panic(err)
		}
		ts := time.Unix(0, timeInt).Format("2006-01-02 15:04:05.999999 -0700")

		// use nil at 2nd position as placeholder for tagKey
		r := make([]interface{}, 0, colLen)
		r = append(r, ts, nil, json)
		if inTableTag {
			r = append(r, tags[0]) // tags[0] = hostname
		}
		for _, v := range metrics[1:] {
			r = append(r, v)
		}

		dataRows = append(dataRows, r)
		tagRows = append(tagRows, tags)
	}

	// Check if any of these tags has yet to be inserted
	newTags := make([][]string, 0, len(rows))
	p.csi.mutex.RLock()
	for _, cols := range tagRows {
		if _, ok := p.csi.m[cols[0]]; !ok {
			newTags = append(newTags, cols)
		}
	}
	p.csi.mutex.RUnlock()

	if len(newTags) > 0 {
		p.csi.mutex.Lock()
		res := insertTags(p.db, newTags, true)
		for k, v := range res {
			p.csi.m[k] = v
		}
		p.csi.mutex.Unlock()
	}

	p.csi.mutex.RLock()
	for i := range dataRows {
		tagKey := tagRows[i][0]
		dataRows[i][1] = p.csi.m[tagKey]
	}
	p.csi.mutex.RUnlock()
	tx := p.db.MustBegin()

	cols := make([]string, 0, colLen)
	cols = append(cols, "time", "tags_id", "additional_tags")
	if inTableTag {
		cols = append(cols, tableCols["tags"][0])
	}
	cols = append(cols, tableCols[tableName]...)
	stmt, err := tx.Prepare(pq.CopyIn(tableName, cols...))
	for _, r := range dataRows {
		stmt.Exec(r...)
	}

	_, err = stmt.Exec()
	if err != nil {
		panic(err)
	}

	err = stmt.Close()
	if err != nil {
		panic(err)
	}

	err = tx.Commit()
	if err != nil {
		panic(err)
	}

	return ret
}

// load.Processor interface implementation
type processor struct {
	db  *sqlx.DB
	csi *syncCSI
}

// load.Processor interface implementation
func (p *processor) Init(workerNum int, doLoad bool) {
	if doLoad {
		p.db = sqlx.MustConnect(dbType, getConnectString())
		if hashWorkers {
			p.csi = newSyncCSI()
		} else {
			p.csi = globalSyncCSI
		}
	}
}

// load.ProcessorCloser interface implementation
func (p *processor) Close(doLoad bool) {
	if doLoad {
		p.db.Close()
	}
}

// load.Processor interface implementation
func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	batches := b.(*tableArr)
	rowCnt := 0
	metricCnt := uint64(0)
	for tableName, rows := range batches.m {
		rowCnt += len(rows)
		if doLoad {
			start := time.Now()
			metricCnt += p.processCSI(tableName, rows)

			if logBatches {
				now := time.Now()
				took := now.Sub(start)
				batchSize := len(rows)
				fmt.Printf("BATCH: batchsize %d row rate %f/sec (took %v)\n", batchSize, float64(batchSize)/float64(took.Seconds()), took)
			}
		}
	}
	batches.m = map[string][]*insertData{}
	batches.cnt = 0

	return metricCnt, uint64(rowCnt)
}
