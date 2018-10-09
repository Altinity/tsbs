package main

import (
	"bufio"
	"fmt"
	"regexp"
	"strings"

	"github.com/jmoiron/sqlx"
)

// loader.DBCreator interface implementation
type dbCreator struct {
	tags    string
	cols    []string
	connStr string
}

// loader.DBCreator interface implementation
func (d *dbCreator) Init() {
	br := loader.GetBufferedReader()
	d.readDataHeader(br)

	// Needed to connect to user's database in order to drop/create database
	re := regexp.MustCompile(`(dbname)=\S*\b`)
	d.connStr = re.ReplaceAllString(getConnectString(), "")
}

// readDataHeader fills dbCreator struct with data structure (tables description)
// specified at the beginning of the data file
func (d *dbCreator) readDataHeader(br *bufio.Reader) {
	// First N lines are header, with the first line containing: "tags:comma-separated list of tags"
	// the second through N-1 line containing: "table name: comma-separated list of column names"
	// and last line being blank to separate from the data
	//
	// Ex.:
	//tags,hostname,region,datacenter,rack,os,arch,team,service,service_version,service_environment
	//cpu,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice
	//disk,total,free,used,used_percent,inodes_total,inodes_free,inodes_used
	//nginx,accepts,active,handled,reading,requests,waiting,writing
	//

	i := 0
	for {
		var err  error
		var line string

		if i == 0 {
			// read first line - list of tags
			d.tags, err = br.ReadString('\n')
			if err != nil {
				fatal("input has wrong header format: %v", err)
			}
			d.tags = strings.TrimSpace(d.tags)
		} else {
			// read the second and further lines - metrics descriptions
			line, err = br.ReadString('\n')
			if err != nil {
				fatal("input has wrong header format: %v", err)
			}
			line = strings.TrimSpace(line)
			if len(line) == 0 {
				// empty line - end of header
				break
			}
			// append new table/columns set to the list of tables/columns set
			d.cols = append(d.cols, line)
		}
		i++
	}
}

// loader.DBCreator interface implementation
func (d *dbCreator) DBExists(dbName string) bool {
	db := sqlx.MustConnect(dbType, d.connStr)
	defer db.Close()

	sql := "SELECT 1 FROM system.tables WHERE name = $1"
	if debug > 0 {
		fmt.Printf(sql)
	}
	r, _ := db.Queryx(sql, dbName)
	defer r.Close()

	return r.Next()
}

// loader.DBCreator interface implementation
func (d *dbCreator) RemoveOldDB(dbName string) error {
	db := sqlx.MustConnect(dbType, d.connStr)
	defer db.Close()

	sql := "DROP DATABASE IF EXISTS " + dbName
	if debug > 0 {
		fmt.Printf(sql)
	}
	db.MustExec(sql)

	return nil
}

// loader.DBCreator interface implementation
func (d *dbCreator) CreateDB(dbName string) error {
	db := sqlx.MustConnect(dbType, d.connStr)
	sql := "CREATE DATABASE " + dbName
	if debug > 0 {
		fmt.Printf(sql)
	}
	db.MustExec(sql)
	db.Close()

	dbBench := sqlx.MustConnect(dbType, getConnectString())
	defer dbBench.Close()

	// d.tags content:
	//tags,hostname,region,datacenter,rack,os,arch,team,service,service_version,service_environment
	//
	// Parts would contain
	// 0: tags - reserved word - tags mark
	// 1:
	// N: actual tags
	// so we'll use tags[1:] for tags specification
	parts := strings.Split(strings.TrimSpace(d.tags), ",")
	if parts[0] != "tags" {
		return fmt.Errorf("input header in wrong format. got '%s', expected 'tags'", parts[0])
	}
	createTagsTable(dbBench, parts[1:])
	tableCols["tags"] = parts[1:]

	// d.cols content are lines (metrics descriptions) as:
	//cpu,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice
	//disk,total,free,used,used_percent,inodes_total,inodes_free,inodes_used
	//nginx,accepts,active,handled,reading,requests,waiting,writing
	for _, cols := range d.cols {
		// cols content:
		//cpu,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice
		// Parts would contain
		// 0: table name
		// 1:
		// N: table columns
		parts = strings.Split(strings.TrimSpace(cols), ",")

		// Ex.: cpu OR disk OR nginx
		tableName := parts[0]
		tableCols[tableName] = parts[1:]

		partitioningField := tableCols["tags"][0] // would be 'hostname'

		pseudoCols := []string{}
		if inTableTag {
			pseudoCols = append(pseudoCols, partitioningField)
		}

		fieldDef := []string{}
		indexes := []string{}
		pseudoCols = append(pseudoCols, parts[1:]...)
		extraCols := 0 // set to 1 when hostname is kept in-table
		for idx, field := range pseudoCols {
			if len(field) == 0 {
				continue
			}
			fieldType := "Float64"
			idxType := fieldIndex
			if inTableTag && idx == 0 {
				fieldType = "String"
				idxType = ""
				extraCols = 1
			}

			fieldDef = append(fieldDef, fmt.Sprintf("%s %s", field, fieldType))
			if fieldIndexCount == -1 || idx < (fieldIndexCount + extraCols) {
				indexes = append(indexes, d.getCreateIndexOnFieldCmds(tableName, field, idxType)...)
			}
		}
		sql := fmt.Sprintf(`
			CREATE TABLE %s (
				time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
				tags_id UInt32,
				%s,
				additional_tags String DEFAULT NULL
			) engine=MergeTree(time, time, 8192)`,
			tableName,
			strings.Join(fieldDef, ","))
		if debug > 0 {
			fmt.Printf(sql)
		}
		dbBench.MustExec(sql)

		//if partitionIndex {
		//	dbBench.MustExec(fmt.Sprintf("CREATE INDEX ON %s(tags_id, \"time\" DESC)", tableName))
		//}

		// Only allow one or the other, it's probably never right to have both.
		// Experimentation suggests (so far) that for 100k devices it is better to
		// use --time-partition-index for reduced index lock contention.
		//if timePartitionIndex {
		//	dbBench.MustExec(fmt.Sprintf("CREATE INDEX ON %s(\"time\" DESC, tags_id)", tableName))
		//} else if timeIndex {
		//	dbBench.MustExec(fmt.Sprintf("CREATE INDEX ON %s(\"time\" DESC)", tableName))
		//}
		//
		//for _, idxDef := range indexes {
		//	dbBench.MustExec(idxDef)
		//}

		//if useHypertable {
		//	dbBench.MustExec("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
		//	dbBench.MustExec(
		//		fmt.Sprintf("SELECT create_hypertable('%s'::regclass, 'time'::name, partitioning_column => '%s'::name, number_partitions => %v::smallint, chunk_time_interval => %d, create_default_indexes=>FALSE)",
		//			tableName, "tags_id", numberPartitions, chunkTime.Nanoseconds()/1000))
		//}
	}

	return nil
}

func (d *dbCreator) getCreateIndexOnFieldCmds(hypertable, field, idxType string) []string {
	//ret := []string{}
	//for _, idx := range strings.Split(idxType, ",") {
	//	if idx == "" {
	//		continue
	//	}
	//
	//	indexDef := ""
	//	if idx == timeValueIdx {
	//		indexDef = fmt.Sprintf("(time DESC, %s)", field)
	//	} else if idx == valueTimeIdx {
	//		indexDef = fmt.Sprintf("(%s, time DESC)", field)
	//	} else {
	//		fatal("Unknown index type %v", idx)
	//	}
	//
	//	ret = append(ret, fmt.Sprintf("CREATE INDEX ON %s %s", table, indexDef))
	//}
	//return ret
	return nil
}


// createTagsTable builds CREATE TABLE SQL statement and runs it
func createTagsTable(db *sqlx.DB, tags []string) {

	// prepare COLUMNs specification for CREATE TABLE statement
	// all columns would be of type String
	cols := strings.Join(tags, " String, ")
	cols += " String"

	// index would be on all fields
	index := strings.Join(tags, ","	)

	sql := fmt.Sprintf(`
		CREATE TABLE tags(
			created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			id,
			%s
		) engine=MergeTree(created_at, (%s), 8192)
		`,
		cols,
		index)
	if debug > 0 {
		fmt.Printf(sql)
	}
	db.MustExec(sql)

	//db.MustExec(fmt.Sprintf("CREATE UNIQUE INDEX uniq1 ON tags(%s)", strings.Join(tags, ",")))
	//db.MustExec(fmt.Sprintf("CREATE INDEX ON tags(%s)", tags[0]))
}
