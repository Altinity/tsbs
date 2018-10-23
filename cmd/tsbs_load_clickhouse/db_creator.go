package main

import (
	"bufio"
	"fmt"
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
	db := sqlx.MustConnect(dbType, getConnectString(false))
	defer db.Close()

	sql := "SELECT name, engine FROM system.databases WHERE name = '" + dbName + "'"
	if debug > 0 {
		fmt.Printf(sql)
	}
	var rows []struct {
		Name    string `db:"name"`
		Engine  string `db:"engine"`
	}

	err := db.Select(&rows, sql)
	if err != nil {
		panic(err)
	}
	for _, row := range rows {
		if row.Name == dbName {
			return true
		}
	}

	return false
}

// loader.DBCreator interface implementation
func (d *dbCreator) RemoveOldDB(dbName string) error {
	db := sqlx.MustConnect(dbType, getConnectString(false))
	defer db.Close()

	sql := "DROP DATABASE IF EXISTS " + dbName
	if _, err := db.Exec(sql); err != nil {
		panic(err)
	}
	return nil
}

// loader.DBCreator interface implementation
func (d *dbCreator) CreateDB(dbName string) error {
	// Connecto to ClickHouse in general and CREATE DATABASE
	db := sqlx.MustConnect(dbType, getConnectString(false))
	sql := "CREATE DATABASE " + dbName
	_, err := db.Exec(sql)
	if err != nil {
		panic(err)
	}
	db.Close()
	db = nil

	// Connect to specified database within ClickHouse
	db = sqlx.MustConnect(dbType, getConnectString(true))
	defer db.Close()

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
	createTagsTable(db, parts[1:])
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
		pseudoCols = append(pseudoCols, parts[1:]...)
		for _, field := range pseudoCols {
			if len(field) == 0 {
				continue
			}
			fieldDef = append(fieldDef, fmt.Sprintf("%s Float64", field))
		}

		sql := fmt.Sprintf(`
			CREATE TABLE %s (
				created_date    Date     DEFAULT today(),
				created_at      DateTime DEFAULT now(),
				time			String,
				tags_id         UInt32,
				%s,
				additional_tags String   DEFAULT ''
			) ENGINE = MergeTree(created_date, (tags_id, created_at), 8192)`,
			tableName,
			strings.Join(fieldDef, ","))
		if debug > 0 {
			fmt.Printf(sql)
		}
		_, err := db.Exec(sql)
		if err != nil {
			panic(err)
		}
	}

	return nil
}

// createTagsTable builds CREATE TABLE SQL statement and runs it
func createTagsTable(db *sqlx.DB, tags []string) {
	// prepare COLUMNs specification for CREATE TABLE statement
	// all columns would be of type String
	cols := strings.Join(tags, " String,\n ")
	cols += " String\n"

	// index would be on all fields
	//index := strings.Join(tags, ","	)
	index := "id"

	sql := fmt.Sprintf(`
		CREATE TABLE tags(
			created_date Date     DEFAULT today(),
			created_at   DateTime DEFAULT now(),
			id           UInt32,
			%s
		) ENGINE=MergeTree(created_date, (%s), 8192)
		`,
		cols,
		index)
	if debug > 0 {
		fmt.Printf(sql)
	}
	_, err := db.Exec(sql)
	if err != nil {
		panic(err)
	}
}

// getConnectString() builds connect string to ClickHouse
// db - whether database specification should be added to the connection string
func getConnectString(db bool) string {
	// connectString: tcp://127.0.0.1:9000?debug=true
	// ClickHouse ex.:
	// tcp://host1:9000?username=user&password=qwerty&database=clicks&read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000
	if db {
		return fmt.Sprintf("tcp://%s:9000?username=%s&password=%s&database=%s", host, user, password, loader.DatabaseName())
	} else {
		return fmt.Sprintf("tcp://%s:9000?username=%s&password=%s", host, user, password)
	}
}
