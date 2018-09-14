package main

import (
	"fmt"
	"testing"
)

func TestGetConnectString(t *testing.T) {
	wantHost := "localhost"
	wantDB := "benchmark"
	wantUser := "default"
	want := fmt.Sprintf("host=%s dbname=%s user=%s", wantHost, wantDB, wantUser)
	cases := []struct {
		desc      string
		chConnect string
	}{
		{
			desc:      "replace host, dbname, user",
			chConnect: "host=foo dbname=bar user=joe",
		},
		{
			desc:      "replace just some",
			chConnect: "host=foo dbname=bar",
		},
		{
			desc:      "no replace",
			chConnect: "",
		},
	}

	for _, c := range cases {
		host = wantHost
		user = wantUser
		clickhouseConnect = c.chConnect
		cstr := getConnectString()
		if cstr != want {
			t.Errorf("%s: incorrect connect string: got %s want %s", c.desc, cstr, want)
		}
	}
}
