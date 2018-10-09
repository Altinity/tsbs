package load

import (
	"bufio"
	"bytes"
	"io"
	"testing"
)

type testBatch struct {
	id  int
	len int
}

func (b *testBatch) Len() int { return b.len }

func (b *testBatch) Append(p *Point) {
	b.len++
	b.id = int(p.Data.(byte))
}

func TestAckAndMaybeSend(t *testing.T) {
	cases := []struct {
		desc         string
		unsent       []Batch
		count        int
		afterCount   int
		afterLen     int
		afterFirstID int
	}{
		{
			desc:       "unsent is nil",
			unsent:     nil,
			count:      0,
			afterCount: -1,
			afterLen:   0,
		},
		{
			desc:       "unsent has 0 elements",
			unsent:     []Batch{},
			count:      0,
			afterCount: -1,
			afterLen:   0,
		},
		{
			desc:       "unsent has 1 element",
			unsent:     []Batch{&testBatch{1, 1}},
			count:      1,
			afterCount: 0,
			afterLen:   0,
		},
		{
			desc:         "unsent has 2 elements",
			unsent:       []Batch{&testBatch{1, 1}, &testBatch{2, 1}},
			count:        2,
			afterCount:   1,
			afterLen:     1,
			afterFirstID: 2,
		},
	}
	ch := createDuplexChannel(100)
	for _, c := range cases {
		c.unsent = sendOutstandingBatchToWorker(ch, &c.count, c.unsent)
		if c.afterCount != c.count {
			t.Errorf("%s: count incorrect: want %d got %d", c.desc, c.afterCount, c.count)
		}
		if c.afterLen != len(c.unsent) {
			t.Errorf("%s: len incorrect: want %d got %d", c.desc, c.afterLen, len(c.unsent))
		}
		if len(c.unsent) > 0 {
			if got := c.unsent[0].(*testBatch); c.afterFirstID != got.id {
				t.Errorf("%s: first element incorrect: want %d got %d", c.desc, c.afterFirstID, got.id)
			}
		}
	}
}

func TestSendOrQueueBatch(t *testing.T) {
	cases := []struct {
		desc           string
		unsent         []Batch
		toSend         []Batch
		queueSize      int
		count          int
		afterCount     int
		afterUnsentLen int
		afterChanLen   int
	}{
		{
			desc:           "unsent is empty, queue does not fill up",
			unsent:         []Batch{},
			toSend:         []Batch{&testBatch{1, 1}},
			queueSize:      1,
			count:          0,
			afterCount:     1,
			afterUnsentLen: 0,
			afterChanLen:   1,
		},
		{
			desc:           "unsent is empty, queue fills up",
			unsent:         []Batch{},
			toSend:         []Batch{&testBatch{1, 1}, &testBatch{2, 1}},
			queueSize:      1,
			count:          0,
			afterCount:     2,
			afterUnsentLen: 1,
			afterChanLen:   1,
		},
		{
			desc:           "unsent is non-empty, queue fills up",
			unsent:         []Batch{&testBatch{1, 1}},
			toSend:         []Batch{&testBatch{2, 1}, &testBatch{3, 1}},
			queueSize:      2,
			count:          1,
			afterCount:     3,
			afterUnsentLen: 3,
			afterChanLen:   0,
		},
	}
	for _, c := range cases {
		ch := createDuplexChannel(c.queueSize)
		for _, b := range c.toSend {
			c.unsent = sendOrQueueBatch(ch, &c.count, b, c.unsent)
		}
		if c.afterCount != c.count {
			t.Errorf("%s: count incorrect: want %d got %d", c.desc, c.afterCount, c.count)
		}
		if c.afterUnsentLen != len(c.unsent) {
			t.Errorf("%s: unsent len incorrect: want %d got %d", c.desc, c.afterUnsentLen, len(c.unsent))
		}
		if c.afterChanLen != len(ch.toWorker) {
			t.Errorf("%s: unsent chan incorrect: want %d got %d", c.desc, c.afterChanLen, len(ch.toWorker))
		}
	}
}

type testDecoder struct {
	called uint64
}

func (d *testDecoder) Decode(br *bufio.Reader) *Point {
	ret := &Point{}
	b, err := br.ReadByte()
	if err != nil {
		if err == io.EOF {
			return nil
		}
		panic(err)
	}
	ret.Data = b
	d.called++

	return ret
}

type testFactory struct{}

func (f *testFactory) New() Batch {
	return &testBatch{}
}

func _checkScan(t *testing.T, desc string, called, read, want uint64) {
	if called != want {
		t.Errorf("%s: decoder not called enough: got %d want %d", desc, called, want)
	}
	if read != want {
		t.Errorf("%s: read incorrect: got %d want %d", desc, read, want)
	}
}

func _boringWorker(c *duplexChannel) {
	for _ = range c.toWorker {
		c.sendToScanner()
	}
}

func TestScanWithIndexer(t *testing.T) {
	data := []byte{0x00, 0x01, 0x02}

	cases := []struct {
		desc      string
		batchSize uint
		limit     uint64
		wantCalls uint64
	}{
		{
			desc:      "scan w/ zero limit",
			batchSize: 1,
			limit:     0,
			wantCalls: uint64(len(data)),
		},
		{
			desc:      "scan w/ one limit",
			batchSize: 1,
			limit:     1,
			wantCalls: 1,
		},
		{
			desc:      "scan w/ over limit",
			batchSize: 1,
			limit:     4,
			wantCalls: uint64(len(data)),
		},

		{
			desc:      "scan w/ leftover batches",
			batchSize: 2,
			limit:     4,
			wantCalls: uint64(len(data)),
		},
	}
	for _, c := range cases {
		br := bufio.NewReader(bytes.NewReader(data))
		channels := []*duplexChannel{createDuplexChannel(1)}
		decoder := &testDecoder{0}
		indexer := &ConstantIndexer{}
		go _boringWorker(channels[0])
		read := scanWithIndexer(channels, c.batchSize, c.limit, br, decoder, &testFactory{}, indexer)
		_checkScan(t, c.desc, decoder.called, read, c.wantCalls)
	}
}
