package load

import (
	"bufio"
	"reflect"
)

// sendOutstandingBatchToWorker adjust the outstanding batches count and potentially sends
// another batch to the worker via ch. If unsent is non-empty, send.
// Returns the updated state of unsent
func sendOutstandingBatchToWorker(ch *duplexChannel, count *int, unsent []Batch) []Batch {
	*count--
	// if there are still batches waiting, send the next
	if len(unsent) > 0 {
		ch.sendToWorker(unsent[0])
		if len(unsent) > 1 {
			return unsent[1:]
		}
		return unsent[:0]
	}
	return unsent
}

// sendOrQueueBatch attempts to send a Batch of data on a duplexChannel; if it would block
// or there is other work to be sent first, the Batch is stored on a queue. The count of
// outstanding work is adjusted upwards
func sendOrQueueBatch(ch *duplexChannel, count *int, batch Batch, outstandingBatches []Batch) []Batch {
	// In case there are no outstanding batches yet and there are empty positions in toWorker queue
	// we can send/put batch into toWorker queue
	*count++
	if len(outstandingBatches) == 0 && len(ch.toWorker) < cap(ch.toWorker) {
		ch.sendToWorker(batch)
	} else {
		return append(outstandingBatches, batch)
	}
	return outstandingBatches
}

// Batch is an aggregate of points for a particular data system.
// It needs to have a way to measure it's size to make sure
// it does not get too large and it needs a way to append a point
type Batch interface {
	Len() int
	Append(*Point)
}

// Point acts a 'holder' for the internal representation of a point in a given load client.
// Instead of using interface{} as a return type, we get compile safety by using Point
type Point struct {
	Data interface{}
}

// NewPoint creates a Point with the provided data as the internal representation
func NewPoint(data interface{}) *Point {
	return &Point{Data: data}
}

// PointIndexer determines the index of the Batch (and subsequently the channel)
// that a particular point belongs to
type PointIndexer interface {
	// GetIndex returns a partition for the given Point
	GetIndex(*Point) int
}

// ConstantIndexer always puts the item on a single channel. This is the typical
// use case where all the workers share the same channel
type ConstantIndexer struct{}

// GetIndex returns a constant index (0) regardless of Point
func (i *ConstantIndexer) GetIndex(_ *Point) int {
	return 0
}

// BatchFactory returns a new empty batch for storing points.
type BatchFactory interface {
	// New returns a new Batch to add Points to
	New() Batch
}

// PointDecoder decodes the next data point in the process of scanning.
type PointDecoder interface {
	//Decode creates a Point from a data stream
	Decode(*bufio.Reader) *Point
}

// ScanWithIndexer reads data from the provided bufio.Reader br until a limit is reached (if -1, all items are read).
// Data is decoded by PointDecoder decoder and then placed into appropriate batches, using the supplied PointIndexer,
// which are then dispatched to workers (duplexChannel chosen by PointIndexer). Scan does flow control to make sure workers are not left idle for too long
// and also that the scanning process  does not starve them of CPU.
func scanWithIndexer(channels []*duplexChannel, batchSize uint, limit uint64, br *bufio.Reader, decoder PointDecoder, factory BatchFactory, indexer PointIndexer) uint64 {
	var itemsRead uint64
	numChannels := len(channels)

	if batchSize < 1 {
		panic("--batch-size cannot be less than 1")
	}

	// Current batches (per channel) that are being filled
	batches := make([]Batch, numChannels)
	for i := range batches {
		batches[i] = factory.New()
	}

	// Batches that are ready to be set when space on a channel opens
	outstandingBatches := make([][]Batch, numChannels)
	for i := range outstandingBatches {
		outstandingBatches[i] = []Batch{}
	}

	// We use Select via reflection to either select an acknowledged channel so
	// that we can potentially send another batch, or if none are ready to continue
	// on scanning. However, when we reach a limit of outstanding batches, we also
	// want to block until one worker is done, so as not to starve the workers.
	// Using an array with Select via reflection gives us this flexibility (i.e.,
	// we can either pass the whole array of cases, or the array less the last item).
	cases := make([]reflect.SelectCase, numChannels + 1)
	for i, ch := range channels {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch.toScanner),
		}
	}
	cases[numChannels] = reflect.SelectCase{
		Dir: reflect.SelectDefault,
	}

	// Keep track of how many batches are outstanding (ocnt) so we don't go over
	// a limit (olimit), in order to slow down the scanner so it doesn't starve the workers
	ocnt := 0
	olimit := numChannels * cap(channels[0].toWorker) * 3
	for {

		// Check whether incoming items limit reached? We do not want to process more items than specified
		if limit > 0 && itemsRead == limit {
			break
		}

		caseLimit := len(cases)
		if ocnt >= olimit { // if we have too many outstanding, wait until one finishes (i.e. no default)
			caseLimit--
		}

		// Only receive an 'ok' when it's from a channel, default does not return 'ok'
		chosen, _, ok := reflect.Select(cases[:caseLimit])
		if ok {
			outstandingBatches[chosen] = sendOutstandingBatchToWorker(channels[chosen], &ocnt, outstandingBatches[chosen])
		}

		// Prepare new batch - decode new itema and append it to batch
		item := decoder.Decode(br)
		if item == nil {
			// Nothing to scan any more - input is empty or failed
			// Time to exit
			break
		}

		// Append new item to batch
		idx := indexer.GetIndex(item)
		batches[idx].Append(item)

		itemsRead++

		// In case batch is big enough - try to send it to worker
		if batches[idx].Len() >= int(batchSize) {
			outstandingBatches[idx] = sendOrQueueBatch(channels[idx], &ocnt, batches[idx], outstandingBatches[idx])
			batches[idx] = factory.New()
		}
	}

	// Finished reading input, make sure last batch goes out.
	// Enqueued batches may be smaller than batchSize requested
	for idx, b := range batches {
		// Do not enqueue empty batches (with 0 items)
		if b.Len() > 0 {
			outstandingBatches[idx] = sendOrQueueBatch(channels[idx], &ocnt, batches[idx], outstandingBatches[idx])
		}
	}

	// Wait until all the outstanding batches get acknowledged so we don't
	// prematurely close the acknowledge channels
	for {
		if ocnt == 0 {
			// No outstanding batches any more
			break
		}

		// Try to send batches to workers
		chosen, _, ok := reflect.Select(cases[:len(cases)-1])
		if ok {
			outstandingBatches[chosen] = sendOutstandingBatchToWorker(channels[chosen], &ocnt, outstandingBatches[chosen])
		}
	}

	return itemsRead
}
