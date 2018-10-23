package load

// duplexChannel acts as a two-way channel for communicating from a scan routine
// to a worker goroutine. The toWorker channel sends data to the worker for it
// to process and the toScan channel allows the worker to acknowledge completion.
// Using this we can accomplish better flow control between the scanner and workers.
type duplexChannel struct {
	toWorker  chan Batch
	toScanner chan bool
}

// createDuplexChannel returns a duplexChannel with specified buffer sizes
func createDuplexChannel(queueLen int) *duplexChannel {
	return &duplexChannel{
		toWorker:  make(chan Batch, queueLen),
		toScanner: make(chan bool, queueLen),
	}
}

// sendToWorker passes a batch of work on to the worker from the scanner
func (dc *duplexChannel) sendToWorker(b Batch) {
	dc.toWorker <- b
}

// SendToScanner passes an acknowledge to the scanner from the worker
func (dc *duplexChannel) sendToScanner() {
	dc.toScanner <- true
}

// Close closes down the duplexChannel
func (dc *duplexChannel) close() {
	close(dc.toWorker)
	close(dc.toScanner)
}
