/*
Package blockwriter is not currently used, but it's useful code we may need

# It maintains a buffered queue for writing blocks

TODO: Delete when confirmed not to be needed
*/
package blockwriter

import (
	"io"
	"sync"
)

type block struct {
	data []byte
	next *block
}

var blockPool = sync.Pool{
	New: func() interface{} {
		return new(block)
	},
}

func newBlock() *block {
	newItem := blockPool.Get().(*block)
	// need to reset next value to nil we're pulling out of a pool of potentially
	// old objects
	newItem.next = nil
	return newItem
}

type BlockWriter struct {
	head        *block
	tail        *block
	dataSize    uint64
	maxSize     uint64
	lk          sync.Mutex
	fullWait    *sync.Cond
	contentWait *sync.Cond
	closeErr    error
	underlying  io.Writer
	handleError ErrorHandler
}

var _ io.WriteCloser = (*BlockWriter)(nil)

type errorType string

func (e errorType) Error() string {
	return string(e)
}

type ErrorHandler func(error) bool

const ErrClosed errorType = "writer closed"

func NewBlockWriter(underlying io.Writer, maxSize uint64, handleError ErrorHandler) *BlockWriter {
	bw := &BlockWriter{
		underlying: underlying,
		maxSize:    maxSize,
	}
	bw.contentWait = sync.NewCond(&bw.lk)
	bw.fullWait = sync.NewCond(&bw.lk)
	go bw.writeLoop()
	return bw
}

func (bq *BlockWriter) Close() error {
	bq.lk.Lock()
	if bq.closeErr != nil {
		bq.lk.Unlock()
		return bq.closeErr
	}
	bq.closeErr = ErrClosed
	bq.lk.Unlock()
	bq.contentWait.Broadcast()
	bq.fullWait.Broadcast()
	return nil
}

func (bq *BlockWriter) Write(p []byte) (n int, err error) {
	bq.lk.Lock()
	defer bq.lk.Unlock()
	for {
		if bq.closeErr != nil {
			return 0, bq.closeErr
		}
		if bq.dataSize+uint64(len(p)) < bq.maxSize {
			block := newBlock()
			block.data = p
			bq.queue(block)
			bq.contentWait.Broadcast()
			return len(p), nil
		}
		bq.fullWait.Wait()
	}
}

func (bq *BlockWriter) writeLoop() {
	bq.lk.Lock()
	defer bq.lk.Unlock()
	for {
		if bq.closeErr != nil {
			return
		}
		if bq.dataSize > 0 {
			_, err := bq.underlying.Write(bq.consume())
			if err != nil {
				if bq.handleError(err) {
					bq.closeErr = err
				}
			}
			bq.lk.Unlock()
			bq.fullWait.Broadcast()
			bq.lk.Lock()
		} else {
			bq.contentWait.Wait()
		}
	}
}

func (bq *BlockWriter) consume() []byte {
	// update our total data size buffered
	bq.dataSize -= uint64(len(bq.head.data))
	// save a reference to head
	consumed := bq.head

	// advance the queue
	bq.head = bq.head.next

	// wipe the block reference - let the memory get freed
	data := consumed.data
	consumed.data = nil
	// put the item back in the pool
	blockPool.Put(consumed)
	return data
}

func (bq *BlockWriter) queue(newItem *block) {
	// update total size buffered
	bq.dataSize += uint64(len(newItem.data))

	// queue the item
	if bq.head == nil {
		bq.tail = newItem
		bq.head = bq.tail
	} else {
		bq.tail.next = newItem
		bq.tail = bq.tail.next
	}
}
