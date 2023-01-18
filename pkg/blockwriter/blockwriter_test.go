package blockwriter_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/ipfs/stargate/pkg/blockwriter"
	"github.com/stretchr/testify/require"
)

func TestBlockWriter(t *testing.T) {
	ctx := context.Background()
	req := require.New(t)
	sequence := []uint64{100, 500, 1000, 1000, 200, 1000, 1500, 30}
	reader, underlying := io.Pipe()
	writer := blockwriter.NewBlockWriter(underlying, 2000, func(error) bool { return false })
	written := make(chan uint64, len(sequence))
	go func() {
		for _, next := range sequence {
			val := make([]byte, next)
			writer.Write(val)
			written <- next
		}
	}()
	checks := []struct {
		read           uint64
		expectedWrites []uint64
	}{
		{
			read:           0,
			expectedWrites: []uint64{100, 500, 1000},
		},
		{
			read:           0,
			expectedWrites: []uint64{},
		},
		{
			read:           600,
			expectedWrites: []uint64{1000},
		},
		{
			read:           1000,
			expectedWrites: []uint64{200},
		},
		{
			read:           1000,
			expectedWrites: []uint64{1000},
		},
		{
			read:           200,
			expectedWrites: []uint64{},
		},
		{
			read:           1000,
			expectedWrites: []uint64{1500, 30},
		},
	}

	for _, check := range checks {
		if check.read > 0 {
			buf := make([]byte, check.read)
			_, err := reader.Read(buf)
			req.NoError(err)
		}
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		for _, expectedWrite := range check.expectedWrites {
			select {
			case <-ctx.Done():
				req.FailNow("did not get expected writes")
			case receivedWrite := <-written:
				require.Equal(t, expectedWrite, receivedWrite)
			}
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-written:
			req.FailNow("received unexpected writes")
		case <-timer.C:
		}
	}
}
