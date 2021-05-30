package core

import (
	"context"
	"github.com/stretchr/testify/assert"
	"summarydb/window"
	"sync"
	"testing"
)

func TestBasicDB(t *testing.T) {
	dbPath := "testdb"
	var streamId int64
	{
		db := New(dbPath)
		exp := window.NewExponentialLengthsSequence(2)
		stream := db.NewStream([]string{"count", "sum"}, exp)
		ctx, cancelFunc := context.WithCancel(context.Background())
		// TODO: Let `db` start the stream, and keep track of the ctxs?
		stream.Run(ctx)
		streamId = stream.streamId
		for i := 0; i < 100; i++ {
			stream.Append(int64(i), float64(i))
		}

		err := db.Close()
		cancelFunc()
		assert.Equal(t, err, nil)
	}
	{
		params := QueryParams{
			ConfidenceLevel: 0.95,
			SDMultiplier:    1.0,
		}
		db := Open(dbPath)
		stream := db.GetStream(streamId)
		{
			result := stream.Query("count", 0, 99, &params)
			assert.Equal(t, result.value.Count.Value, 100.0)
			assert.Equal(t, result.error, 0.0)
		}
		{
			result := stream.Query("sum", 0, 99, &params)
			assert.Equal(t, result.value.Sum.Value, 99.0*100/2)
			assert.Equal(t, result.error, 0.0)
		}
	}
}

func TestDBWithLambda(t *testing.T) {
	dbPath := "testdb2"
	var streamId int64
	{
		db := New(dbPath)
		exp := window.NewExponentialLengthsSequence(2)
		stream := db.NewStream([]string{"count", "sum"}, exp)
		ctx, cancelFunc := context.WithCancel(context.Background())
		// TODO: Let `db` start the stream, and keep track of the ctxs?
		stream.Run(ctx)
		streamId = stream.streamId
		for i := 0; i < 100; i++ {
			if i == 90 {
				stream.StartLandmark(int64(i))
			}
			stream.Append(int64(i), float64(i))
		}
		stream.EndLandmark(int64(99))

		cancelFunc()
		err := db.Close()
		assert.Equal(t, err, nil)
	}
	{
		params := QueryParams{
			ConfidenceLevel: 0.95,
			SDMultiplier:    1.0,
		}
		db := Open(dbPath)
		stream := db.GetStream(streamId)
		{
			result := stream.Query("count", 0, 99, &params)
			assert.Equal(t, result.value.Count.Value, 100.0)
			assert.Equal(t, result.error, 0.0)
		}
		{
			result := stream.Query("sum", 0, 99, &params)
			assert.Equal(t, result.value.Sum.Value, 99.0*100/2)
			assert.Equal(t, result.error, 0.0)
		}
	}
}

func BenchmarkDB_Append(b *testing.B) {
	dbPath := "testdb_bm1"
	nStreams := 8
	db := New(dbPath)
	wg := sync.WaitGroup{}
	for s := 0; s < nStreams; s += 1 {
		wg.Add(1)
		go func() {
			exp := window.NewExponentialLengthsSequence(2)
			stream := db.NewStream([]string{"count", "sum", "max"}, exp)
			ctx, cancelFunc := context.WithCancel(context.Background())
			defer cancelFunc()
			stream.Run(ctx)

			for i := 0; i < b.N/nStreams; i++ {
				stream.Append(int64(i), float64(i))
			}
			wg.Done()
		}()
	}
	wg.Wait()
	err := db.Close()
	if err != nil {
		panic(err)
	}
}

func BenchmarkDB_Append_Buffered(b *testing.B) {
	dbPath := "testdb_bm2"
	nStreams := 8
	db := New(dbPath)
	wg := sync.WaitGroup{}
	config := StoreConfig{
		EachBufferSize:  32,
		NumBuffer:       8,
		WindowsPerMerge: 8,
	}
	for s := 0; s < nStreams; s += 1 {
		wg.Add(1)
		go func() {
			exp := window.NewExponentialLengthsSequence(2)
			stream := db.NewStream([]string{"count", "sum", "max"}, exp)
			stream.SetConfig(&config)
			ctx, cancelFunc := context.WithCancel(context.Background())
			defer cancelFunc()
			stream.Run(ctx)

			for i := 0; i <= b.N/nStreams; i++ {
				stream.Append(int64(i), float64(i))
			}
			stream.Flush()
			wg.Done()
		}()
	}
	wg.Wait()
	err := db.Close()
	if err != nil {
		panic(err)
	}
}
