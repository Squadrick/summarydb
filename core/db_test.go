package core

import (
	"github.com/stretchr/testify/assert"
	"os"
	"summarydb/window"
	"sync"
	"testing"
)

func TestBasicDB(t *testing.T) {
	dbPath := "testdb"
	var streamId int64
	{
		err := os.RemoveAll(dbPath)
		assert.NoError(t, err)
		db, err := New(dbPath)
		assert.NoError(t, err)
		exp := window.NewExponentialLengthsSequence(2)
		stream, err := db.NewStream([]string{"count", "sum"}, exp)
		assert.NoError(t, err)
		err = stream.Run()
		assert.NoError(t, err)
		streamId = stream.streamId
		for i := 0; i < 100; i++ {
			err := stream.Append(int64(i), float64(i))
			assert.NoError(t, err)
		}

		err = db.Close()
		assert.NoError(t, err)
		assert.Equal(t, err, nil)
	}
	{
		params := QueryParams{
			ConfidenceLevel: 0.95,
			SDMultiplier:    1.0,
		}
		db, err := Open(dbPath)
		assert.NoError(t, err)
		stream, err := db.GetStream(streamId)
		assert.NoError(t, err)
		{
			result, err := stream.Query("count", 0, 99, &params)
			assert.NoError(t, err)
			assert.Equal(t, result.value.Count.Value, 100.0)
			assert.Equal(t, result.error, 0.0)
		}
		{
			result, err := stream.Query("sum", 0, 99, &params)
			assert.NoError(t, err)
			assert.Equal(t, result.value.Sum.Value, 99.0*100/2)
			assert.Equal(t, result.error, 0.0)
		}
		numSummaryWindows, err := stream.manager.GetSummaryWindowInRange(0, 99)
		assert.NoError(t, err)
		assert.Equal(t, len(numSummaryWindows), 9)
	}
}

func TestDBWithLambda(t *testing.T) {
	dbPath := "testdb2"
	var streamId int64
	{
		err := os.RemoveAll(dbPath)
		assert.NoError(t, err)
		db, err := New(dbPath)
		assert.NoError(t, err)
		exp := window.NewExponentialLengthsSequence(2)
		stream, err := db.NewStream([]string{"count", "sum"}, exp)
		assert.NoError(t, err)
		err = stream.Run()
		assert.NoError(t, err)
		streamId = stream.streamId
		for i := 0; i < 100; i++ {
			if i == 90 {
				err := stream.StartLandmark(int64(i))
				assert.NoError(t, err)
			}
			err := stream.Append(int64(i), float64(i))
			assert.NoError(t, err)
		}
		err = stream.EndLandmark(int64(99))
		assert.NoError(t, err)

		err = db.Close()
		assert.NoError(t, err)
	}
	{
		params := QueryParams{
			ConfidenceLevel: 0.95,
			SDMultiplier:    1.0,
		}
		db, err := Open(dbPath)
		assert.NoError(t, err)
		stream, err := db.GetStream(streamId)
		assert.NoError(t, err)
		{
			result, err := stream.Query("count", 0, 99, &params)
			assert.NoError(t, err)
			assert.Equal(t, result.value.Count.Value, 100.0)
			assert.Equal(t, result.error, 0.0)
		}
		{
			result, err := stream.Query("sum", 0, 99, &params)
			assert.NoError(t, err)
			assert.Equal(t, result.value.Sum.Value, 99.0*100/2)
			assert.Equal(t, result.error, 0.0)
		}

		numSummaryWindows, err := stream.manager.GetSummaryWindowInRange(0, 99)
		assert.NoError(t, err)
		assert.Equal(t, len(numSummaryWindows), 10)
	}
}

// Test that the appends to a stream after it is loaded from disk
// works as expected. This checks for the persistence of the heap
// to disk while flushing.
func TestDBAppendAfterRead(t *testing.T) {
	dbPath := "testdb3"
	var streamId int64
	{
		err := os.RemoveAll(dbPath)
		assert.NoError(t, err)
		db, err := New(dbPath)
		assert.NoError(t, err)
		exp := window.NewExponentialLengthsSequence(2)
		stream, err := db.NewStream([]string{"count", "sum"}, exp)
		assert.NoError(t, err)
		err = stream.Run()
		assert.NoError(t, err)
		streamId = stream.streamId
		for i := 0; i < 50; i++ {
			err := stream.Append(int64(i), float64(i))
			assert.NoError(t, err)
		}

		err = db.Close()
		assert.NoError(t, err)
	}
	{
		db, err := Open(dbPath)
		assert.NoError(t, err)
		stream, err := db.GetStream(streamId)
		assert.NoError(t, err)
		params := QueryParams{
			ConfidenceLevel: 0.95,
			SDMultiplier:    1.0,
		}
		{
			result, err := stream.Query("count", 0, 49, &params)
			assert.NoError(t, err)
			assert.Equal(t, result.value.Count.Value, 50.0)
			assert.Equal(t, result.error, 0.0)
		}
		{
			result, err := stream.Query("sum", 0, 49, &params)
			assert.NoError(t, err)
			assert.Equal(t, result.value.Sum.Value, 49.0*50/2)
			assert.Equal(t, result.error, 0.0)
		}
		err = stream.Run()
		assert.NoError(t, err)
		for i := 50; i < 100; i++ {
			err := stream.Append(int64(i), float64(i))
			assert.NoError(t, err)
		}

		err = db.Close()
		assert.NoError(t, err)
	}
	{
		params := QueryParams{
			ConfidenceLevel: 0.95,
			SDMultiplier:    1.0,
		}
		db, err := Open(dbPath)
		assert.NoError(t, err)
		stream, err := db.GetStream(streamId)
		assert.NoError(t, err)
		{
			result, err := stream.Query("count", 0, 99, &params)
			assert.NoError(t, err)
			assert.Equal(t, result.value.Count.Value, 100.0)
			assert.Equal(t, result.error, 0.0)
		}
		{
			result, err := stream.Query("sum", 0, 99, &params)
			assert.NoError(t, err)
			assert.Equal(t, result.value.Sum.Value, 99.0*100/2)
			assert.Equal(t, result.error, 0.0)
		}
		numSummaryWindows, err := stream.manager.GetSummaryWindowInRange(0, 99)
		assert.NoError(t, err)
		assert.Equal(t, len(numSummaryWindows), 9)
	}
}

func testStub(t *testing.T,
	dbPath string,
	timesteps int64,
	seq window.LengthsSequence,
	expectedWindows1 int,
	expectedWindows2 int) {

	var streamId int64
	config := StoreConfig{
		EachBufferSize:  32,
		NumBuffer:       8,
		WindowsPerMerge: 8,
	}
	{
		err := os.RemoveAll(dbPath)
		assert.NoError(t, err)
		db, err := New(dbPath)
		assert.NoError(t, err)
		stream, err := db.NewStream([]string{"count", "sum", "max"}, seq)
		assert.NoError(t, err)
		stream.SetConfig(&config)
		err = stream.Run()
		assert.NoError(t, err)
		streamId = stream.streamId
		for i := int64(0); i < timesteps; i++ {
			err := stream.Append(i, 2*float64(i))
			assert.NoError(t, err)
		}

		err = stream.Flush()
		assert.NoError(t, err)
		err = db.Close()
		assert.NoError(t, err)
	}
	{
		params := QueryParams{
			ConfidenceLevel: 0.95,
			SDMultiplier:    1.0,
		}
		db, err := Open(dbPath)
		assert.NoError(t, err)
		stream, err := db.GetStream(streamId)
		assert.NoError(t, err)
		{
			result, err := stream.Query("count", 0, timesteps-1, &params)
			assert.NoError(t, err)
			assert.Equal(t, result.value.Count.Value, float64(timesteps))
			assert.Equal(t, result.error, 0.0)
		}
		{
			result, err := stream.Query("sum", 0, timesteps-1, &params)
			assert.NoError(t, err)
			assert.Equal(t, result.value.Sum.Value, float64((timesteps-1)*timesteps))
			assert.Equal(t, result.error, 0.0)
		}
		{
			result, err := stream.Query("max", 0, timesteps-1, &params)
			assert.NoError(t, err)
			assert.Equal(t, result.value.Max.Value, 2*float64(timesteps-1))
		}
		numSummaryWindows, err := stream.manager.GetSummaryWindowInRange(0, timesteps)
		assert.NoError(t, err)
		assert.Equal(t, len(numSummaryWindows), expectedWindows1)

		stream.SetConfig(&config)
		err = stream.Run()
		assert.NoError(t, err)

		for i := timesteps; i < 2*timesteps; i++ {
			err := stream.Append(i, 2*float64(i))
			assert.NoError(t, err)
		}

		err = stream.Flush()
		assert.NoError(t, err)
		err = db.Close()
		assert.NoError(t, err)
	}
	{
		timesteps *= 2
		params := QueryParams{
			ConfidenceLevel: 0.95,
			SDMultiplier:    1.0,
		}
		db, err := Open(dbPath)
		assert.NoError(t, err)
		stream, err := db.GetStream(streamId)
		assert.NoError(t, err)
		{
			result, err := stream.Query("count", 0, timesteps-1, &params)
			assert.NoError(t, err)
			assert.Equal(t, result.value.Count.Value, float64(timesteps))
			assert.Equal(t, result.error, 0.0)
		}
		{
			result, err := stream.Query("sum", 0, timesteps-1, &params)
			assert.NoError(t, err)
			assert.Equal(t, result.value.Sum.Value, float64((timesteps-1)*timesteps))
			assert.Equal(t, result.error, 0.0)
		}
		{
			result, err := stream.Query("max", 0, timesteps-1, &params)
			assert.NoError(t, err)
			assert.Equal(t, result.value.Max.Value, 2*float64(timesteps-1))
		}

		numSummaryWindows, err := stream.manager.GetSummaryWindowInRange(0, timesteps)
		assert.NoError(t, err)
		assert.Equal(t, len(numSummaryWindows), expectedWindows2)
	}
}

func TestDB_Power_5K(t *testing.T) {
	dbPath := "testdb_power_5k"
	seq := window.NewPowerLengthsSequence(1, 1, 10, 1)
	testStub(t, dbPath, 5000, seq, 442, 598)
}

func TestDB_Power_10K(t *testing.T) {
	dbPath := "testdb_power_10k"
	seq := window.NewPowerLengthsSequence(1, 1, 10, 1)
	testStub(t, dbPath, 10000, seq, 598, 910)
}

func TestDB_Exp2_5K(t *testing.T) {
	dbPath := "testdb_exp2_5k"
	seq := window.NewExponentialLengthsSequence(2)
	testStub(t, dbPath, 5000, seq, 16, 18)
}

func TestDB_Exp2_10K(t *testing.T) {
	dbPath := "testdb_exp2_10k"
	seq := window.NewExponentialLengthsSequence(2)
	testStub(t, dbPath, 10000, seq, 18, 20)
}

func TestDB_Exp1Point5_10K(t *testing.T) {
	dbPath := "testdb_1point5_10k"
	seq := window.NewExponentialLengthsSequence(1.5)
	testStub(t, dbPath, 10000, seq, 33, 35)
}

func BenchmarkDB_Append(b *testing.B) {
	dbPath := "testdb_bm1"
	nStreams := 8

	if b.N/nStreams < 1 {
		return
	}

	err := os.RemoveAll(dbPath)
	if err != nil {
		b.FailNow()
	}
	db, err := New(dbPath)
	if err != nil {
		b.FailNow()
	}
	wg := sync.WaitGroup{}
	for s := 0; s < nStreams; s += 1 {
		wg.Add(1)
		go func() {
			exp := window.NewExponentialLengthsSequence(2)
			stream, err := db.NewStream([]string{"count", "sum", "max"}, exp)
			if err != nil {
				b.FailNow()
			}
			err = stream.Run()
			if err != nil {
				b.FailNow()
			}

			for i := 0; i < b.N/nStreams; i++ {
				err := stream.Append(int64(i), float64(i))
				if err != nil {
					b.FailNow()
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	err = db.Close()
	if err != nil {
		b.FailNow()
	}
}

func BenchmarkDB_Append_Buffered(b *testing.B) {
	dbPath := "testdb_bm2"
	nStreams := 64

	if b.N/nStreams < 1 {
		return
	}

	err := os.RemoveAll(dbPath)
	if err != nil {
		b.FailNow()
	}
	db, err := New(dbPath)
	if err != nil {
		b.FailNow()
	}
	wg := sync.WaitGroup{}
	config := StoreConfig{
		EachBufferSize:  32,
		NumBuffer:       8,
		WindowsPerMerge: 8,
	}
	for s := 0; s < nStreams; s += 1 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			exp := window.NewExponentialLengthsSequence(2)
			stream, err := db.NewStream([]string{"count", "sum", "max"}, exp)
			if err != nil {
				b.FailNow()
			}
			stream.SetConfig(&config)
			err = stream.Run()
			if err != nil {
				b.FailNow()
			}

			for i := 0; i <= b.N/nStreams; i++ {
				err := stream.Append(int64(i), float64(i))
				if err != nil {
					b.FailNow()
				}
			}
			err = stream.Flush()
			if err != nil {
				b.FailNow()
			}
		}()
	}
	wg.Wait()
	err = db.Close()
	if err != nil {
		b.FailNow()
	}
}
