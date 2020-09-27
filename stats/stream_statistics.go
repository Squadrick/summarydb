package stats

type StreamStatistics struct {
	FirstArrivalTimestamp int64
	LastArrivalTimestamp  int64
	NumValues             uint64
	IntervalStats         *Welford
	ValueStats            *Welford
}

func NewStreamStatistics() *StreamStatistics {
	return &StreamStatistics{
		FirstArrivalTimestamp: -1,
		LastArrivalTimestamp:  -1,
		NumValues:             0,
		IntervalStats:         NewWelford(),
		ValueStats:            NewWelford(),
	}
}

func (stream *StreamStatistics) Append(timestamp int64, value float64) {
	if stream.FirstArrivalTimestamp == -1 {
		stream.FirstArrivalTimestamp = timestamp
	} else {
		interval := timestamp - stream.LastArrivalTimestamp
		stream.IntervalStats.Update(float64(interval))
	}

	// TODO: value is number
	stream.ValueStats.Update(value)
	stream.NumValues++
	stream.LastArrivalTimestamp = timestamp
}
