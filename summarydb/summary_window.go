package summarydb

import "fmt"

// Window holding a set of summary data structures.
// SummaryDB stream = list of contiguous SummaryWindows

// Use int64 for window IDs, timestamps and count markers.
// Valid values should be non-negative; use -1 for null.
type SummaryWindow struct {
	timeStart  int64
	timeEnd    int64
	countStart int64
	countEnd   int64
}

func NewSummaryWindow(timeStart, timeEnd, countStart, countEnd int64) *SummaryWindow {
	window := SummaryWindow{
		timeStart:  timeStart,
		timeEnd:    timeEnd,
		countStart: countStart,
		countEnd:   countEnd,
	}
	return &window
}

func (window SummaryWindow) String() string {
	return fmt.Sprintf("<SummaryWindow: Time [%d, %d] Count [%d, %d]>",
		window.timeStart,
		window.timeEnd,
		window.countStart,
		window.countEnd)
}
