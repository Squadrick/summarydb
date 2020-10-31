package core

import (
	"fmt"
	"sync"
)

// Window holding a set of summary data structures.
// SummaryDB stream = list of contiguous SummaryWindows

// Use int64 for window IDs, timestamps and count markers.
// Valid values should be non-negative; use -1 for null.
type SummaryWindow struct {
	TimeStart  int64
	TimeEnd    int64
	CountStart int64
	CountEnd   int64
	Data       *DataTable
}

var shutdownSummaryWindow *SummaryWindow = nil
var shutdownSummaryWindowMutex sync.Mutex
var flushSummaryWindow *SummaryWindow = nil
var flushSummaryWindowMutex sync.Mutex

func ConstShutdownSummaryWindow() *SummaryWindow {
	shutdownSummaryWindowMutex.Lock()
	defer shutdownSummaryWindowMutex.Unlock()
	if shutdownSummaryWindow == nil {
		shutdownSummaryWindow = NewSummaryWindow(0, 0, 0, 0)
	}
	return shutdownSummaryWindow
}

func ConstFlushSummaryWindow() *SummaryWindow {
	flushSummaryWindowMutex.Lock()
	defer flushSummaryWindowMutex.Unlock()
	if flushSummaryWindow == nil {
		flushSummaryWindow = NewSummaryWindow(0, 0, 0, 0)
	}
	return flushSummaryWindow
}

func NewSummaryWindow(timeStart, timeEnd, countStart, countEnd int64) *SummaryWindow {
	window := SummaryWindow{
		TimeStart:  timeStart,
		TimeEnd:    timeEnd,
		CountStart: countStart,
		CountEnd:   countEnd,
		Data:       NewDataTable(),
	}
	return &window
}

func (window *SummaryWindow) Id() int64 {
	return window.TimeStart
}

func (window SummaryWindow) String() string {
	return fmt.Sprintf("<SummaryWindow: Time [%d, %d] Count [%d, %d]>",
		window.TimeStart,
		window.TimeEnd,
		window.CountStart,
		window.CountEnd)
}
