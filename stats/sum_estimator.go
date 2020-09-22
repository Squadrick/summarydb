package stats

import (
	"summarydb/core"
)

type WindowInfo struct {
	Start   int64
	End     int64
	Sum     float64
	Overlap int64
	Length  int64
}

func NewWindowInfo() *WindowInfo {
	return &WindowInfo{
		Start:   0,
		End:     0,
		Sum:     0,
		Overlap: 1,
		Length:  1,
	}
}

func (wi *WindowInfo) SetValues(window *core.SummaryWindow, value float64) {
	wi.Start = window.TimeStart
	wi.End = window.TimeEnd
	wi.Sum = value
}

func (wi *WindowInfo) SetLengthAndOverlap(t0 int64, t1 int64) {
	wi.Overlap = WindowOverlap(wi.Start, wi.End, t0, t1)
	wi.Length = WindowLength(wi.Start, wi.End)
}

func GetSumStats(t0, t1 int64,
	summaryWindows []core.SummaryWindow,
	landmarkWindows []core.LandmarkWindow,
	getData func(table *core.DataTable) float64) (*Bounds, *Stats) {
	firstWindow := NewWindowInfo()
	lastWindow := NewWindowInfo()
	middleWindow := NewWindowInfo()
	landmarkWindow := NewWindowInfo()

	nDecayedWindows := len(summaryWindows)
	totalSum := 0.0
	for i, window := range summaryWindows {
		value := getData(window.Data)
		if i == 0 {
			firstWindow.SetValues(&window, value)
		}
		if i == nDecayedWindows-1 {
			lastWindow.SetValues(&window, value)
		}
		totalSum += value
	}

	if nDecayedWindows == 1 { // no middle or right
		lastWindow.Sum = 0
	}

	middleWindow.Sum = totalSum - (firstWindow.Sum + lastWindow.Sum)

	firstWindow.SetLengthAndOverlap(t0, t1)
	lastWindow.SetLengthAndOverlap(t0, t1)

	for _, window := range landmarkWindows {
		firstWindow.Length -= WindowOverlap(window.TimeStart, window.TimeEnd,
			firstWindow.Start, firstWindow.End)
		firstWindow.Overlap -= WindowOverlap(window.TimeStart, window.TimeEnd,
			t0, firstWindow.End)

		// We don't run the same checks on middle-window since the data overlapping
		// with landmark windows is contained entirely within [t0, t1].

		lastWindow.Length -= WindowOverlap(window.TimeStart, window.TimeEnd,
			lastWindow.Start, lastWindow.End)
		lastWindow.Overlap -= WindowOverlap(window.TimeStart, window.TimeEnd,
			lastWindow.Start, t1)

		for _, landmark := range window.Landmarks {
			if t0 <= landmark.Timestamp && landmark.Timestamp <= t1 {
				landmarkWindow.Sum += getData(landmark.Data)
			}
		}
	}

	bounds := &Bounds{
		Lower: 0,
		Upper: 0,
	}

	stats := &Stats{
		Mean: 0,
		Var:  0,
	}

	UpdateEstimate(bounds, stats, landmarkWindow)
	UpdateEstimate(bounds, stats, firstWindow)
	UpdateEstimate(bounds, stats, middleWindow)
	UpdateEstimate(bounds, stats, lastWindow)

	return bounds, stats
}

func UpdateEstimate(bounds *Bounds, stats *Stats, info *WindowInfo) {
	bounds.Upper += info.Sum
	if info.Overlap == info.Length {
		bounds.Lower += info.Sum
	}

	if info.Overlap > 0 {
		ratio := float64(info.Overlap) / float64(info.Length)
		stats.Mean += info.Sum * ratio
		stats.Var += info.Sum * ratio * (1 - ratio)
	}
}
