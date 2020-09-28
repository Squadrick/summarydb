package core

import "summarydb/stats"

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

func (wi *WindowInfo) SetValues(window *SummaryWindow, value float64) {
	wi.Start = window.TimeStart
	wi.End = window.TimeEnd
	wi.Sum = value
}

func (wi *WindowInfo) SetLengthAndOverlap(t0 int64, t1 int64) {
	wi.Overlap = stats.WindowOverlap(wi.Start, wi.End, t0, t1)
	wi.Length = stats.WindowLength(wi.Start, wi.End)
}

func GetSumStats(t0, t1 int64,
	summaryWindows []SummaryWindow,
	landmarkWindows []LandmarkWindow,
	getData func(table *DataTable) float64) (*stats.Bounds, *stats.Stats) {
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
		firstWindow.Length -= stats.WindowOverlap(window.TimeStart, window.TimeEnd,
			firstWindow.Start, firstWindow.End)
		firstWindow.Overlap -= stats.WindowOverlap(window.TimeStart, window.TimeEnd,
			t0, firstWindow.End)

		// We don't run the same checks on middle-window since the data overlapping
		// with landmark windows is contained entirely within [t0, t1].

		lastWindow.Length -= stats.WindowOverlap(window.TimeStart, window.TimeEnd,
			lastWindow.Start, lastWindow.End)
		lastWindow.Overlap -= stats.WindowOverlap(window.TimeStart, window.TimeEnd,
			lastWindow.Start, t1)

		for _, landmark := range window.Landmarks {
			if t0 <= landmark.Timestamp && landmark.Timestamp <= t1 {
				landmarkWindow.Sum += landmark.Value
			}
		}
	}

	bounds := &stats.Bounds{
		Lower: 0,
		Upper: 0,
	}

	statistics := &stats.Stats{
		Mean: 0,
		Var:  0,
	}

	UpdateEstimate(bounds, statistics, landmarkWindow)
	UpdateEstimate(bounds, statistics, firstWindow)
	UpdateEstimate(bounds, statistics, middleWindow)
	UpdateEstimate(bounds, statistics, lastWindow)

	return bounds, statistics
}

func UpdateEstimate(bounds *stats.Bounds, stats *stats.Stats, info *WindowInfo) {
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
