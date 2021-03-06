package core

// Window holding exact values

type Landmark struct {
	Timestamp int64
	Value     float64
}

type LandmarkWindow struct {
	TimeStart int64
	TimeEnd   int64
	Landmarks []Landmark
}

func NewLandmarkWindow(timeStart int64) *LandmarkWindow {
	return &LandmarkWindow{
		TimeStart: timeStart,
		TimeEnd:   0,
		Landmarks: make([]Landmark, 0),
	}
}

func (window *LandmarkWindow) Id() int64 {
	return window.TimeStart
}

func (window *LandmarkWindow) Insert(timestamp int64, value float64) {
	window.Landmarks = append(window.Landmarks, Landmark{
		Timestamp: timestamp,
		Value:     value,
	})
}

func (window *LandmarkWindow) Close(timestamp int64) {
	window.TimeEnd = timestamp
}
