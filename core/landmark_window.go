package core

// Window holding exact values

type Landmark struct {
	Timestamp int64
	Data      *DataTable
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

func (landmarkWindow *LandmarkWindow) Insert(timestamp int64, data *DataTable) {
	landmarkWindow.Landmarks = append(landmarkWindow.Landmarks, Landmark{
		Timestamp: timestamp,
		Data:      data,
	})
}

func (landmarkWindow *LandmarkWindow) Close(timestamp int64) {
	landmarkWindow.TimeEnd = timestamp
}
