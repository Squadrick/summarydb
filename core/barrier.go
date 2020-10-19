package core

const (
	SUMMARIZER = iota
	WRITER     = iota
	MERGER     = iota
)

type Barrier struct {
	counters [3]chan bool
}

func NewBarrier() *Barrier {
	return &Barrier{
		counters: [3]chan bool{make(chan bool, 1), make(chan bool, 1), make(chan bool, 1)},
	}
}

func (b *Barrier) Notify(barrierType int) {
	b.counters[barrierType] <- true
}

func (b *Barrier) Wait(barrierType int) {
	<-b.counters[barrierType]
}
