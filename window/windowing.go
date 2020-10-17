package window

type Info struct {
	Id   int64
	Size int64
}

var shutdownSentinel *Info = nil
var flushSentinel *Info = nil

func ConstShutdownSentinel() *Info {
	if shutdownSentinel == nil {
		shutdownSentinel = &Info{-1, -1}
	}
	return shutdownSentinel
}

func ConstFlushSentinel() *Info {
	if flushSentinel == nil {
		flushSentinel = &Info{-1, -1}
	}
	return flushSentinel
}

type Windowing interface {
	// Return the first T' >= T such that at T', the interval [l, r]
	// is contained inside a single window.
	GetFirstContainingTime(Tl, Tr, T int64) (int64, bool)

	GetSizeOfFirstWindow() int64

	// Return the sizes of the first K windows, where
	// 		first K windows cover <= n elements
	// 		first K+1 windows cover > n elements
	GetWindowsCoveringUpto(n int64) []int64
}
