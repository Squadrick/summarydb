package window

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
