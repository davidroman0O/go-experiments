package gronos

/// nothing fancy, it is just an incremental id generator

type flipID struct {
	current uint
}

func newFlipID() *flipID {
	return &flipID{
		current: 0, // 0 means no id, we will use Next() all the time which mean we will have `1` always as a starter
	}
}

func (f *flipID) Next() uint {
	f.current++
	return f.current
}
