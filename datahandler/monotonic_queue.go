package datahandler

type MonotonicMaxQueue struct {
	windowlengthus int64
	deque          *RingDeque
}

type MonotonicMinQueue struct {
	windowlengthus int64
	deque          *RingDeque
}

func NewMonotonicMaxQueue(windowlengthus int64, capacity int) *MonotonicMaxQueue {
	return &MonotonicMaxQueue{
		windowlengthus: windowlengthus,
		deque:          NewRingDeque(capacity),
	}
}

func NewMonotonicMinQueue(windowlengthus int64, capacity int) *MonotonicMinQueue {
	return &MonotonicMinQueue{
		windowlengthus: windowlengthus,
		deque:          NewRingDeque(capacity),
	}
}

func (mq *MonotonicMaxQueue) Push(t Tick) {
	// 1. remove useless ticks
	for !mq.deque.IsEmpty() && mq.deque.Back().Price <= t.Price {
		mq.deque.PopBack()
	}
	// 2. push tick
	mq.deque.PushBack(t)
	// 3. remove old ticks
	cutoffTime := t.Timestamp - mq.windowlengthus
	for !mq.deque.IsEmpty() && mq.deque.Front().Timestamp < cutoffTime {
		mq.deque.PopFront()
	}
}

func (mq *MonotonicMinQueue) Push(t Tick) {
	// 1. remove useless ticks
	for !mq.deque.IsEmpty() && mq.deque.Back().Price >= t.Price {
		mq.deque.PopBack()
	}
	// 2. push tick
	mq.deque.PushBack(t)
	// 3. remove old ticks
	cutoffTime := t.Timestamp - mq.windowlengthus
	for !mq.deque.IsEmpty() && mq.deque.Front().Timestamp < cutoffTime {
		mq.deque.PopFront()
	}
}

func (mq *MonotonicMaxQueue) Max() Tick {
	return mq.deque.Front()
}

func (mq *MonotonicMinQueue) Min() Tick {
	return mq.deque.Front()
}

func (mq *MonotonicMaxQueue) IsEmpty() bool {
	return mq.deque.IsEmpty()
}

func (mq *MonotonicMinQueue) IsEmpty() bool {
	return mq.deque.IsEmpty()
}
