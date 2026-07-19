package datahandler

type Tick struct {
	Timestamp int64
	Price     int64
}

type RingDeque struct {
	buf  []Tick
	head int
	tail int
	size int
}

func NewRingDeque(capacity int) *RingDeque {
	return &RingDeque{
		buf:  make([]Tick, capacity),
		head: 0,
		tail: 0,
		size: 0,
	}
}

func (d *RingDeque) IsEmpty() bool {
	return d.size == 0
}

func (d *RingDeque) Front() Tick {
	return d.buf[d.head]
}

func (d *RingDeque) Back() Tick {
	idx := (d.tail - 1 + len(d.buf)) % len(d.buf)
	return d.buf[idx]
}

func (d *RingDeque) PushBack(t Tick) {
	if d.size == len(d.buf) {
		panic("RingDeque is full")
	}
	d.buf[d.tail] = t
	d.tail = (d.tail + 1) % len(d.buf)
	d.size++
}

func (d *RingDeque) PopBack() {
	d.tail = (d.tail - 1 + len(d.buf)) % len(d.buf)
	d.size--
}

func (d *RingDeque) PopFront() {
	d.head = (d.head + 1) % len(d.buf)
	d.size--
}
