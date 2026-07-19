package datahandler

type SymbolManager struct {
	symbol             string
	arbMsgCh           chan<- ArbitrageMsg
	arbitrageThreshold float64
	slowWindowBid      *MonotonicMinQueue
	slowWindowAsk      *MonotonicMaxQueue
	fastWindowBid      *MonotonicMinQueue
	fastWindowAsk      *MonotonicMaxQueue
	pendingToSlowBid   RingDeque
	pendingToSlowAsk   RingDeque
}

func NewSymbolManager(symbol string, slowWindowLength int64, fastWindowLength int64, arbMsgCh chan<- ArbitrageMsg, arbitrageThreshold float64, capacity int) *SymbolManager {
	return &SymbolManager{
		symbol:             symbol,
		arbMsgCh:           arbMsgCh,
		arbitrageThreshold: arbitrageThreshold,
		slowWindowBid:      NewMonotonicMinQueue(slowWindowLength, capacity),
		slowWindowAsk:      NewMonotonicMaxQueue(slowWindowLength, capacity),
		fastWindowBid:      NewMonotonicMinQueue(fastWindowLength, capacity),
		fastWindowAsk:      NewMonotonicMaxQueue(fastWindowLength, capacity),
		pendingToSlowBid:   *NewRingDeque(capacity),
		pendingToSlowAsk:   *NewRingDeque(capacity),
	}
}

func (sm *SymbolManager) Update(timestamp int64, bidPrice int64, askPrice int64) {
	bidTick := Tick{Timestamp: timestamp, Price: bidPrice}
	askTick := Tick{Timestamp: timestamp, Price: askPrice}
	sm.fastWindowBid.Push(bidTick)
	sm.fastWindowAsk.Push(askTick)
	sm.pendingToSlowBid.PushBack(bidTick)
	sm.pendingToSlowAsk.PushBack(askTick)

	slowCutoffTime := timestamp - sm.fastWindowBid.windowlengthus
	for !sm.pendingToSlowBid.IsEmpty() && sm.pendingToSlowBid.Front().Timestamp < slowCutoffTime {
		sm.slowWindowBid.Push(sm.pendingToSlowBid.Front())
		sm.pendingToSlowBid.PopFront()
	}
	for !sm.pendingToSlowAsk.IsEmpty() && sm.pendingToSlowAsk.Front().Timestamp < slowCutoffTime {
		sm.slowWindowAsk.Push(sm.pendingToSlowAsk.Front())
		sm.pendingToSlowAsk.PopFront()
	}
}

func (sm *SymbolManager) ArbitrageCheck() {
	// long condition check
	longExitPrice := sm.fastWindowBid.Min().Price
	longEntryPrice := sm.slowWindowAsk.Max().Price
	longpct := float64(longExitPrice-longEntryPrice) / float64(longEntryPrice)
	// short condition check
	shortEntryPrice := sm.slowWindowBid.Min().Price
	shortExitPrice := sm.fastWindowAsk.Max().Price
	shortpct := float64(shortEntryPrice-shortExitPrice) / float64(shortExitPrice)

	// check threshold
	if longpct < sm.arbitrageThreshold && shortpct < sm.arbitrageThreshold {
		return
	}
	if longpct > shortpct {
		msg := ArbitrageMsg{
			Symbol:     sm.symbol,
			IsLong:     true,
			EntryPrice: longEntryPrice,
			ExitPrice:  longExitPrice,
		}
		sm.arbMsgCh <- msg
	} else {
		msg := ArbitrageMsg{
			Symbol:     sm.symbol,
			IsLong:     false,
			EntryPrice: shortEntryPrice,
			ExitPrice:  shortExitPrice,
		}
		sm.arbMsgCh <- msg
	}
}

type ArbitrageMsg struct {
	Symbol     string
	IsLong     bool
	EntryPrice int64
	ExitPrice  int64
}
