package main

import (
	"time"

	"github.com/Rin18/bookticker-manager/binance/binancefutures"
	"github.com/Rin18/bookticker-manager/common"
	"github.com/gammazero/deque"
)

type TickerManager struct {
	tickers  map[string]*deque.Deque[common.Ticker]
	wsClient *binancefutures.WsClient
}

func NewTickerManager() *TickerManager {
	wsClient := binancefutures.NewWsClient()
	tickers := make(map[string]*deque.Deque[common.Ticker])
	for _, symbol := range symbols {
		tickers[symbol] = new(deque.Deque[common.Ticker])
	}
	tickerManager := TickerManager{
		tickers:  tickers,
		wsClient: wsClient,
	}
	return &tickerManager
}

type arbMsg struct {
	symbol  string
	isLong  bool
	price   int64
	tpPrice int64
	slPrice int64
}

func (m *TickerManager) Start(symbols []string, period time.Duration, latency time.Duration, threshold float64, arbch chan<- arbMsg, statusch chan<- common.WsStatus) {
	ch := make(chan common.Ticker, 1000)
	go m.wsClient.Start(symbols, 2*time.Second, ch, statusch)
	for ticker := range ch {
		m.processUpdate(ticker, period)
		m.calculateArbitrage(ticker.Symbol, latency, threshold, arbch)
	}
}

// push the latest ticker to deque and pop the ones ealier than period
func (m *TickerManager) processUpdate(ticker common.Ticker, period time.Duration) {
	symbol := ticker.Symbol
	timestamp := time.Now().Add(-1 * period).UnixMicro()
	m.tickers[symbol].PushBack(ticker)
	for m.tickers[symbol].Len() > 0 {
		if m.tickers[symbol].Front().ReceiveTime < timestamp {
			m.tickers[symbol].PopFront()
		} else {
			break
		}
	}
}

func (m *TickerManager) calculateArbitrage(symbol string, latency time.Duration, threshold float64, arbch chan<- arbMsg) {
	timestamp := time.Now().Add(-1 * latency).UnixMicro()
	i := 0
	for ; i < m.tickers[symbol].Len(); i++ {
		t := m.tickers[symbol].At(i)
		if t.ReceiveTime > timestamp {
			break
		}
	}
	if i == m.tickers[symbol].Len()-1 || i == 0 {
		return
	}

	firstticker := m.tickers[symbol].At(i - 1)
	firstBid := firstticker.BidPrice
	firstAsk := firstticker.AskPrice
	var maxbid int64 = 0
	var minask int64 = m.tickers[symbol].At(i).AskPrice
	for ; i < m.tickers[symbol].Len(); i++ {
		t := m.tickers[symbol].At(i)
		bid := t.BidPrice
		ask := t.AskPrice
		if bid > maxbid {
			maxbid = bid
		}
		if ask < minask {
			minask = ask
		}
	}
	longpct := float64(maxbid-firstAsk) / float64(firstAsk)
	shortpct := float64(firstBid-minask) / float64(minask)
	maxpct := max(longpct, shortpct)

	if maxpct < threshold {
		return
	}

	if longpct >= shortpct {
		tpPrice := (maxbid * 4999) / 5000
		slPrice := (minask * 4999) / 5000

		msg := arbMsg{
			symbol:  symbol,
			isLong:  true,
			price:   firstAsk,
			tpPrice: tpPrice,
			slPrice: slPrice,
		}
		arbch <- msg
	} else {
		tpPrice := (minask * 5001) / 5000
		slPrice := (maxbid * 5001) / 5000

		msg := arbMsg{
			symbol:  symbol,
			isLong:  false,
			price:   firstBid,
			tpPrice: tpPrice,
			slPrice: slPrice,
		}
		arbch <- msg
	}
}
