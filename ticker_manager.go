package main

import (
	"time"

	"github.com/Rin18/bookticker-manager/binance/binancefutures"
	"github.com/Rin18/bookticker-manager/common"
	"github.com/gammazero/deque"
)

type TickerManager struct {
	symbolsInfo map[string]*SymbolInfo
	wsClient    *binancefutures.WsClient
}

type SymbolInfo struct {
	recentTickers *deque.Deque[common.Ticker]
	cooldown      time.Duration
}

func NewTickerManager() *TickerManager {
	wsClient := binancefutures.NewWsClient()
	symbolsInfo := make(map[string]*SymbolInfo)
	for _, symbol := range symbols {
		symbolsInfo[symbol] = &SymbolInfo{
			recentTickers: new(deque.Deque[common.Ticker]),
		}
	}

	tickerManager := TickerManager{
		symbolsInfo: symbolsInfo,
		wsClient:    wsClient,
	}
	return &tickerManager
}

type arbMsg struct {
	tickerReceiveTime int64
	symbol            string
	isLong            bool
	price             int64
	tpPrice           int64
	slPrice           int64
}

func (m *TickerManager) Start(symbols []string, period time.Duration, latency time.Duration, threshold float64, arbch chan<- arbMsg, statusch chan<- common.WsStatus) {
	ch := make(chan common.Ticker, 1000)
	go m.wsClient.Start(symbols, 1*time.Second, ch, statusch)
	for ticker := range ch {
		m.processUpdate(ticker, period)
		m.calculateArbitrage(ticker.Symbol, latency, threshold, arbch, ticker.ReceiveTime)
	}
}

// push the latest ticker to deque and pop the ones ealier than period
func (m *TickerManager) processUpdate(ticker common.Ticker, period time.Duration) {
	symbol := ticker.Symbol
	timestamp := time.Now().Add(-1 * period).UnixMicro()
	m.symbolsInfo[symbol].recentTickers.PushBack(ticker)
	for m.symbolsInfo[symbol].recentTickers.Len() > 0 {
		if m.symbolsInfo[symbol].recentTickers.Front().ReceiveTime < timestamp {
			m.symbolsInfo[symbol].recentTickers.PopFront()
		} else {
			break
		}
	}
}

func (m *TickerManager) calculateArbitrage(symbol string, latency time.Duration, threshold float64, arbch chan<- arbMsg, tickerReceiveTime int64) {
	timestamp := time.Now().Add(-1 * latency).UnixMicro()
	i := 0
	for ; i < m.symbolsInfo[symbol].recentTickers.Len(); i++ {
		t := m.symbolsInfo[symbol].recentTickers.At(i)
		if t.ReceiveTime > timestamp {
			break
		}
	}
	if i >= m.symbolsInfo[symbol].recentTickers.Len()-1 || i == 0 {
		return
	}

	firstticker := m.symbolsInfo[symbol].recentTickers.At(i - 1)
	firstBid := firstticker.BidPrice
	firstAsk := firstticker.AskPrice
	var maxbid int64 = 0
	var minask int64 = m.symbolsInfo[symbol].recentTickers.At(i).AskPrice
	for ; i < m.symbolsInfo[symbol].recentTickers.Len(); i++ {
		t := m.symbolsInfo[symbol].recentTickers.At(i)
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
		slPrice := 2*firstAsk - tpPrice

		msg := arbMsg{
			tickerReceiveTime: tickerReceiveTime,
			symbol:            symbol,
			isLong:            true,
			price:             firstAsk,
			tpPrice:           tpPrice,
			slPrice:           slPrice,
		}
		arbch <- msg
	} else {
		tpPrice := (minask * 5001) / 5000
		slPrice := 2*firstBid - tpPrice

		msg := arbMsg{
			tickerReceiveTime: tickerReceiveTime,
			symbol:            symbol,
			isLong:            false,
			price:             firstBid,
			tpPrice:           tpPrice,
			slPrice:           slPrice,
		}
		arbch <- msg
	}
}
