package main

import (
	"fmt"
	"log"
	"time"

	"github.com/Rin18/bookticker-manager/binance/binancefutures"
	"github.com/Rin18/bookticker-manager/common"
	"github.com/Rin18/lbank-latency-arbitrage/datahandler"
	"github.com/Rin18/place-order-client/lbankfutures"
)

const scale = 8

func main() {
	config, err := ReadConfig("config.toml")
	if err != nil {
		log.Fatalf("Read Config Error: %v", err)
	}
	telegramBotClient, err := NewtelegramBotClient(config.TelegramToken, config.TelegramChatID)
	if err != nil {
		log.Fatalf("Telegram Bot Client initialize error: %v", err)
	}
	binanceClient := binancefutures.NewWsClient()

	lbankClient := lbankfutures.NewPlaceOrderClient(config.LbankToken)
	err = lbankClient.Fetch_metadata()
	if err != nil {
		log.Fatalf("lbank fetch metadata error: %v", err)
	}
	go lbankClient.StartIdleRequest()

	binanceTickerCh := make(chan common.Ticker, 2000)
	statusCh := make(chan common.WsStatus, 100)
	go binanceClient.Start(config.Symbols, time.Duration(config.WsTimeOut)*time.Millisecond, binanceTickerCh, statusCh)
	healthy := false
	arbMsgch := make(chan datahandler.ArbitrageMsg, 10)
	symbolsManager := NewSymbolsManager(config.Symbols, config.SlowWindowLengthms, config.FastWindowLengthms, arbMsgch, config.ArbitrageThreshold)

	go func() {
		for msg := range statusCh {
			healthy = msg.IsHealthy
			log.Printf("Receive Binance Websocket Status: %v\n", msg)
		}
	}()
	go func() {
		for tickerMsg := range binanceTickerCh {
			symbolsManager.ProcessTicker(tickerMsg)
		}
	}()

	lastOrderTime := time.Now()
	for arbMsg := range arbMsgch {
		if !healthy {
			continue
		}
		if time.Since(lastOrderTime) < 5*time.Second {
			continue
		}
		symbol := arbMsg.Symbol
		isLong := arbMsg.IsLong
		price := arbMsg.EntryPrice
		tpPrice := arbMsg.ExitPrice
		slPrice := 2*price - tpPrice
		isIOC := config.IsIOC
		volume := (config.Volume * 1e16) / price
		// format
		price, err := lbankClient.Format.FormatPrice(symbol, price)
		if err != nil {
			log.Printf("Format price error: %v\n", err)
			continue
		}
		tpPrice, err = lbankClient.Format.FormatPrice(symbol, tpPrice)
		if err != nil {
			log.Printf("Format price error: %v\n", err)
			continue
		}
		slPrice, err = lbankClient.Format.FormatPrice(symbol, slPrice)
		if err != nil {
			log.Printf("Format price error: %v\n", err)
			continue
		}
		volume, isEnough, err := lbankClient.Format.FormatVolume(symbol, volume)
		if err != nil {
			log.Printf("Format volume error: %v\n", err)
			continue
		}
		if !isEnough {
			log.Printf("volume %v less than minimum required\n", volume)
			continue
		}
		// convert to string
		price_str := common.Parseint64tostring(price, scale)
		volume_str := common.Parseint64tostring(volume, scale)
		tpPrice_str := common.Parseint64tostring(tpPrice, scale)
		slPrice_str := common.Parseint64tostring(slPrice, scale)

		lastOrderTime = time.Now()
		sendRequestTime := time.Now().UnixMicro()
		receipt, err := lbankClient.LimitTPSLOrder(symbol, price_str, volume_str, tpPrice_str, slPrice_str, isLong, isIOC)
		if err != nil {
			log.Printf("Send LimitTPSLOrder error: %v\n", err)
			continue
		}
		tgMsg := fmt.Sprintf("Order Sent\n"+
			"Sent Time: %d"+
			"receipt: %v", sendRequestTime, receipt)
		go telegramBotClient.Send(tgMsg)
	}
}

type SymbolsManager struct {
	symbolManagers map[string]*datahandler.SymbolManager
}

func NewSymbolsManager(symbols []string, slowWindowLengthms int64, fastWindowLengthms int64, arbMsgCh chan<- datahandler.ArbitrageMsg, arbitrageThreshold float64) *SymbolsManager {
	symbolManagers := make(map[string]*datahandler.SymbolManager)
	for _, symbol := range symbols {
		symbolManagers[symbol] = datahandler.NewSymbolManager(symbol, slowWindowLengthms*1000, fastWindowLengthms*1000, arbMsgCh, arbitrageThreshold, 8000)
	}
	return &SymbolsManager{
		symbolManagers: symbolManagers,
	}
}

func (ssm *SymbolsManager) ProcessTicker(ticker common.Ticker) {
	symbol := ticker.Symbol
	timestamp := ticker.ReceiveTime
	ssm.symbolManagers[symbol].Update(timestamp, ticker.BidPrice, ticker.AskPrice)
	ssm.symbolManagers[symbol].ArbitrageCheck()
}
