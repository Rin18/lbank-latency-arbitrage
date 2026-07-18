package main

import (
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/Rin18/bookticker-manager/common"
	"github.com/Rin18/place-order-client/lbankfutures"
)

const token = "779e05cbfbc34b778b6bdc96e401ed8e"

var symbols = []string{"BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT", "WLFIUSDT", "QTUMUSDT", "LTCUSDT", "ZECUSDT", "BANDUSDT", "BANKUSDT"}

const threshold = 0.0025

const scale = 8

func main() {
	InitLogger()
	manager := NewTickerManager()
	orderclient := lbankfutures.NewPlaceOrderClient(token)
	period := 2 * time.Second
	latency := 250 * time.Millisecond
	arbch := make(chan arbMsg, 10)
	statusch := make(chan common.WsStatus, 500)
	var health bool = false
	err := orderclient.Fetch_metadata()
	if err != nil {
		log.Fatalf("Fetch Metadata error: %v", err)
	}
	go manager.Start(symbols, period, latency, threshold, arbch, statusch)
	go orderclient.StartIdleRequest()
	go func() {
		for msg := range statusch {
			health = msg.IsHealthy
			log.Println(msg)
		}
	}()
	for msg := range arbch {
		if !health {
			continue
		}
		symbol := msg.symbol
		isLong := msg.isLong
		isIOC := false

		price := msg.price
		tpPrice := msg.tpPrice
		slPrice := msg.slPrice

		price, err = orderclient.Format.FormatPrice(symbol, price)
		if err != nil {
			log.Printf("price format error: %v\n", err)
			continue
		}
		tpPrice, err = orderclient.Format.FormatPrice(symbol, tpPrice)
		if err != nil {
			log.Printf("price format error: %v\n", err)
			continue
		}
		slPrice, err = orderclient.Format.FormatPrice(symbol, slPrice)
		if err != nil {
			log.Printf("price format error: %v\n", err)
			continue
		}
		volume, err := orderclient.Format.GetMinVolume(symbol)
		if err != nil {
			log.Printf("price format error: %v\n", err)
			continue
		}
		price_str := common.Parseint64tostring(price, scale)
		tpPrice_str := common.Parseint64tostring(tpPrice, scale)
		slPrice_str := common.Parseint64tostring(slPrice, scale)

		orderclient.LimitTPSLOrder(symbol, price_str, volume, tpPrice_str, slPrice_str, isLong, isIOC)

		// save for analysis
		tickerSnapshot := make([]common.Ticker, 0, manager.tickers[symbol].Len())
		for i := 0; i < manager.tickers[symbol].Len(); i++ {
			tickerSnapshot = append(tickerSnapshot, manager.tickers[symbol].At(i))
		}

		slog.Info("LimitTPSLOrder called",
			"symbol", symbol,
			"price_str", price_str,
			"volume", volume,
			"tpPrice_str", tpPrice_str,
			"slPrice_str", slPrice_str,
			"isLong", isLong,
			"isIOC", isIOC,
			"tickers_snapshot", tickerSnapshot,
		)
	}

}

func InitLogger() *os.File {
	// 打开或创建日志文件，追加模式
	file, err := os.OpenFile("trade_app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic("无法打开日志文件: " + err.Error())
	}

	// 推荐使用 JSON 格式保存到文件，方便后续排查和日志系统收集
	// 如果你更喜欢可读的普通文本，可以换成 slog.NewTextHandler(file, nil)
	handler := slog.NewJSONHandler(file, nil)

	// 替换全局默认的 logger
	logger := slog.New(handler)
	slog.SetDefault(logger)

	return file // 返回 file 以便在 main 函数结束时关闭
}
