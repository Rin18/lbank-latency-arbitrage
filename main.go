package main

import (
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/Rin18/bookticker-manager/common"
	"github.com/Rin18/place-order-client/lbankfutures"
)

const token = "09eb22f9ccbe40cdbe9d9dd0f7d01e6a"

var symbols = []string{"BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT", "WLFIUSDT", "QTUMUSDT", "LTCUSDT", "ZECUSDT", "BANDUSDT", "BANKUSDT", "BUSDT", "AKEUSDT"}

const threshold = 0.0025

const scale = 8

func main() {
	manager := NewTickerManager()
	orderclient := lbankfutures.NewPlaceOrderClient(token)
	period := 1 * time.Second
	latency := 250 * time.Millisecond
	arbch := make(chan arbMsg, 10)
	statusch := make(chan common.WsStatus, 500)
	var health bool = false
	err := orderclient.Fetch_metadata()
	if err != nil {
		log.Fatalf("Fetch Metadata error: %v", err)
	}
	file, err := os.OpenFile("log.jsonl", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Open file error: %v", err)
	}
	defer file.Close()
	fileLogger := slog.New(slog.NewJSONHandler(file, nil))

	go manager.Start(symbols, period, latency, threshold, arbch, statusch)
	go orderclient.StartIdleRequest()
	go func() {
		for msg := range statusch {
			health = msg.IsHealthy
			fileLogger.Info("Status message received", "Status", msg)
		}
	}()
	for msg := range arbch {
		if !health {
			continue
		}
		symbol := msg.symbol
		if manager.symbolsInfo[symbol].cooldown > 0 {
			continue
		}

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

		volume := int64(20e16) / price
		volume, isEnough, err := orderclient.Format.FormatVolume(symbol, volume)
		if err != nil {
			log.Printf("price format error: %v\n", err)
			continue
		}
		if !isEnough {
			continue
		}
		price_str := common.Parseint64tostring(price, scale)
		volume_str := common.Parseint64tostring(volume, scale)
		tpPrice_str := common.Parseint64tostring(tpPrice, scale)
		slPrice_str := common.Parseint64tostring(slPrice, scale)

		manager.symbolsInfo[symbol].cooldown = 5 * time.Second
		sendRequestTime := time.Now().UnixMicro()
		receipt, err := orderclient.LimitTPSLOrder(symbol, price_str, volume_str, tpPrice_str, slPrice_str, isLong, isIOC)
		if err != nil {
			slog.Error("Place order error", "error", err)
			continue
		}

		fileLogger.Info("LimitTPSLOrder called",
			"tickerReceiveTime", msg.tickerReceiveTime,
			"sendRequestTime", sendRequestTime,
			"symbol", symbol,
			"price_str", price_str,
			"volume", volume_str,
			"tpPrice_str", tpPrice_str,
			"slPrice_str", slPrice_str,
			"isLong", isLong,
			"isIOC", isIOC,
			"rawArbMsg", msg,
			"receipt", receipt,
		)
	}

}
