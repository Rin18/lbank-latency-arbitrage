package main

import (
	tele "gopkg.in/telebot.v4"
)

type TelegramBotClient struct {
	bot       *tele.Bot
	recipient tele.ChatID
}

func NewtelegramBotClient(token string, chatID int64) (*TelegramBotClient, error) {
	pref := tele.Settings{
		Token: token,
	}
	b, err := tele.NewBot(pref)
	if err != nil {
		return nil, err
	}
	return &TelegramBotClient{
		bot:       b,
		recipient: tele.ChatID(chatID),
	}, nil
}

func (t *TelegramBotClient) Send(msg string) error {
	if len(msg) > 4000 {
		msg = msg[:4000] + "\n...[Truncated]</code></pre>"
	}
	_, err := t.bot.Send(t.recipient, msg, &tele.SendOptions{
		ParseMode: tele.ModeHTML,
	})
	return err
}
