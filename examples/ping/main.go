package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/wanasyraf96/mqttmgr"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	mgr := mqttmgr.New(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	broker := "mqtt://localhost:1883"
	clientId := fmt.Sprintf("demo-client-%d", time.Now().UnixNano())

	err := mgr.Add(ctx, mqttmgr.ConnConfig{
		Name:          "primary",
		Brokers:       []string{broker},
		ClientID:      clientId,
		Username:      "",
		Password:      "",
		CleanStart:    false,
		SessionExpiry: 30 * time.Minute,
		DefaultQoS:    1,
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
			Certificates:       []tls.Certificate{},
		},
		Reconnect: mqttmgr.ReconnectPolicy{
			Initial: 1 * time.Second,
			Max:     30 * time.Second,
			Jitter:  0.2,
		},
	})
	Check(err)
	Check(mgr.Subscribe(ctx, "primary", mqttmgr.SubReq{
		Topics: []mqttmgr.TopicQoS{{Filter: "demo/+", QoS: 1}},
		Handler: func(msg mqttmgr.Message) {
			logger.Info("received", "topic", msg.Topic, "len", len(msg.Payload), "qos", msg.QoS, "retained", msg.Retained)
		},
	}))

	go func() {
		t := time.NewTicker(1 * time.Second)
		defer t.Stop()
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				i++
				payload := fmt.Appendf(nil, "hello %d", i)
				err := mgr.Publish(ctx, "primary", mqttmgr.PublishReq{
					Topic:   "demo/ping",
					Payload: payload,
					QoS:     1,
					Retain:  false,
				})
				if err != nil {
					logger.Warn("publish error", "err", err)
				}
			}
		}
	}()
	<-ctx.Done()
	_ = mgr.Shutdown(context.Background())
}

func Check(err error) {
	if err != nil {
		panic(err)
	}
}
