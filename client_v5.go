package mqttmgr

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

type Client struct {
	name       string
	cfg        ConnConfig
	log        *slog.Logger
	cm         *autopaho.ConnectionManager
	muSubs     sync.RWMutex
	once       sync.Once
	subs       map[string]SubHandler
	defaultQos byte
	closed     chan struct{}
}

type SubHandler struct {
	qos     byte
	handler func(Message)
}

func newV5Client(ctx context.Context, cfg ConnConfig, log *slog.Logger) (*Client, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("no brokers configured for %s", cfg.Name)
	}

	tlsCfg := cfg.TLSConfig

	if needsTLS(cfg.Brokers) && tlsCfg == nil {
		tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	c := &Client{
		name:       cfg.Name,
		cfg:        cfg,
		log:        log,
		subs:       make(map[string]SubHandler),
		defaultQos: chooseQoS(cfg.DefaultQoS),
		closed:     make(chan struct{}),
	}

	uurls := make([]*url.URL, 0, len(cfg.Brokers))
	for _, b := range cfg.Brokers {
		uu, err := url.Parse(b)
		if err != nil {
			return nil, fmt.Errorf("bad broker URL %q: %w", b, err)
		}
		uurls = append(uurls, uu)
	}

	acfg := autopaho.ClientConfig{
		BrokerUrls: uurls,
		TlsCfg:     tlsCfg,
		KeepAlive:  uint16(durationOr(cfg.KeepAlive, 60*time.Second)),
		OnConnectionUp: func(cm *autopaho.ConnectionManager, ca *paho.Connack) {
			log.Info("connection up", "session_present", ca.SessionPresent)

			// Re-subscribe tracked topics
			c.muSubs.RLock()
			defer c.muSubs.RUnlock()
			if len(c.subs) == 0 {
				return
			}

			props := &paho.SubscribeProperties{}
			packets := make([]paho.SubscribeOptions, 0, len(c.subs))

			for filter, sh := range c.subs {
				packets = append(packets, paho.SubscribeOptions{Topic: filter, QoS: sh.qos})
			}

			if !ca.SessionPresent {
				if _, err := cm.Subscribe(ctx, &paho.Subscribe{
					Subscriptions: packets,
					Properties:    props,
				}); err != nil {
					log.Error("resubscribe failed", "err", err)
				} else {
					log.Info("resubscribe", "count", len(packets))
				}
			}
		},
		OnConnectError: func(err error) {
			log.Warn("connect error", "err", err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID: cfg.ClientID,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					c.muSubs.RLock()
					defer c.muSubs.RUnlock()

					for filter, sh := range c.subs {
						if topicMatch(filter, pr.Packet.Topic) {
							// non-blocking
							go sh.handler(Message{
								Topic:     pr.Packet.Topic,
								Payload:   pr.Packet.Payload,
								QoS:       pr.Packet.QoS,
								Retained:  pr.Packet.Retain,
								Timestamp: time.Now(),
							})
						}
					}
					// return false so other handlers in the slice may run too
					return false, nil
				},
			},
		},
	}

	if cfg.Username != "" {
		cfg.Username = acfg.ConnectUsername
		cfg.Password = string(acfg.ConnectPassword)
	}

	if cfg.LWT != nil {
		cfg.LWT = &Will{
			Topic:   acfg.WillMessage.Topic,
			QoS:     acfg.WillMessage.QoS,
			Retain:  acfg.WillMessage.Retain,
			Payload: acfg.WillMessage.Payload,
		}
	}

	// Session options
	acfg.CleanStartOnInitialConnection = cfg.CleanStart
	if cfg.SessionExpiry > 0 {
		acfg.SessionExpiryInterval = uint32(cfg.SessionExpiry / time.Second)
	}

	// Interceptor for incoming messages -> non-blocking dispatch
	//...

	cm, err := autopaho.NewConnection(ctx, acfg)
	if err != nil {
		return nil, err
	}
	c.cm = cm

	// Initial connect with backoff & jitter (Autopaho reconnect afterward)
	if err := c.initialConnectWithBackoff(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) initialConnectWithBackoff(ctx context.Context) error {
	p := c.cfg.Reconnect
	if p.Initial <= 0 {
		p.Initial = 1 * time.Second
	}

	if p.Max <= 0 {
		p.Max = 1 * time.Minute
	}

	if p.Jitter <= 0 {
		p.Jitter = 0.2
	}

	backoff := p.Initial
	for {
		if err := c.cm.AwaitConnection(ctx); err == nil {
			return nil
		} else {
			c.log.Warn("await connection failed; backing off", "delay", backoff, "err", err)
		}
		select {
		case <-time.After(applyJitter(backoff, p.Jitter)):
			if backoff *= 2; backoff > p.Max {
				backoff = p.Max
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *Client) Publish(ctx context.Context, req PublishReq) error {
	if req.QoS == 0 {
		req.QoS = c.defaultQos
	}
	_, err := c.cm.Publish(ctx, &paho.Publish{
		Topic:   req.Topic,
		QoS:     req.QoS,
		Retain:  req.Retain,
		Payload: req.Payload,
	})

	return err
}

func (c *Client) Subscribe(ctx context.Context, req SubReq) error {
	if len(req.Topics) == 0 {
		return fmt.Errorf("no topics to subscribe")
	}

	// Register handlers first (so in flight retained msgs are caught)

	c.muSubs.Lock()
	for _, t := range req.Topics {
		q := t.QoS
		if q == 0 {
			q = c.defaultQos
		}
		c.subs[t.Filter] = SubHandler{qos: q, handler: req.Handler}
	}
	c.muSubs.Unlock()

	subs := make([]paho.SubscribeOptions, 0, len(req.Topics))
	for _, t := range req.Topics {
		q := t.QoS
		if q == 0 {
			q = c.defaultQos
		}
		subs = append(subs, paho.SubscribeOptions{Topic: t.Filter, QoS: q})
	}
	_, err := c.cm.Subscribe(ctx, &paho.Subscribe{Subscriptions: subs})
	return err
}

func (c *Client) Unsubscribe(ctx context.Context, filters ...string) error {
	if len(filters) == 0 {
		return nil
	}

	c.muSubs.Lock()
	for _, f := range filters {
		delete(c.subs, f)
	}

	c.muSubs.Unlock()
	_, err := c.cm.Unsubscribe(ctx, &paho.Unsubscribe{Topics: filters})
	return err

}

func (c *Client) Close(ctx context.Context) error {
	var err error
	c.once.Do(func() {
		close(c.closed)
		err = c.cm.Disconnect(ctx)
	})
	return err
}
