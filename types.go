package mqttmgr

import (
	"crypto/tls"
	"time"
)

type ReconnectPolicy struct {
	// initial connect backoff and gatekeeping
	Initial time.Duration
	Max     time.Duration
	Jitter  float64
}

type Will struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
}

type ConnConfig struct {
	Name          string
	Brokers       []string
	ClientID      string
	Username      string
	Password      string
	TLSConfig     *tls.Config
	KeepAlive     time.Duration
	CleanStart    bool
	SessionExpiry time.Duration
	LWT           *Will
	DefaultQoS    byte
	Reconnect     ReconnectPolicy
}

type Message struct {
	Topic     string
	Payload   []byte
	QoS       byte
	Retained  bool
	Timestamp time.Time
}

type PublishReq struct {
	Topic   string
	Payload []byte
	QoS     byte //0/1/2; 0 uses DefaultQoS
	Retain  bool
}

type TopicQoS struct {
	Filter string
	QoS    byte
}

type SubReq struct {
	Topics  []TopicQoS
	Handler func(msg Message)
}

type Interceptor func(Message) bool
type Dispatcher struct {
	workers int
	queue   chan Message
	process func(Message)
}
