package mqttmgr

import "log/slog"

func NewDispatcher(workers, queueSize int, process func(Message)) *Dispatcher {
	d := &Dispatcher{
		workers: workers,
		queue:   make(chan Message, queueSize),
		process: process,
	}
	d.start()
	return d
}

func (d *Dispatcher) start() {
	for i := 0; i < d.workers; i++ {
		go func() {
			for msg := range d.queue {
				d.process(msg)
			}
		}()
	}
}

func (d *Dispatcher) Dispatch(msg Message) {
	select {
	case d.queue <- msg:
	default:
		slog.Warn("message dropped (queue full)", "topic", msg.Topic)
	}
}
