package mqttmgr

import (
	"math/rand/v2"
	"time"
)

func needsTLS(brokers []string) bool {
	for _, b := range brokers {
		if hasPrefixAny(b, "ssl://", "tls://", "mqtts://", "wss://", "https://") {
			return true
		}
	}
	return false
}

func hasPrefixAny(s string, pref ...string) bool {
	for _, p := range pref {
		if len(s) >= len(p) && s[:len(p)] == p {
			return true
		}
	}
	return false
}

func chooseQoS(q byte) byte {
	switch q {
	case 0, 1, 2:
		return q
	default:
		return 1
	}
}

func durationOr(d, fallback time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return fallback
}

func applyJitter(d time.Duration, frac float64) time.Duration {
	if frac <= 0 {
		return 0
	}

	min := 1 - frac
	max := 1 + frac
	scale := min + rand.Float64()*(max-min)
	return time.Duration(float64(d) * scale)
}

// Very small MQTT wildcard matcher for '#' and '+' in filters.
func topicMatch(filter, topic string) bool {
	// Fast path exact
	if filter == topic {
		return true
	}

	// crude implementationl; adequate for tests
	f := split(filter, '/')
	t := split(topic, '/')
	for i := 0; i < len(f); i++ {
		if i >= len(t) {
			return false
		}
		switch f[i] {
		case "#":
			return true
		case "+":
			// matches any single level
		default:
			if f[i] != t[i] {
				return false
			}
		}
	}
	return len(t) == len(f)
}

func split(s string, sep rune) []string {
	var out []string
	last := 0
	for i, r := range s {
		if r == sep {
			out = append(out, s[last:i])
			last = i + 1
		}
	}
	out = append(out, s[last:])
	return out
}
