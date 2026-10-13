package outbox

import "time"

// Recordable is how a delivery error is stored.
var Recordable = recordable

// NextBackoff is Run's wait after a failure, given the wait before it.
func NextBackoff(o Options, last time.Duration) time.Duration {
	return o.withDefaults().nextBackoff(last)
}
