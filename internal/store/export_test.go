package store

// SetHook lets a test run code at named points of a PostEntry attempt: "snapshot" (after the first statement),
// "locked" (after the accounts are locked and checked) and "commit" (just before the commit).
func SetHook(s *Store, hook func(point string, attempt int)) { s.hook = hook }
