//go:build !faults

package faults

// Enabled reports whether this binary was built with the faults tag, and so whether a point can crash it.
const Enabled = false

// Arm checks spec and arms nothing, since without the faults tag no point can crash: a spec that names a point is
// ErrNotBuilt.
func Arm(spec string) (map[string]Rule, error) {
	rules, err := Parse(spec)
	if err != nil {
		return nil, err
	}
	if len(rules) > 0 {
		return nil, ErrNotBuilt
	}
	return rules, nil
}

// Point does nothing in a build without the faults tag.
func Point(string) {}
