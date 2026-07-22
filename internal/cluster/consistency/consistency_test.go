package consistency

import "testing"

func TestRequiredAcks_ONE(t *testing.T) {
	cases := []struct {
		rf   int
		want int
	}{
		{1, 1},
		{2, 1},
		{3, 1},
		{5, 1},
	}
	for _, tc := range cases {
		got := ONE.RequiredAcks(tc.rf)
		if got != tc.want {
			t.Errorf("ONE.RequiredAcks(%d) = %d, want %d", tc.rf, got, tc.want)
		}
	}
}

func TestRequiredAcks_QUORUM(t *testing.T) {
	cases := []struct {
		rf   int
		want int
	}{
		{1, 1},
		{2, 2},
		{3, 2},
		{4, 3},
		{5, 3},
		{6, 4},
	}
	for _, tc := range cases {
		got := QUORUM.RequiredAcks(tc.rf)
		if got != tc.want {
			t.Errorf("QUORUM.RequiredAcks(%d) = %d, want %d", tc.rf, got, tc.want)
		}
	}
}

func TestRequiredAcks_ALL(t *testing.T) {
	cases := []struct {
		rf   int
		want int
	}{
		{1, 1},
		{2, 2},
		{3, 3},
		{5, 5},
	}
	for _, tc := range cases {
		got := ALL.RequiredAcks(tc.rf)
		if got != tc.want {
			t.Errorf("ALL.RequiredAcks(%d) = %d, want %d", tc.rf, got, tc.want)
		}
	}
}

func TestRequiredAcks_ZeroReplicationFactor(t *testing.T) {
	// replicationFactor < 1 is treated as 1
	if got := ONE.RequiredAcks(0); got != 1 {
		t.Errorf("ONE.RequiredAcks(0) = %d, want 1", got)
	}
	if got := QUORUM.RequiredAcks(0); got != 1 {
		t.Errorf("QUORUM.RequiredAcks(0) = %d, want 1", got)
	}
	if got := ALL.RequiredAcks(0); got != 1 {
		t.Errorf("ALL.RequiredAcks(0) = %d, want 1", got)
	}
}

func TestConsistencyLevelString(t *testing.T) {
	cases := []struct {
		level ConsistencyLevel
		want  string
	}{
		{ONE, "ONE"},
		{QUORUM, "QUORUM"},
		{ALL, "ALL"},
		{ConsistencyLevel(99), "UNKNOWN"},
	}
	for _, tc := range cases {
		if got := tc.level.String(); got != tc.want {
			t.Errorf("ConsistencyLevel(%d).String() = %q, want %q", tc.level, got, tc.want)
		}
	}
}

func TestDefaultConsistencyLevels(t *testing.T) {
	if DefaultReadConsistency != ONE {
		t.Errorf("DefaultReadConsistency = %v, want ONE", DefaultReadConsistency)
	}
	if DefaultWriteConsistency != QUORUM {
		t.Errorf("DefaultWriteConsistency = %v, want QUORUM", DefaultWriteConsistency)
	}
}
