package scaler

import (
	"context"
	"time"

	"gitlab.com/dunn.dev/forgehand/internal/fleet"
)

// State is where a slot is in its life. A slot only moves along allowed.
type State int

const (
	Acquiring State = iota
	Starting
	Listening
	Reaping
	Adopting
	Busy
	Cancelled
	Failed
	Done
)

var stateNames = [...]string{"ACQUIRING", "STARTING", "LISTENING", "REAPING", "ADOPTING", "BUSY", "CANCELLED", "FAILED", "DONE"}

func (s State) String() string { return stateNames[s] }

// Terminal reports whether a slot in this state has released its VM for good.
func (s State) Terminal() bool { return s >= Cancelled }

var allowed = map[State][]State{
	Acquiring: {Starting, Cancelled, Failed},
	Starting:  {Listening, Cancelled, Failed},
	Listening: {Busy, Reaping, Cancelled, Failed},
	Reaping:   {Busy, Cancelled, Done},
	Adopting:  {Busy, Reaping, Cancelled, Done},
	Busy:      {Reaping, Cancelled, Done},
}

type slot struct {
	key    string
	name   string
	seq    int
	state  State
	since  time.Time
	cancel context.CancelFunc
	inst   fleet.Instance

	acquired  bool
	reaping   bool
	timedOut  bool
	exited    bool
	completed bool
	lost      bool

	firstEnd time.Time
	// probeAt is the earliest a lost BUSY slot asks GitHub about its job again.
	probeAt time.Time
}
