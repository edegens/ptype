package prime

import (
	"time"
)

type Args struct {
	Min    int
	Max    int
	Target int
}

type Prime int

func (t *Prime) Check(args *Args, reply *int) error {
	for i := args.Min; i < min(args.Max, args.Target); i++ {
		time.Sleep(250 * time.Millisecond)
		if i != 0 && args.Target%i == 0 {
			*reply = i
			return nil
		}
	}
	*reply = args.Target
	return nil
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
