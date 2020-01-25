package calculator

type Args struct {
	A, B int
}

type Calculator int

func (t *Calculator) Multiply(args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}
