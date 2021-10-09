package impl

import (
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

func chatHandler(m types.Message, p transport.Packet) error {
	return nil
}

func rumorsHandler(m types.Message, p transport.Packet) error {
	panic("to implement")
}

func statusHandler(m types.Message, p transport.Packet) error {
	panic("to implement")
}

func ackHandler(m types.Message, p transport.Packet) error {
	panic("to implement")
}

func emptyHandler(m types.Message, p transport.Packet) error {
	panic("to implement")
}

func privateHandler(m types.Message, p transport.Packet) error {
	panic("to implement")
}
