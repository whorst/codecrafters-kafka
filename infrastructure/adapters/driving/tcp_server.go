package driving

import (
	"fmt"
	"io"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/core/domain"
	"github.com/codecrafters-io/kafka-starter-go/core/ports/driving"
)

// TCPServer is a primary adapter (driving adapter) that uses the driving port.
// Rule 2: Adapters use the ports defined by the core.
// Rule 3: Dependencies point inward - this adapter depends on the core port.
type TCPServer struct {
	handler driving.KafkaHandler
	port    string
}

// NewTCPServer creates a new TCP server adapter
func NewTCPServer(handler driving.KafkaHandler, port string) *TCPServer {
	return &TCPServer{
		handler: handler,
		port:    port,
	}
}

// Start starts the TCP server and begins accepting connections
func (s *TCPServer) Start() error {
	l, err := net.Listen("tcp", s.port)
	if err != nil {
		return fmt.Errorf("failed to bind to port %s: %w", s.port, err)
	}
	defer l.Close()

	fmt.Printf("Server listening on %s\n", s.port)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			os.Exit(1)
		}
		go s.handleConnection(conn)
	}
}

func (s *TCPServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		buff := make([]byte, 1024)
		n, err := conn.Read(buff)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Error reading from connection: %v\n", err)
			break
		}

		// Create domain request
		req := domain.Request{
			Data: buff[:n],
		}

		// Call the driving port (core business logic)
		// Rule 3: Adapter depends on and uses the port, pointing inward
		// The handler will route to the appropriate service based on the API key
		resp, err := s.handler.HandleRequest(req)
		if err != nil {
			fmt.Printf("Error handling request: %v\n", err)
			break
		}

		// Write response back to client
		_, err = conn.Write(resp.Data)
		if err != nil {
			fmt.Printf("Error writing response: %v\n", err)
			break
		}
	}
}
