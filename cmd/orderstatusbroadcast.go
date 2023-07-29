package cmd

import (
	"context"
	"fmt"
	socketio "github.com/googollee/go-socket.io"
	"github.com/googollee/go-socket.io/engineio"
	"github.com/googollee/go-socket.io/engineio/transport"
	"github.com/googollee/go-socket.io/engineio/transport/polling"
	"github.com/googollee/go-socket.io/engineio/transport/websocket"
	uuid "github.com/satori/go.uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
	"net/http"
	"socket-io-with-redis-adapter/consumers/orderstatusbroadcast"
)

var orderStatusBroadcastCmd = &cobra.Command{
	Use:   "orderchangednotificationbroadcast",
	Short: "order status stream",
	Long:  `order status stream`,
	RunE:  runOrderStatusBroadcast,
}

func init() {
	RootCmd.AddCommand(orderStatusBroadcastCmd)
}

func runOrderStatusBroadcast(cmd *cobra.Command, args []string) error {

	var configuration config.Configuration

	err := viper.Unmarshal(&configuration)

	if err != nil {
		panic("configuration is invalid!")
	}

	var logger = log.NewLogger()

	var messageBus = rabbitmq.NewRabbitMqClient(
		configuration.RabbitMQ.Host,
		configuration.RabbitMQ.Username,
		configuration.RabbitMQ.Password,
		"",
		rabbitmq.RetryCount(0),
		rabbitmq.PrefetchCount(1))

	ctx := logger.WithCorrelationId(context.Background(), uuid.NewV4().String())

	http.HandleFunc("/v1/healthcheck", func(w http.ResponseWriter, req *http.Request) {
		fmt.Fprintf(w, "Service Up")
	})

	var allowOriginFunc = func(r *http.Request) bool {
		return true
	}

	var server = socketio.NewServer(&engineio.Options{
		Transports: []transport.Transport{
			&polling.Transport{
				CheckOrigin: allowOriginFunc,
			},
			&websocket.Transport{
				CheckOrigin: allowOriginFunc,
			},
		},
	})

	if _, err = server.Adapter(&socketio.RedisAdapterOptions{
		Addr:     configuration.RedisStandAlone.Host,
		Prefix:   configuration.RedisStandAlone.Prefix,
		Password: configuration.RedisStandAlone.Password,
	}); err != nil {
		panic(err)
	}

	handleOrderStatusSocketDisconnect(ctx, server, logger)
	handleOrderStatusSocketError(ctx, server, logger)

	runOrderStatusSocket(ctx, server, logger)
	runOrderTrackingSocket(ctx, server, logger)

	go func() {
		if err = server.Serve(); err != nil {
			panic(err)
		}
	}()

	defer func(server *socketio.Server) {
		if err = server.Close(); err != nil {
			logger.Exception(ctx, "Socket close error", err)
		}
	}(server)

	http.Handle("/socket.io/", server)
	http.Handle("/", http.FileServer(http.Dir("./asset")))

	var broadcastConsumer = orderstatusbroadcast.NewOrderStatusBroadcast(configuration, messageBus, logger, server)
	broadcastConsumer.Construct()

	go func() {
		if err = messageBus.RunConsumers(); err != nil {
			panic(err)
		}
	}()

	return http.ListenAndServe(":5000", nil)
}

func runOrderStatusSocket(ctx context.Context, server *socketio.Server, logger log.Logger) {
	server.OnConnect(orderstatusbroadcast.ORDER_STATUS_NAMESPACE, func(connection socketio.Conn) error {
		connection.SetContext("")
		connection.Join(orderstatusbroadcast.ORDER_STATUS_ROOM)
		logger.Info(ctx, fmt.Sprintf("Order status connection established ConnectionId : %s", connection.ID()))
		return nil
	})
}

func runOrderTrackingSocket(ctx context.Context, server *socketio.Server, logger log.Logger) {
	server.OnConnect(orderstatusbroadcast.ORDER_TRACKING_NAMESPACE, func(connection socketio.Conn) error {
		connection.SetContext("")
		connection.Join(orderstatusbroadcast.ORDER_TRACKING_ROOM)
		logger.Info(ctx, fmt.Sprintf("Order tracking connection established ConnectionId : %s", connection.ID()))
		return nil
	})
}

func handleOrderStatusSocketDisconnect(ctx context.Context, server *socketio.Server, logger log.Logger) {
	server.OnDisconnect("/", func(s socketio.Conn, reason string) {
		s.LeaveAll()
		logger.Info(ctx, fmt.Sprintf("Socket connection closed Reason : %s", reason))

	})
}

func handleOrderStatusSocketError(ctx context.Context, server *socketio.Server, logger log.Logger) {
	server.OnError("/", func(s socketio.Conn, err error) {
		s.LeaveAll()
		logger.Exception(ctx, "Error on socket connection", err)
	})
}
