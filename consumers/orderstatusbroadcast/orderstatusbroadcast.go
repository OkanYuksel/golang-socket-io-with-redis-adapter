package orderstatusbroadcast

import (
	"context"
	"encoding/json"
	"fmt"
	socketio "github.com/googollee/go-socket.io"
	uuid "github.com/satori/go.uuid"
	"log"
)

const ORDER_STATUS_NAMESPACE = "/namespace_orderstatus"
const ORDER_STATUS_ROOM = "room_update_order_status"

type OrderStatusBroadcast struct {
	logger        log.Logger
	configuration config.Configuration
	messageBus    rabbitmq.Client
	socket        *socketio.Server
}

func NewOrderStatusBroadcast(configuration config.Configuration, messageBus rabbitmq.Client, logger log.Logger, socket *socketio.Server) *OrderStatusBroadcast {

	return &OrderStatusBroadcast{
		logger:        logger,
		messageBus:    messageBus,
		configuration: configuration,
		socket:        socket,
	}
}

func (self *OrderStatusBroadcast) Construct() {

	self.messageBus.AddConsumer("Out.Socket.BroadcastOrderStatus").
		SubscriberExchange("*", rabbitmq.Topic, "Events.V1:OrderStatusChangedNotificationEvent").
		HandleConsumer(self.broadcastOrderStatusConsume())

}

func (self *OrderStatusBroadcast) broadcastOrderStatusConsume() func(message rabbitmq.Message) error {

	return func(message rabbitmq.Message) error {

		var (
			eventMessage event.OrderStatusChangedNotificationEvent
			err          error
		)

		ctx := self.logger.WithCorrelationId(context.Background(), uuid.NewV4().String())

		if err = json.Unmarshal(message.Payload, &eventMessage); err != nil {
			return err
		}

		self.logger.Info(ctx, fmt.Sprintf("%s order status updated Status : %s", eventMessage.OrderNumber, eventMessage.Status))

		self.socket.BroadcastToRoom(ORDER_STATUS_NAMESPACE, ORDER_STATUS_ROOM, ORDER_STATUS_ROOM, eventMessage)

		return nil
	}
}
