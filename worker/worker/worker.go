package worker

import (
	"encoding/json"
	"log"
	"reflect"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Worker struct {
	Tasks []Task
}

func (w Worker) Run() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()

	cases := make([]reflect.SelectCase, len(w.Tasks))
	for i, task := range w.Tasks {
		inputChannel := newFunction(task, ch)
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(inputChannel),
		}
	}
	for {
		i, val, ok := reflect.Select(cases)
		if !ok {
			panic("Channel closed")
		}
		delivery, ok := val.Interface().(amqp.Delivery)
		if !ok {
			panic("Failed to cast to amqp.Delivery")
		}
		blob := delivery.Body
		var row common.Row
		err = json.Unmarshal(blob, &row)
		unwrap(err, "Failed to unmarshal JSON")
		task := w.Tasks[i]
		result := task.operation.Process(row)
		if result == nil {
			log.Println("Row filtered out")
			continue
		}
		buf, err := json.Marshal(result)
		unwrap(err, "Failed to marshal JSON")
		err = ch.Publish(
			task.name, // exchange
			"",        // routing key
			false,     // mandatory
			false,     // immediate
			amqp.Publishing{
				ContentType: "text/json",
				Body:        buf,
			})
		unwrap(err, "Failed to publish a message")
		err = delivery.Ack(false)
		unwrap(err, "Failed to ack message")
	}
}

func unwrap(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func newFunction(task Task, ch *amqp.Channel) <-chan amqp.Delivery {
	err := ch.ExchangeDeclare(
		task.input, // name
		"fanout",   // type
		true,       // durable
		false,      // auto-deleted
		false,      // internal
		false,      // no-wait
		nil,        // arguments
	)
	unwrap(err, "Failed to declare an exchange")

	inputQueue, err := ch.QueueDeclare(
		task.name, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)

	unwrap(err, "Failed to declare a queue")

	err = ch.QueueBind(
		inputQueue.Name, // queue name
		"",              // routing key
		task.input,      // exchange
		false,
		nil,
	)
	unwrap(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
		inputQueue.Name, // queue
		"",              // consumer
		false,           // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)
	unwrap(err, "Failed to register a consumer")

	err = ch.ExchangeDeclare(
		task.name, // name
		"fanout",  // type
		true,      // durable
		false,     // auto-deleted
		false,     // internal
		false,     // no-wait
		nil,       // arguments
	)
	unwrap(err, "Failed to declare an exchange")
	return msgs
}

func NewWorker() Worker {
	return Worker{
		Tasks: []Task{
			Task{
				input:     "movies_metadata",
				name:      "filter_release_date_ge_2000_and_include_argentina",
				operation: FilterReleaseDateGe2000AndIncludeAR{},
			},
		},
	}
}
