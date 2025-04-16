package main

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/worker/clean"
	"github.com/ptourne/sistemas-distribuidos-1/worker/filter"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Worker struct {
	Tasks []task.Task
}

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("worker_%s", WORKER_ID), logger.Debug)

func (w Worker) Run() {
	var conn *amqp.Connection
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	for range 5 {
		if err == nil {
			break
		}
		time.Sleep(5 * time.Second)
		conn, err = amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	}
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
		return
	}
	defer conn.Close()
	log.Infof("Connected to RabbitMQ")
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
		//log.Infof("Received message from %s: %s", task.Input, row.Strings["title"])
		result := task.Process(row)
		if result == nil {
			log.Infof("Row filtered out: %v name: %v", row.Strings["title"], task.Name())
			continue
		}

		buf, err := json.Marshal(result)
		unwrap(err, "Failed to marshal JSON")
		err = ch.Publish(
			task.Name(), // exchange
			"",          // routing key
			false,       // mandatory
			false,       // immediate
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
		log.Fatalf("%s: %s", msg, err)
		panic(err)
	}
}

func newFunction(task task.Task, ch *amqp.Channel) <-chan amqp.Delivery {
	err := ch.ExchangeDeclare(
		task.Input(), // name
		"fanout",     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	unwrap(err, "Failed to declare an exchange")

	inputQueue, err := ch.QueueDeclare(
		task.Name(), // name
		false,       // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)

	unwrap(err, "Failed to declare a queue")

	err = ch.QueueBind(
		inputQueue.Name, // queue name
		"",              // routing key
		task.Input(),    // exchange
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
		task.Name(), // name
		"fanout",    // type
		true,        // durable
		false,       // auto-deleted
		false,       // internal
		false,       // no-wait
		nil,         // arguments
	)
	unwrap(err, "Failed to declare an exchange")
	return msgs
}

type SourceTask struct {
	name string
}

func NewSourceTask(name string) task.Task {
	return &SourceTask{name}
}

func (t *SourceTask) Process(r common.Row) *common.Row {
	return nil
}

func (t *SourceTask) Name() string {
	return t.name
}

func (t *SourceTask) Input() string {
	return ""
}

func NewWorker() Worker {
	movies_metadata := NewSourceTask("movies_metadata")
	credits := NewSourceTask("credits")
	movies_metadata_clean := clean.NewCleanMovies(movies_metadata)
	credits_clean := clean.NewCleanCredits(credits)
	filter_release_date_ge_2000_and_include_ar := filter.NewFilterReleaseDateGe2000AndIncludeAR(movies_metadata_clean)
	filter_release_date_l_2010_and_include_es := filter.NewFilterReleaseDateL2010AndIncludeES(filter_release_date_ge_2000_and_include_ar)
	filter_one_production_country := filter.NewFilterProductionCountriesLen1(movies_metadata_clean)
	return Worker{
		Tasks: []task.Task{
			movies_metadata_clean,
			credits_clean,
			filter_release_date_ge_2000_and_include_ar,
			filter_release_date_l_2010_and_include_es,
			filter_one_production_country,
		},
	}
}
