package main
import (
	"KafkaAndProbuf/models"
	"fmt"
	"log"
	"os"
	"os/signal"

	cluster "github.com/bsm/sarama-cluster"
)

func main() {

	// init (custom) config, enable errors and notifications
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	// init consumer
	brokers := []string{"bigdatakafka.qtt.prd.1sapp.com:9092"}
	topics := []string{"log_wailaxin"}
	consumer, err := cluster.NewConsumer(brokers, "my-test-consumer-group", topics, config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// consume errors
	go func() {
		for err := range consumer.Errors() {
			fmt.Println("zhai error 000")
			log.Printf("Error: %s\n", err.Error())
			fmt.Println("zhai error 111")

		}
	}()

	// consume notifications
	go func() {
		for ntf := range consumer.Notifications() {
			fmt.Println("zhai notification start")
			log.Printf("Rebalanced: %+v\n", ntf)
			fmt.Println("zhai notification over")

		}
	}()

	// consume messages, watch signals
	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				fmt.Println("zhai get  message start")
				fmt.Println(string(msg.Value))
				fmt.Println("zhai decode probuf:")
				m, _, _, _ :=models.DecodeFromProtobuf(msg.Value)
				fmt.Println(m)
				fmt.Println(m["device_identity"])
				fmt.Println(m["adgroup"])

				//fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				consumer.MarkOffset(msg, "")    // mark message as processed
				fmt.Println("zhai get  message over")
				return
			}
		case <-signals:
			return
		}
	}
}
