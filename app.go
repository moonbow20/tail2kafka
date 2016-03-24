package main

import (
	//"fmt"
	"gopkg.in/Shopify/sarama.v1"
	"github.com/hpcloud/tail"
	"github.com/codegangsta/cli"
	"os"
	"strings"
	"log"
	"time"
)

func main() {
	app := cli.NewApp()
	app.Name = "tail2kafka"
	app.Version = "0.1"
	app.Usage = "Tail a file (like a log file) and send the output to a Kafka topic"
	app.EnableBashCompletion = true
	app.Commands = []cli.Command{
		{
			Name:      "tail",
			ShortName: "t",
			Usage:     "tail log file and send to kafka",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "logdir", Value: "/var/log/apache2/access_log", Usage: "log file (absolute path)"},
				cli.StringFlag{Name: "server", Value: "", Usage: "Kafka server location with port `localhost:9092`"},
				cli.StringFlag{Name: "topic", Value: "apache", Usage: "Kafka queue topic"},
			},
			Action: func(c *cli.Context) {
				run(c)
			},
		},
	}
	app.Run(os.Args)
}

func run(c *cli.Context) {
	var address = strings.Split(c.String("server"), ",")
	var topic = c.String("topic")

	var asyncProducer = newAccessLogProducer(address)

	t, err := tail.TailFile(c.String("logdir"), tail.Config{Follow: true})
	for line := range t.Lines {
		asyncProducer.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(line.Text),
		}
	}
	if err != nil {
		panic(err)
	}


}

func newAccessLogProducer(brokerList []string) sarama.AsyncProducer {

	// For the access log, we are looking for AP semantics, with high throughput.
	// By creating batches of compressed messages, we reduce network I/O at a cost of more latency.
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

	producer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	go func() {
		for err := range producer.Errors() {
			log.Println("Failed to write access log entry:", err)
		}
	}()

	return producer
}
