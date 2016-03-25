package main

import (
	//"fmt"
	"github.com/codegangsta/cli"
	"github.com/hpcloud/tail"
	"gopkg.in/Shopify/sarama.v1"
	"log"
	"os"
	"strings"
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
				cli.BoolFlag{Name: "debug"},
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
	var debug = c.Bool("debug")

	var asyncProducer = newAccessLogProducer(address)

	t, err := tail.TailFile(c.String("logdir"), tail.Config{Follow: true})
	for line := range t.Lines {
		//go func(asyncProducer sarama.AsyncProducer) {
		asyncProducer.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(line.Text),
		}
		//}(asyncProducer)
		if debug {
			log.Println(line.Text)
		}
	}
	if err != nil {
		panic(err)
	}

}

func newAccessLogProducer(brokerList []string) sarama.AsyncProducer {

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Flush.Frequency = 500 * time.Millisecond

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
