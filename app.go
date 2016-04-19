package main

import (
	"github.com/codegangsta/cli"
	"gopkg.in/Shopify/sarama.v1"
	"log"
	"os"
	"time"
	"strings"
	"sync"
	"fmt"
	"github.com/hpcloud/tail"
	"io/ioutil"
)

func main() {
	app := cli.NewApp()
	app.Name = "tail2kafka"
	app.Version = "0.1"
	app.Usage = "Tail a file and send the output to a Kafka topic"
	app.EnableBashCompletion = true
	app.Commands = []cli.Command{
		{
			Name:      "tail",
			ShortName: "t",
			Usage:     "tail log file and send to kafka",
			Flags: []cli.Flag{
				cli.BoolFlag{Name: "debug"},
				cli.StringFlag{Name: "logdir", Value: "/var/log/apache2", Usage: "log file absolute path"},
				cli.StringFlag{Name: "filename", Value: "access_log.*", Usage: "log filename (pattern)"},
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

func watcher(logDir string) {
	files, err := ioutil.ReadDir(logDir)
	if err != nil {
		log.Fatal(err)
	}
	fileList := []string{}
	for _, file := range files {
		if file.IsDir() == false {
			fileList = append(fileList, file.Name())
		}
	}
	for _, file := range fileList {
		fmt.Println(file)
	}

}

func tailingFile(file string )(*tail.Tail) {

	t, err := tail.TailFile(file, tail.Config{Follow: true})
	if err != nil {
		panic(err)
	}
	return t
}

func run(cli *cli.Context) {
	var wg sync.WaitGroup

	var logDir = cli.String("logdir")
	queue := make(chan tail.Tail)
	defer close(queue)

	wg.Add(1)
	go func(server string, topic string, Tail *tail.Tail) {
		defer wg.Done()
		var address = strings.Split(server, ",")
		var asyncProducer = newAccessLogProducer(address)
		defer asyncProducer.Close()

		for line := range Tail.Lines {
			asyncProducer.Input() <- &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(line.Text),
			}
		}
	}(cli.String("server"), cli.String("topic"), nil)

	wg.Add(1)
	go func() {
		defer wg.Done()
		watcher(logDir)
	}()
	wg.Wait()
	fmt.Println("Done")
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

