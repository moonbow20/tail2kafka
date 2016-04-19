package tail2kafka

import (
	"log"
	"io/ioutil"
	"fmt"
)

type Tail struct {

}

func a() {
	files, err := ioutil.ReadDir("/home/daisy")
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		if file.IsDir() == false {
			fmt.Println(file.Name())
		}
	}
}