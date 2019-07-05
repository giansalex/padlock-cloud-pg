package main

import "os"
import "log"

import "github.com/giansalex/padlock-cloud-pg/padlockcloud"

func main() {
	err := padlockcloud.NewCliApp().Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
