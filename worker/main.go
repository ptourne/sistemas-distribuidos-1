package main

import "github.com/ptourne/sistemas-distribuidos-1/worker/worker"

func main() {
	worker.NewWorker().Run()
}
