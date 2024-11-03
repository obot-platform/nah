package main

import (
	"os"

	"github.com/otto8-ai/nah/pkg/deepcopy"
)

func main() {
	deepcopy.Deepcopy(os.Args[1:]...)
}
