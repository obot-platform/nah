package main

import (
	"os"

	"github.com/obot-platform/nah/pkg/deepcopy"
)

func main() {
	deepcopy.Deepcopy(os.Args[1:]...)
}
