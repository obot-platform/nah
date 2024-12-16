package main

import (
	"os"

	"github.com/acorn-io/nah/pkg/deepcopy"
)

func main() {
	deepcopy.Deepcopy(os.Args[1:]...)
}
