/*
Copyright © 2023 Dagu Yota Hamada
*/
package main

import (
	"github.com/mendge/daku/cmd"
	"github.com/mendge/daku/internal/constants"
)

func main() {
	cmd.Execute()
}

var version = "0.0.0"

func init() {
	constants.Version = version
}
