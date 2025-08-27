package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type InputBuffer struct {
    buffer string
}

func NewInputBuffer() *InputBuffer {
    return &InputBuffer{}
}

func printPrompt() {
	fmt.Print("db > ")
}

func (ib *InputBuffer) readInput(reader *bufio.Reader) error {
    input, err := reader.ReadString('\n')
    if err != nil {
        return fmt.Errorf("Error reading input: %v", err)
    }

    // Remove the trailing newline
    ib.buffer = strings.TrimRight(input, "\n\r")
    return nil
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	inputBuffer := NewInputBuffer()

	for {
		printPrompt()

		err := inputBuffer.readInput(reader)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		if inputBuffer.buffer == ".exit" {
			fmt.Println("Bye!")
			os.Exit(0)
		} else {
			fmt.Printf("Unrecognized command '%s'.\n", inputBuffer.buffer)
		}
	}
}
