package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// runScript executes the database with a series of commands and returns the output
func runScript(commands []string) ([]string, error) {
	// Build the executable
	buildCmd := exec.Command("go", "build", "-o", "testdb", "main.go")
	if err := buildCmd.Run(); err != nil {
		return nil, err
	}
	defer os.Remove("testdb")  // Clean up after test
	defer os.Remove("test.db") // Clean up database file

	// Run the database with a temporary filename
	cmd := exec.Command("./testdb", "test.db")

	// Create pipes for stdin and stdout
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	// Send commands
	for _, command := range commands {
		io.WriteString(stdin, command+"\n")
	}
	stdin.Close()

	// Read output
	output, err := io.ReadAll(stdout)
	if err != nil {
		return nil, err
	}

	// Wait for command to finish
	cmd.Wait()

	// Split output into lines and remove empty lines
	lines := strings.Split(string(output), "\n")
	var result []string
	for _, line := range lines {
		if line != "" {
			result = append(result, line)
		}
	}

	return result, nil
}

func TestInsertAndRetrieveRow(t *testing.T) {
	commands := []string{
		"insert 1 user1 person1@example.com",
		"select",
		".exit",
	}

	result, err := runScript(commands)
	if err != nil {
		t.Fatalf("Failed to run script: %v", err)
	}

	expected := []string{
		"db > Executed.",
		"db > (1, user1, person1@example.com)",
		"Executed.",
		"db > Bye!",
	}

	if !equalSlices(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestNegativeID(t *testing.T) {
	commands := []string{
		"insert -1 cstack foo@bar.com",
		"select",
		".exit",
	}

	result, err := runScript(commands)
	if err != nil {
		t.Fatalf("Failed to run script: %v", err)
	}

	if result[0] != "db > ID must be positive." {
		t.Errorf("Expected 'db > ID must be positive.', got '%s'", result[0])
	}

	// Select should show no rows
	if result[1] != "db > Executed." {
		t.Errorf("Expected 'db > Executed.', got '%s'", result[1])
	}
}

func TestTableFull(t *testing.T) {
	var commands []string

	// Insert enough rows to trigger table full condition
	// With B-tree structure, we need to fill all TABLE_MAX_PAGES (100 pages)
	// Each leaf node holds about 13 rows, but we also need internal nodes
	// Insert 1200 rows which should be enough to use up all pages
	for i := 1; i <= 1200; i++ {
		commands = append(commands,
			fmt.Sprintf("insert %d user%d person%d@example.com", i, i, i))
	}
	commands = append(commands, ".exit")

	result, err := runScript(commands)
	if err != nil {
		t.Fatalf("Failed to run script: %v", err)
	}

	// Check that the last insert failed with table full error
	// Look for "Error: Table full." in the output
	foundTableFull := false
	for _, line := range result {
		if strings.Contains(line, "Error: Table full.") {
			foundTableFull = true
			break
		}
	}

	if !foundTableFull {
		t.Errorf("Expected to find 'Error: Table full.' in output, but got: %v", result)
	}
}

func TestMaxLengthStrings(t *testing.T) {
	longUsername := strings.Repeat("a", 32)
	longEmail := strings.Repeat("a", 255)

	commands := []string{
		fmt.Sprintf("insert 1 %s %s", longUsername, longEmail),
		"select",
		".exit",
	}

	result, err := runScript(commands)
	if err != nil {
		t.Fatalf("Failed to run script: %v", err)
	}

	expected := []string{
		"db > Executed.",
		fmt.Sprintf("db > (1, %s, %s)", longUsername, longEmail),
		"Executed.",
		"db > Bye!",
	}

	if !equalSlices(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// Helper function to compare slices
func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
