package cos418_hw1_1

import (
	"bufio"
	"io"
	"os"
	"strconv"
)

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
	sum := 0
	for {
		num, openC := <-nums
		if openC {
			sum += num
		} else {
			out <- sum
			break
		}
	}
}

type Worker struct {
	inputChannel  chan int
	outputChannel chan int
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {

	//Define the workers
	maxWorkers := num
	var workers []Worker
	for i := 0; i < maxWorkers; i++ {
		workers = append(workers, Worker{make(chan int), make(chan int)})
	}
	master := Worker{make(chan int), make(chan int)}

	//Read the numbers from the file
	f, err := os.Open(fileName)
	checkError(err)
	numbers, _ := readInts(f)
	f.Close()

	//Make the workers and master work
	for _, worker := range workers {
		go sumWorker(worker.inputChannel, worker.outputChannel)
	}
	go sumWorker(master.inputChannel, master.outputChannel)

	//Delegate work to workers
	for i, num := range numbers {
		workerId := i % maxWorkers
		workers[workerId].inputChannel <- num
	}

	//Close the channels and call master worker
	for _, worker := range workers {
		close(worker.inputChannel)
		a := <-worker.outputChannel
		master.inputChannel <- a
	}
	close(master.inputChannel)

	//Final Sum of output of workers
	sum := <-(master.outputChannel)
	return sum
}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		val, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return elems, err
		}
		elems = append(elems, val)
	}
	return elems, nil
}
