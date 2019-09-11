package mapreduce

import (
	"encoding/json"
	"os"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {

	//Read the keyValues of reducer on data structure
	jsonMap := make(map[string][]KeyValue)
	for m := 0; m < nMap; m++ {

		filename := reduceName(jobName, m, reduceTaskNumber)

		file, err := os.Open(filename)
		checkError(err)

		var kv []KeyValue
		encoder := json.NewDecoder(file)
		err = encoder.Decode(&kv)
		checkError(err)

		jsonMap[filename] = kv
	}

	//Create the encode data
	reduceMap := make(map[string][]string)
	for _, kvs := range jsonMap {
		for _, kv := range kvs {
			reduceMap[kv.Key] = append(reduceMap[kv.Key], kv.Value)
		}
	}

	//Create the output file
	outputFile := mergeName(jobName, reduceTaskNumber)
	file, err := os.OpenFile(outputFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
	checkError(err)
	encoder := json.NewEncoder(file)

	//Call the reducer with the actual data
	for key, values := range reduceMap {
		res := reduceF(key, values)
		encoder.Encode(KeyValue{key, res})
	}

	file.Close()
}
