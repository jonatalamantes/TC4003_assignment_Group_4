package mapreduce

import (
    "encoding/json"
    "hash/fnv"
    "io/ioutil"
    "os"
    //"regexp"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {

    //Read the inFile
    file, err := ioutil.ReadFile(inFile)
    checkError(err)
    fileContent := string(file)

    //Create mapF keyValues output
    var mapValues []KeyValue
    mapValues = mapF(inFile, fileContent)

    //Create the structures of the json format
    jsonMap := make(map[string][]KeyValue)
    for _, v := range mapValues {

        //Get the index of the reduce worker and file name
        reduceIndex := int(ihash(v.Key)) % nReduce
        reduceFilename := reduceName(jobName, mapTaskNumber, reduceIndex)
        jsonObj := jsonMap[reduceFilename]

        //Write the data in the json Obj
        jsonMap[reduceFilename] = append(jsonObj, v)
    }

    //Create the files with the json
    for filename, kv := range jsonMap {

        file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
        checkError(err)

        encoder := json.NewEncoder(file)
        encoder.Encode(&kv)

        file.Close()
    }
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
