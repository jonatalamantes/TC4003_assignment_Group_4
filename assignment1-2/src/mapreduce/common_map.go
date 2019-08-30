package mapreduce

import (
	"hash/fnv"
    "io/ioutil"
    "os"
//    "fmt"
    "encoding/json"
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
    jsonMap := make(map[string] []KeyValue)
    for i, v := range mapValues {

        //Get the index of the reduce worker and file name
        reduceIndex := i % nReduce
        reduceFilename := reduceName(jobName, mapTaskNumber, reduceIndex)
        encoder, inMap := jsonMap[reduceFilename]

        //In case of not exist the key on the map of jsonEncoder
        if inMap == false {

            //Create the KeyValue of that node
            var kv []KeyValue
            jsonMap[reduceFilename] = kv
        }

        //Write the data in the encoder
        jsonMap[reduceFilename] = append(encoder, v)
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
