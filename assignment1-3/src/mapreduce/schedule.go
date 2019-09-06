package mapreduce

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)
    channel := make(chan bool)

    for i := 0; i < ntasks; i++ {

        anonymous := func(i int, done chan bool) {

            ok := false
            workerId := ""
            for ok == false {

                workerId = <- mr.registerChannel

                var args DoTaskArgs
                args.JobName = mr.jobName
                args.Phase = phase
                args.TaskNumber = i
                args.NumOtherPhase = nios
                if phase == mapPhase {
                    args.File = mr.files[i]
                }

                ok = call(workerId, "Worker.DoTask", &args, new(struct{}))
                if ok == false {
                    debug("Scheduler, Worker.DoTask fails, Retry connection %d \n", i)
                }
            }

            var rargs RegisterArgs
            rargs.Worker = workerId
            ok = call(mr.address, "Master.Register", rargs, new(struct{}))
            if ok == false {
                debug("Scheduler, Master.Registr fails\n")
            }

            done <- true
        }

        go anonymous(i, channel)
    }

    for i := 0; i < ntasks; i++ {
        <-channel
    }

	debug("Schedule: %v phase done\n", phase)
}
