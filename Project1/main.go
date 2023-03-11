package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	//Sort arrival time (Just to be safe)
	sort.Slice(processes[:], func(a, b int) bool {
		return processes[a].ArrivalTime < processes[b].ArrivalTime
	})

	// First-come, first-serve scheduling
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	SJFSchedule(os.Stdout, "Shortest-job-first", processes)

	//SJFPrioritySchedule(os.Stdout, "Priority", processes)

	//RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
                gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func calculateAndPrintStats(w io.Writer, processes []Process, gantt []TimeSlice){
	var (
                totalWait       float64
                totalTurnaround float64
                lastCompletion  float64
                schedule        = make([][]string, len(processes))
        )
	for i := range processes {
		var computationTime int64 = 0
		var waitingTime int64 = 0
		var finishTime int64 = 0
		for j := range gantt {
			if(gantt[j].PID == processes[i].ProcessID){
				computationTime += gantt[j].Stop - gantt[j].Start
			}else{
				waitingTime += gantt[j].Stop - gantt[j].Start
			}
			if(computationTime >= processes[i].BurstDuration){
				finishTime = gantt[j].Stop
				break
			}
		}
		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(waitingTime + computationTime),
			fmt.Sprint(finishTime),
		}
		totalWait += float64(waitingTime)
		totalTurnaround += float64(waitingTime + computationTime)
		if(float64(finishTime) > lastCompletion){
			lastCompletion = float64(finishTime)
		}
	}

	count := float64(len(processes))
        aveWait := totalWait / count
        aveTurnaround := totalTurnaround / count
        aveThroughput := count / lastCompletion

	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

//Plan: do my scheduling here, and make the FCFS code calculate all the statistics
func SJFSchedule(w io.Writer, title string, inputProcesses []Process) {

	processes := inputProcesses

	var gantt = make([]TimeSlice, 0)
	var time int64 = 0
	var timeSlot int64 = 0 //The current running process's TimeSlice index in gantt
	var ganttStart = func(pid int){
		gantt = append(gantt, TimeSlice{
			PID:	processes[pid].ProcessID,
			Start:	time,
			Stop:	time,	//Temporary value
		})
	}
	var ganttStop = func(){
		gantt[timeSlot].Stop = time
		timeSlot++
	}
	var ganttSwap = func(pid int){
		ganttStop()
		ganttStart(pid)
	}

	//Waiting queue just holds the index of the process in the processes array
	var waitingQueue = make([]int, 0)
	var waitingQueueAdd = func(pid int){
		waitingQueue = append(waitingQueue, pid)
	}
	var waitingQueueRemove = func() int{
		var pid int = waitingQueue[0]
		waitingQueue = waitingQueue[1:]
		return pid
	}

	//We can assume processes are sorted by arrival time
	var running int = -1
	for i := 0; i < len(processes); i++ {
		//This does it for i too
		//This is to ensure that processess that appear at the same time are evaluated together
		for arrivalTime := processes[i].ArrivalTime; (i < len(processes)) && (processes[i].ArrivalTime == arrivalTime); i++ {
			waitingQueue = append(waitingQueue, i)
		}
		i--

		//Should really use insertion sort here, but this is too easy
		//Sort the items in the waiting queue by their burstDuration
		sort.Slice(waitingQueue[:], func (a, b int) bool{
			return processes[waitingQueue[a]].BurstDuration < processes[waitingQueue[b]].BurstDuration
		})

		var SHORTEST_JOB_IN_THE_QUEUE int = waitingQueue[0]

		var PREVIOUS_TIME int64 = time

		//This way, on i == 0, TIME_ELAPSED == processes[i].ArrivalTime
		var TIME_ELAPSED int64 = processes[i].ArrivalTime - PREVIOUS_TIME

		time += TIME_ELAPSED

		//Elapse time of running program
		if (running >= 0){
			processes[running].BurstDuration -= TIME_ELAPSED

			if(processes[SHORTEST_JOB_IN_THE_QUEUE].BurstDuration < processes[running].BurstDuration){
				waitingQueueAdd(running)
				running = waitingQueueRemove()
				ganttSwap(running)
			}

			//In theory, this shouldn't happen
			//We shouldn't have a burst duration of 0 after a fast forward because we let the process finish processing before we fast forward
			//if processes[running].BurstDuration <= 0 {
				//Make note of the end time in gantt
			//}
		}else{
			running = waitingQueueRemove()
			ganttStart(running)
		}

		//If the current process won't get preempted by the next arriving process
		//The greater than in the if statement means that if the process gets preempted, there will be at least one burst time left in the process when that new process arrives
		//If BurstDuration + time == process[i+1].ArrivalTime, then the next TIME_ELAPSED will be 0
		for !((i + 1) < len(processes) && (processes[running].BurstDuration) + time > processes[i+1].ArrivalTime) {
			//We wait it out before we fast forward
			time += processes[running].BurstDuration
			if (len(waitingQueue) <= 0){
				running = -1
				ganttStop()
				break
			}else{
				running = waitingQueueRemove()
				fmt.Printf("%d\n", running)
				ganttSwap(running)
			}
		}
	}

	outputTitle(w, title)
	calculateAndPrintStats(w, processes, gantt);
}

//func SJFPrioritySchedule(w io.Writer, title string, processes []Process) { }

//func RRSchedule(w io.Writer, title string, processes []Process) { }

//endregion

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion
