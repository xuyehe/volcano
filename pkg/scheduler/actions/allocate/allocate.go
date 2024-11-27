/*
 Copyright 2021 The Volcano Authors.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package allocate

import (
	// "time"

	"k8s.io/klog/v2"

	// disabling the following packages to make p3k8s work
	// "volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
	// "volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	// "volcano.sh/volcano/pkg/scheduler/metrics"
	"volcano.sh/volcano/pkg/scheduler/util"

	// packages needed to make p3k8s work
	// Author: Tianya Chen
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

/****************   p3k8s specific strcuts  ********************/
// Author: Tianya Chen
type JobT struct {
	JobID        int    `json:"jobID"`
	JobType      string `json:"jobType"`
	K            int    `json:"k"`
	Duration     int    `json:"duration"`
	SlowDuration int    `json:"slowDuration"`
}

type InputT struct {
	RackCap              []int  `json:"rack_cap"`
	NumLargeMachineRacks int    `json:"numLargeMachineRacks"`
	Queue                []JobT `json:"queue"`
	Machines             []int  `json:"machines"`
}

type OutputT struct {
	JobID    int   `json:"jobID"`
	Machines []int `json:"machines"`
}

type Message struct {
	Input  InputT      `json:"input"`
	Output interface{} `json:"output"`
}

/*****************   p3k8s specific structs above  ******************/

// Xuye He: volcano v1.10 updates action struct
type Action struct {
	session *framework.Session
	// configured flag for error cache
	enablePredicateErrorCache bool
}

func New() *Action {
	return &Action{
		enablePredicateErrorCache: true, // default to enable it
	}
}

func (alloc *Action) Name() string {
	return "allocate"
}

func (alloc *Action) Initialize() {}

// volcano v1.10 updates action struct
// requires importing conf, omitted
// func (alloc *Action) parseArguments(ssn *framework.Session) {
// 	arguments := framework.GetArgOfActionFromConf(ssn.Configurations, alloc.Name())
// 	arguments.GetBool(&alloc.enablePredicateErrorCache, conf.EnablePredicateErrCacheKey)
// }

// Custom execute function for p3k8s
// Authors: Tianya Chen, Baljit Singh
func (alloc *Action) Execute(ssn *framework.Session) {
	klog.V(3).Infof("Enter Allocate ...")
	defer klog.V(3).Infof("Leaving Allocate ...")

	// parse config, populates the Action struct
	// alloc.parseArguments(ssn)
	alloc.session = ssn

	// Load configuration of policy
	policyConf := ssn.GetPolicy("kube-system/scheduler-conf")
	klog.V(3).Infof("Using policy %v.", policyConf)

	// Select a policy function
	policyFn := fifoRandomFn
	switch policyConf {
	case "fifoRandom":
		policyFn = fifoRandomFn
	case "fifoHeter":
		policyFn = fifoHeterFn
	case "sjfHeter":
		policyFn = sjfHeterFn
	case "custom":
		policyFn = customFn
	}

	// Prepare job queue
	jobQueue := util.NewPriorityQueue(ssn.JobOrderFn)
	var trace string
	var t *api.TaskInfo
	for _, job := range ssn.Jobs {
		numPendingTasks := len(job.TaskStatusIndex[api.Pending])
		if numPendingTasks >= int(job.MinAvailable) {
			job = addJobProperty(job)
			jobQueue.Push(job)
			if trace == "" {
				trace = job.Trace
			} else if trace != job.Trace {
				klog.Errorf("Found multiple traces (%v, %v) in Session %v",
					trace, job.Trace, ssn.UID)
			}
			if t == nil {
				t = getOneTask(job)
			}
		} else {
			klog.V(3).Infof("Job <%v, %v> has %v tasks pending but requires %v tasks (creation in progress?).",
				job.Namespace, job.Name, numPendingTasks, job.MinAvailable)
		}
	}

	// no jobs in the queue, exit
	if jobQueue.Empty() {
		klog.V(3).Infof("No jobs awaiting, DONE")
		return
	}

	// add jobs to the queue in the order of their creation time
	jobs := []*api.JobInfo{}
	for {
		job := jobQueue.Pop()
		jobs = append(jobs, job.(*api.JobInfo))
		if jobQueue.Empty() {
			break
		}
	}

	// Prepare node info
	nodes := []*api.NodeInfo{}
	nodesAvailable := make(map[string]*api.NodeInfo)
	selector := labels.SelectorFromSet(labels.Set(map[string]string{"type": "virtual-kubelet"}))
	for _, node := range ssn.Nodes {
		if selector.Matches(labels.Set(node.Node.Labels)) {
			node = addNodeProperty(node)
			if node.Rack < 0 {
				continue
			}
			nodes = append(nodes, node)
			if t.Resreq.LessEqual(node.Idle, api.Zero) {
				nodesAvailable[node.Node.ObjectMeta.Name] = node
			}
		}
	}

	// no nodes available, exit
	if len(nodesAvailable) <= 0 {
		klog.V(3).Infof("No nodes available, DONE")
		return
	}

	klog.V(3).Infof("%v/%v nodes available:", len(nodesAvailable), len(nodes))
	for _, node := range nodes {
		if _, found := nodesAvailable[node.Name]; found {
			klog.V(3).Infof("    <%v>: available", node.Name)
		} else {
			klog.V(3).Infof("    <%v>", node.Name)
		}
	}

	nothingScheduled := true
	var input InputT

	// repeat until no more jobs can be scheduled (one job scheduled in each iteration)
	for {
		// Prepare policy input for grader json
		input = prepareInput(jobs, nodes, nodesAvailable)

		// Call policy function to get allocation
		allocation := policyFn(jobs, nodes)

		// nothing could be scheduled
		if len(allocation) == 0 {
			break
		}

		// Validate allocation returned by the policy
		var jobAllocated *api.JobInfo
		var jobAllocatedIdx int
		validAllocation := true
		// Tasks don't include reference to job, so need to traverse all jobs and tasks
		for idx, job := range jobs {
			nodeInUse := make(map[*api.NodeInfo]bool)
			for _, task := range job.TaskStatusIndex[api.Pending] {
				node, taskAllocated := allocation[task]
				// task found in allocation
				if taskAllocated {
					nothingScheduled = false
					if jobAllocated == nil {
						// we found the job
						jobAllocated = job
						jobAllocatedIdx = idx
					} else {
						// we already found allocated task before, check if they match
						// allocated included multiple jobs
						if job != jobAllocated {
							validAllocation = false
							klog.Errorf("ERROR! Allocation included both Job %v and %v.",
								jobAllocated.Name, job.Name)
							break
						}
					}
					if nodeInUse[node] {
						validAllocation = false
						klog.Errorf("ERROR! Could not allocate Task <%v/%v>: Node %v already in use",
							task.Namespace, task.Name, node.Name)
						break
					}
					if !task.Resreq.LessEqual(node.Idle, api.Zero) {
						validAllocation = false
						klog.Errorf("ERROR! Could not allocate Task <%v/%v>: node enough idle resources in Node %v",
							task.Namespace, task.Name, node.Name)
						break
					}
					nodeInUse[node] = true
				} else { // task not allocated by the policy
					if jobAllocated != nil { // some task from this job was allocated, but this task wasn't
						validAllocation = false
						klog.Errorf("ERROR! Job %v partially allocated", job.Name)
						break
					} else {
						// can contiue to the next task
						// not skipping the entire job, to detect partial allocations
						continue
					}
				}
			}
			// allocation included task(s) from this job
			if jobAllocated != nil {
				// no need to check other jobs
				break
			}
		}

		if jobAllocated == nil {
			// returned allocation does not contain tasks of a valid job from the queue
			// no point to retry with the same inputs - exit the loop
			break
		}

		// prepare output for grader
		var output OutputT
		output.JobID = jobAllocated.ID
		// find nodes in the returned allocation that belong to <jobAllocated>
		for _, task := range jobAllocated.TaskStatusIndex[api.Pending] {
			node, found := allocation[task]
			if found {
				output.Machines = append(output.Machines, node.ID)
			}
		}

		// Record scheduling decision in a json file
		recordDecision(input, output, trace)

		if validAllocation {
			// Allocate tasks
			for task, node := range allocation {
				klog.V(3).Infof("Try to bind Task <%v/%v> to Node <%v>: <%v> vs. <%v>",
					task.Namespace, task.Name, node.Name, task.Resreq, node.Idle)
				if err := ssn.Allocate(task, node); err != nil {
					klog.V(3).Infof("ERROR! Failed to bind Task %v, %v on %v in Session %v\n",
						task.UID, task.Name, node.Name, ssn.UID)
				} else {
					ssn.UpdateScheduledTime(task)
					// update nodesAvailable for next iteration
					delete(nodesAvailable, node.Name)
				}
			}

		}

		//		for _, j := range jobs {
		//			fmt.Printf("Printing job %v in jobs list and its annotations \n", j.Name)
		//			for _, info := range j.Tasks {
		//				p, e := ssn.KubeClient().CoreV1().Pods(j.Namespace).Get(context.TODO(), info.Pod.Name, metav1.GetOptions{})
		//				if e != nil {
		//					fmt.Printf("Error getting pod %v: %v\n", p.Name, e)
		//				} else {
		//					fmt.Printf("Printing annotations from pod %v: %v\n", p.Name, p.Annotations)
		//				}
		//			}
		//		}
		//		fmt.Printf("Finished printing all jobs in job list passed to the policy in iteration %v\n", itCounter)

		// remove the allocated job from the list passed to the policy in the next loop iteration
		// if allocation was not valid, the job will be considered again next time Execute() is called
		jobs = append(jobs[:jobAllocatedIdx], jobs[jobAllocatedIdx+1:]...)

		// if no more jobs or nodes, exit the loop
		if len(jobs) == 0 {
			klog.V(3).Infof("No jobs awaiting, DONE")
			break
		}

		klog.V(3).Infof("%v jobs awaiting:", len(jobs))
		for _, job := range jobs {
			klog.V(3).Infof("    <%v/%v>", job.Namespace, job.Name)
		}

		if len(nodesAvailable) <= 0 {
			klog.V(3).Infof("No nodes available, DONE")
			break
		}

		klog.V(3).Infof("%v/%v nodes available:", len(nodesAvailable), len(nodes))
		for _, node := range nodes {
			if _, found := nodesAvailable[node.Name]; found {
				klog.V(3).Infof("    <%v>: available", node.Name)
			} else {
				klog.V(3).Infof("    <%v>", node.Name)
			}
		}
	}
	if nothingScheduled { // if nothing scheduled, record empty scheduling decision
		var output OutputT // empty
		recordDecision(input, output, trace)
	}
}

func (alloc *Action) UnInitialize() {}

/*********************** p3k8s specific functions *********************************/
// Author: Tianya Chen

// keep track of input and output in the previous allocation decision
var prevInput InputT
var prevOutput OutputT

func recordDecision(input InputT, output OutputT, trace string) {
	// Marshal policy input and output to json and write to file
	var message Message
	message.Input = input
	if len(output.Machines) > 0 {
		sort.Ints(output.Machines)
		message.Output = output
	}
	// save only if input is different than the previous one
	if !reflect.DeepEqual(input, prevInput) || !reflect.DeepEqual(output, prevOutput) {
		jobsInfo := []int{}
		for _, jq := range input.Queue {
			jobsInfo = append(jobsInfo, jq.JobID)
		}
		sort.Ints(jobsInfo)
		nodesInfo := input.Machines
		sort.Ints(nodesInfo)
		if len(output.Machines) > 0 {
			klog.Infof("Policy scheduled JobID=%v to %v (Input queue: %v, nodes: %v)",
				output.JobID, output.Machines, jobsInfo, nodesInfo)
		} else {
			klog.Infof("Policy could not schedule any job (Input queue: %v, nodes: %v)",
				jobsInfo, nodesInfo)
		}
		b, _ := json.Marshal(message)
		traceFile, _ := os.OpenFile(fmt.Sprintf("/tmp/trace-%s.json", trace), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		traceFile.Write(append(b, ','))
		traceFile.Close()
	} else {
		klog.V(3).Infof("Same input, skip recording")
	}
	// remember input and output, to avoid saving identical scheduling decisions
	prevInput = input
	prevOutput = output
}

func addJobProperty(job *api.JobInfo) *api.JobInfo {
	for _, task := range job.TaskStatusIndex[api.Pending] {
		//		jobID, _ := strconv.ParseInt(job.Name[3 :], 10, 64)
		jobID, _ := strconv.ParseInt(strings.Split(job.Name, "-")[1], 10, 64)
		job.ID = int(jobID)
		job.Trace = task.Pod.ObjectMeta.Labels["trace"]
		job.Type = task.Pod.ObjectMeta.Labels["type"]
		fastDuration, _ := strconv.ParseInt(task.Pod.ObjectMeta.Labels["FastDuration"], 10, 64)
		job.FastDuration = int(fastDuration)
		slowDuration, _ := strconv.ParseInt(task.Pod.ObjectMeta.Labels["SlowDuration"], 10, 64)
		job.SlowDuration = int(slowDuration)
		break
	}
	job.CreationTime = metav1.Now()
	for _, task := range job.TaskStatusIndex[api.Pending] {
		if task.Pod.ObjectMeta.CreationTimestamp.Before(&job.CreationTime) {
			job.CreationTime = task.Pod.ObjectMeta.CreationTimestamp
		}
	}
	return job
}

func addNodeProperty(node *api.NodeInfo) *api.NodeInfo {
	nodeID, _ := strconv.ParseInt(node.Node.ObjectMeta.Name[3:], 10, 64)
	node.ID = int(nodeID)
	if rack, found := node.Node.ObjectMeta.Labels["Rack"]; found {
		rackID, _ := strconv.ParseInt(rack, 10, 64)
		node.Rack = int(rackID)
	} else {
		node.Rack = -1
	}
	if gpu, found := node.Node.ObjectMeta.Labels["GPU"]; found && gpu == "true" {
		node.GPU = true
	} else {
		node.GPU = false
	}
	return node
}

func getOneTask(job *api.JobInfo) *api.TaskInfo {
	for _, t := range job.TaskStatusIndex[api.Pending] {
		return t
	}
	return nil
}

func prepareInput(jobs []*api.JobInfo, nodes []*api.NodeInfo, nodesAvailable map[string]*api.NodeInfo) InputT {
	var input InputT

	// Collect rack capacities and number of GPU racks from node info
	rackCap := make(map[int]int)
	for _, node := range nodes {
		if node.Rack >= 0 {
			if _, found := rackCap[node.Rack]; found {
				rackCap[node.Rack] = rackCap[node.Rack] + 1
			} else {
				rackCap[node.Rack] = 1
			}
			if node.GPU {
				if node.Rack > input.NumLargeMachineRacks {
					input.NumLargeMachineRacks = node.Rack
				}
			}
		}
	}
	for rackID := 1; rackID <= len(rackCap); rackID++ {
		input.RackCap = append(input.RackCap, rackCap[rackID])
	}

	// Collect job info
	for _, job := range jobs {
		var queueJob JobT
		queueJob.JobID = job.ID
		queueJob.K = int(job.MinAvailable)
		queueJob.JobType = job.Type
		queueJob.Duration = job.FastDuration
		queueJob.SlowDuration = job.SlowDuration
		input.Queue = append(input.Queue, queueJob)
	}

	// Collect node info
	for _, node := range nodesAvailable {
		input.Machines = append(input.Machines, node.ID)
	}

	sort.Ints(input.Machines)

	return input
}
