package allocate

import (
	"volcano.sh/volcano/pkg/scheduler/api"
	"math/rand"
	"time"
)

func fifoRandomFn(jobs []*api.JobInfo, nodes []*api.NodeInfo) map[*api.TaskInfo]*api.NodeInfo {
	// TODO: UPDATE THIS FUNCTION
	allocation := make(map[*api.TaskInfo]*api.NodeInfo)

	if len(jobs) == 0 {
		return allocation
	}

	job := jobs[0]
	// get available nodes
	availableNodeIdx := []int{}
	for i, node := range nodes {
		if len(node.Tasks) == 0 {
			availableNodeIdx = append(availableNodeIdx, i)
		}
	}
	// shuffle the nodes index
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(availableNodeIdx), func(i, j int) {availableNodeIdx[i], availableNodeIdx[j] = availableNodeIdx[j], availableNodeIdx[i]})
	// allocate
	i := 0
	for _, task := range job.TaskStatusIndex[api.Pending] {
		if i < len(availableNodeIdx) {
			allocation[task] = nodes[availableNodeIdx[i]]
		} else {
			break
		}
		i++
	}
	if len(job.TaskStatusIndex[api.Pending]) != len(allocation) {
		// could not allocate all the tasks, return empty allocation
		allocation = make(map[*api.TaskInfo]*api.NodeInfo)
	}
	return allocation
}
