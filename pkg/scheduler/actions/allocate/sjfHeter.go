package allocate

import (
	"volcano.sh/volcano/pkg/scheduler/api"
	"math/rand"
)

func sjfHeterFn(jobs []*api.JobInfo, nodes []*api.NodeInfo) map[*api.TaskInfo]*api.NodeInfo {
	// TODO: UPDATE THIS FUNCTION
	allocation := make(map[*api.TaskInfo]*api.NodeInfo)
	i := 0
	job := jobs[rand.Intn(len(jobs))]
	for _, task := range job.TaskStatusIndex[api.Pending] {
		for i<len(nodes) && len(nodes[i].Tasks) > 0 {
			// skip nodes that have tasks running
			i++
		}
		if i>=len(nodes) {
			// out of nodes
			break
		}
		allocation[task] = nodes[i]
		i++
	}
	if len(job.TaskStatusIndex[api.Pending]) != len(allocation) {
		// could not allocate all the tasks, return empty allocation
		allocation = make(map[*api.TaskInfo]*api.NodeInfo)
	}
	return allocation
}
