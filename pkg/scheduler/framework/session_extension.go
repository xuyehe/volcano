package framework

import (
	"k8s.io/klog/v2"
	"volcano.sh/volcano/pkg/scheduler/api"
)

func (ssn Session) GetPolicy(schedulerConf string) string {
	var err error
	conf := map[string]string{"policy": "fifoRandom"}
	if conf, err = ssn.cache.LoadSchedulerConf(schedulerConf); err != nil {
		klog.Errorf("Failed to load scheduler policy '%s', using default fifoRandom policy: %v",
			schedulerConf, err)
	}

	policyConf, found := conf["policy"]
	if !found {
		policyConf = "fifoRandom"
	}

	return policyConf
}

func (ssn Session) UpdateScheduledTime(task *api.TaskInfo) {
	if err := ssn.cache.UpdateScheduledTime(task); err != nil {
		klog.Errorf("Failed to update scheduled time of task <%v/%v>: %v", task.Namespace, task.Name, err)
	}
}
