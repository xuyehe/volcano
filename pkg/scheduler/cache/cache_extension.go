package cache

import (
	"context"
	"encoding/json"
	"fmt"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"time"

	volapi "volcano.sh/volcano/pkg/scheduler/api"
)

func (sc *SchedulerCache) UpdateScheduledTime(task *volapi.TaskInfo) error {
	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()
	updateInterval := 3 * time.Second
	retryTimes := 5
	scheduledTime := metav1.NewTime(time.Now())
	for i := 0; i < retryTimes; i++ {
		pod, err := sc.kubeClient.CoreV1().Pods(task.Pod.Namespace).Get(context.TODO(), task.Pod.Name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}

		refJob := pod.Annotations["volcano.sh/job-name"]
		if refJob == "" {
			fmt.Printf("Sh*t we don't have volcano.sh/job-name label that is supposed to be here present for pod %v\n", pod.Name)
		}

		var perr error
		if (pod.ObjectMeta.Annotations == nil) || (pod.ObjectMeta.Annotations != nil && pod.ObjectMeta.Annotations["scheduledTime"] == "") {
			a := map[string]string{"scheduledTime": scheduledTime.Rfc3339Copy().String()}
			m := map[string]interface{}{"annotations": a}
			pat := map[string]interface{}{"metadata": m}

			bt, err := json.Marshal(pat)
			if err != nil {
				fmt.Printf("Error with marshalling patch %v\n", err)
			}
			_, perr = sc.kubeClient.CoreV1().Pods(pod.Namespace).Patch(context.TODO(), task.Pod.Name, types.StrategicMergePatchType, bt, metav1.PatchOptions{})

			if perr != nil {
				fmt.Printf("Error with patching pod %v\n", err)
				return perr
			}
		}


		// Patch a vcjob
		vcj, err := sc.vcClient.BatchV1alpha1().Jobs("default").Get(context.TODO(), refJob, metav1.GetOptions{})
		// Problem usually for only one replica pod  jobs
		if vcj.Spec.Tasks[0].Replicas == 1 {
			var patches []map[string]interface{}
			var patch map[string]interface{}
			if len(vcj.Annotations) == 0 {
				patch = map[string]interface{}{
					"op":    "add",
					"path":  "/metadata/annotations",
					"value": map[string]string{"scheduledTime": scheduledTime.Rfc3339Copy().String()},
				}
				patches = append(patches, patch)
			} else {
				patch = map[string]interface{}{
					"op":    "add",
					"path":  "/metadata/annotations/scheduledTime",
					"value": scheduledTime.Rfc3339Copy().String(),
				}
				patches = append(patches, patch)
			}

			bt, err := json.Marshal(patches)
			if err != nil {
				fmt.Printf("Error with marshalling patch %v\n", err)
			}
			vcj, jerr := sc.vcClient.BatchV1alpha1().Jobs("default").Patch(context.TODO(), refJob, types.JSONPatchType, bt, metav1.PatchOptions{})
			if jerr != nil {
				fmt.Printf("Patching vcjob %v not successful: %v\n", refJob, err)
				return jerr
			}
			fmt.Printf("New annotations of job %v are %v\n", refJob, vcj.Annotations)
			if perr == nil && jerr == nil {
				return nil
			}
		}
		if perr == nil {
			return nil
		}

		time.Sleep(updateInterval)
	}
	return fmt.Errorf("update pod scheduled time failed after %d retries", retryTimes)
}

func (sc *SchedulerCache) LoadSchedulerConf(path string) (map[string]string, error) {
	ns, name, err := cache.SplitMetaNamespaceKey(path)
	if err != nil {
		return nil, err
	}

	confMap, err := sc.kubeClient.CoreV1().ConfigMaps(ns).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	return confMap.Data, nil
}
