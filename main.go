package main

import (
	"encoding/json"
	"k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"
)

type pvcItem struct {
	name 		string
	createTime 	time.Time
	boundTime 	time.Time
}

//func (p *pvcItem) String() string {
//	return p.name + "_" + p.createTime.String() + "_" + p.boundTime.String()
//}

var pvcNums = 40
var pvcItems = make(map[string]pvcItem, pvcNums)

//var gChan = make(chan struct{})

func main() {
	//klog.InitFlags(nil)
	//flag.Set("alsologtostderr", "true")
	//flag.Set("v", "4")

	configFile := filepath.Join(os.Getenv("HOME"), ".kube", "config")

	config, err := clientcmd.BuildConfigFromFlags("", configFile)
	if err != nil {
		klog.Errorf("Failed to build config: %s, Try to in cluster...", err.Error())
		config, err = rest.InClusterConfig()
		if err != nil {
			klog.Fatalf("Failed to build config: %s", err)
		}
	}

	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Failed to new clientSet: %s", err)
	}

	informerFactory := informers.NewSharedInformerFactoryWithOptions(clientSet, 0, informers.WithNamespace("test"))

	podInformer := informerFactory.Core().V1().Pods()
	podRehf := cache.ResourceEventHandlerFuncs{
		AddFunc:    podAddFunc,
		UpdateFunc: podUpdateFunc,
		DeleteFunc: podDeleteFunc,
	}
	podInformer.Informer().AddEventHandler(podRehf)

	pvcInformer := informerFactory.Core().V1().PersistentVolumeClaims()
	pvcRehf := cache.ResourceEventHandlerFuncs{
		AddFunc:    pvcAddFunc,
		UpdateFunc: pvcUpdateFunc,
		DeleteFunc: pvcDeleteFunc,
	}
	pvcInformer.Informer().AddEventHandler(pvcRehf)

	//pvInformer := informerFactory.Core().V1().PersistentVolumes()
	//pvRehf := cache.ResourceEventHandlerFuncs{
	//	AddFunc:    pvAddFunc,
	//	UpdateFunc: pvUpdateFunc,
	//	DeleteFunc: pvDeleteFunc,
	//}
	//pvInformer.Informer().AddEventHandler(pvRehf)

	stopCh := make(chan struct{}, 1)
	go informerFactory.Start(stopCh)
	if !cache.WaitForCacheSync(stopCh, podInformer.Informer().HasSynced, pvcInformer.Informer().HasSynced) {
		klog.Fatalln("Failed to sync cache")
	}
	//pvInformer.Informer().Run()

	createPvc(clientSet)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)
	<-signalCh
	klog.Infoln("Got OS shutdown signal, shutting down informer gracefully...")
	close(stopCh)
	//time.Sleep(9 * time.Second)

	//klog.Infof("Pvc items: %s", pvcItems)
	timeList := make([]time.Duration, 0)
	var minDur time.Duration
	var maxDur time.Duration
	var sumDur time.Duration
	var aveDur time.Duration
	for _, v := range pvcItems {
		dur := v.boundTime.Sub(v.createTime)

		if minDur == 0 {
			minDur = dur
		} else if minDur > dur {
			minDur = dur
		}

		if dur > maxDur {
			maxDur = dur
		}

		klog.Infof("Pv: name: %s, use time: %v", v.name, dur)
		timeList = append(timeList, dur)
	}

	//计算时间使用率
	klog.Infof("Time list: %v", timeList)
	sort.Slice(timeList, func(i, j int) bool {
		return timeList[i] < timeList[j]
	})
	for _, dur := range timeList {
		sumDur += dur
	}
	aveDur = sumDur / time.Duration(len(timeList))
	klog.Infof("Summary %v: min: %v, max: %v, ave: %v", len(timeList), minDur, maxDur, aveDur)
}

var pvcString = `{
    "apiVersion": "v1",
    "kind": "PersistentVolumeClaim",
    "metadata": {
        "name": "pvc-test-"
    },
    "spec": {
        "accessModes": [
            "ReadWriteOnce"
        ],
        "resources": {
            "requests": {
                "storage": "1Gi"
            }
        },
        "storageClassName": "unity"
    }
}`

func createPvc(clientSet *kubernetes.Clientset) {
	//limitCh := make(chan struct{}, 100)
	rawPvc := v1.PersistentVolumeClaim{}
	err := json.Unmarshal([]byte(pvcString), &rawPvc)
	if err != nil {
		klog.Fatalln("Failed to unmarshal json: %s", err)
	}

	wg := &sync.WaitGroup{}

	for i := 0; i < pvcNums; i++ {
		wg.Add(1)

		go func(rawPvc v1.PersistentVolumeClaim, i int) {
			defer wg.Done()

			pvc := rawPvc
			pvc.Name = pvc.Name + strconv.Itoa(i)
			resultPvc, err := clientSet.CoreV1().PersistentVolumeClaims("test").Create(&pvc)
			if err != nil {
				klog.Errorf("Failed to create pvc: %s", err)
				return
			}
			klog.V(4).Infof("Created pvc: %v", resultPvc)
		}(rawPvc, i)
	}
	wg.Wait()
	klog.Infof("Create pvc done")
}

//pod
func podAddFunc(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		klog.Errorf("Receive not pod")
		return
	}

	klog.Infoln("Add pod: ", pod.Name, pod.Status.Phase)
	return
}

func podUpdateFunc(oldObj, newObj interface{}) {
	pod, ok := newObj.(*v1.Pod)
	if !ok {
		klog.Errorf("Receive not pod")
		return
	}

	klog.Infoln("Update pod: ", pod.Name, pod.Status.Phase)
	return
}
func podDeleteFunc(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		klog.Errorf("Receive not pod")
		return
	}

	klog.Infoln("Del pod: ", pod.Name, pod.Status.Phase)
	data, _ := json.Marshal(pod)
	klog.Infoln(string(data))
	return
}

//pvc
func pvcAddFunc(obj interface{}) {
	pv, ok := obj.(*v1.PersistentVolumeClaim)
	if !ok {
		klog.Errorf("Receive not pv")
		return
	}

	klog.Infoln("Add pvc: ", pv.Name, pv.Status.Phase)
	return
}

func pvcUpdateFunc(oldObj, newObj interface{}) {
	start := time.Now().UTC()

	pv, ok := newObj.(*v1.PersistentVolumeClaim)
	if !ok {
		klog.Errorf("Receive not pv")
		return
	}

	if pv.Status.Phase == "Bound" {
		item := pvcItem{
			name:       pv.Name,
			createTime: pv.CreationTimestamp.Time,
			boundTime: start,
		}

		pvcItems[pv.Name] = item

		klog.Infof("Items add pvc: %v", item)
	}

	klog.Infoln("Update pvc: ", pv.Name, pv.Status.Phase)
	data, _ := json.Marshal(pv)
	klog.Infoln(string(data))
	return
}
func pvcDeleteFunc(obj interface{}) {
	pv, ok := obj.(*v1.PersistentVolumeClaim)
	if !ok {
		klog.Errorf("Receive not pv")
		return
	}

	klog.Infoln("Del pvc: ", pv.Name, pv.Status.Phase)
	data, _ := json.Marshal(pv)
	klog.Infoln(string(data))
	return
}

//pv
func pvAddFunc(obj interface{}) {
	pv, ok := obj.(*v1.PersistentVolume)
	if !ok {
		klog.Errorf("Receive not pv")
		return
	}

	klog.Infoln("Add pv: ", pv.Name, pv.Status.Phase)
	return
}

func pvUpdateFunc(oldObj, newObj interface{}) {
	pv, ok := newObj.(*v1.PersistentVolume)
	if !ok {
		klog.Errorf("Receive not pv")
		return
	}

	klog.Infoln("Update pv: ", pv.Name, pv.Status.Phase)
	data, _ := json.Marshal(pv)
	klog.Infoln(string(data))
	return
}
func pvDeleteFunc(obj interface{}) {
	pv, ok := obj.(*v1.PersistentVolume)
	if !ok {
		klog.Errorf("Receive not pv")
		return
	}

	klog.Infoln("Del pv: ", pv.Name, pv.Status.Phase)
	data, _ := json.Marshal(pv)
	klog.Infoln(string(data))
	return
}