package main

import (
	"encoding/json"
	"github.com/amourlinux/dell-cst-test/cmap"
	"k8s.io/api/core/v1"
	//metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
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

	deleteTime	time.Time
	deletedTime	time.Time
	pvName 		string
	pvDeleteTime	time.Time
	pvDeletedTime	time.Time
}

//func (p *pvcItem) String() string {
//	return p.name + "_" + p.createTime.String() + "_" + p.boundTime.String()
//}

var pvcNums = 5
//var pvcItems = make(map[string]pvcItem, pvcNums)

var pvcItemsConcurrent = cmap.New()

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

	pvInformer := informerFactory.Core().V1().PersistentVolumes()
	pvRehf := cache.ResourceEventHandlerFuncs{
		AddFunc:    pvAddFunc,
		UpdateFunc: pvUpdateFunc,
		DeleteFunc: pvDeleteFunc,
	}
	pvInformer.Informer().AddEventHandler(pvRehf)

	stopCh := make(chan struct{}, 1)
	go informerFactory.Start(stopCh)
	if !cache.WaitForCacheSync(stopCh, podInformer.Informer().HasSynced, pvcInformer.Informer().HasSynced) {
		klog.Fatalln("Failed to sync cache")
	}


	createPvc(clientSet)
	//check pvc bound
	go checkPvcBound()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)
	<-signalCh
	klog.Infoln("Got OS shutdown signal, shutting down informer gracefully...")
	//close(stopCh)
	//time.Sleep(9 * time.Second)

	//compute percentage time
	//klog.Infof("Pvc items: %s", pvcItems)
	timeList := make([]time.Duration, 0)
	var minDur time.Duration
	var maxDur time.Duration
	var sumDur time.Duration
	var aveDur time.Duration
	tupsCh := pvcItemsConcurrent.IterBuffered()
	for tup := range tupsCh {
		item := tup.Val.(*pvcItem)

		dur := item.boundTime.Sub(item.createTime)

		if minDur == 0 {
			minDur = dur
		} else if minDur > dur {
			minDur = dur
		}

		if dur > maxDur {
			maxDur = dur
		}

		klog.Infof("Pvc: name: %s, use time: %v", item.name, dur)
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
	klog.Infoln(pvcItemsConcurrent.Keys())
	klog.Infof("Time list sorted: %v", timeList)
	klog.Infof("Summary %v: min: %v, max: %v, ave: %v", len(timeList), minDur, maxDur, aveDur)

	for tup := range pvcItemsConcurrent.IterBuffered() {
		item := tup.Val.(*pvcItem)
		klog.Infoln(item.name, item.pvName, item.createTime, item.boundTime, item.deleteTime, item.deletedTime)
	}

	//Delete resource before created
	deletePvc(clientSet)

	//wait del event
	//time.Sleep(time.Second * 10)
	sCh := make(chan struct{})
	check := func() bool {
		for _, v := range pvcItemsConcurrent.Items() {
			if v.(*pvcItem).pvDeletedTime.IsZero() {
				return false
			}
		}
		return true
	}
	wait.Until(func() {
		ok := check()
		if ok {
			close(sCh)
		}
	}, time.Second, sCh)


	for tup := range pvcItemsConcurrent.IterBuffered() {
		item := tup.Val.(*pvcItem)
		klog.Infoln(item.name, item.deleteTime, item.deletedTime, item.pvName, item.pvDeleteTime, item.pvDeletedTime)
	}

	//Compute deleted time
	delTimeList := make([]time.Duration, 0)
	var sumDelDur time.Duration
	//var minDelDur time.Duration
	//var maxDelDur time.Duration
	var aveDelDur time.Duration
	for tup := range pvcItemsConcurrent.IterBuffered() {
		item := tup.Val.(*pvcItem)
		usedTime := item.pvDeletedTime.Sub(item.deleteTime)
		delTimeList = append(delTimeList, usedTime)

		sumDelDur += usedTime
	}
	klog.Infof("Del time list: %v", delTimeList)
	sort.Slice(delTimeList, func(i, j int) bool {
		return delTimeList[i] < delTimeList[j]
	})
	klog.Infof("Del time list sorted: %v", delTimeList)

	aveDelDur = sumDelDur / time.Duration(len(delTimeList))
	//klog.Infof("Summary: Ave: %v", aveDelDur)
	klog.Infof("Summary %v: min: %v, max: %v, ave: %v", len(delTimeList), delTimeList[0], delTimeList[len(delTimeList)-1], aveDelDur)


	//Exiting
	close(stopCh)
	klog.Infoln("Exited")
}

func checkPvcBound() {
	stopCh := make(chan struct{})

	check := func() {
		if pvcItemsConcurrent.Count() != pvcNums {
			return
		}
		for _, val := range pvcItemsConcurrent.Items() {
			if val.(*pvcItem).boundTime.IsZero() {
				return
			}
		}

		close(stopCh)
	}

	wait.Until(check, time.Second, stopCh)

	klog.Infoln("---------------- All pvc bound ----------------------")
}

func deletePvc(cs *kubernetes.Clientset) {
	wg := &sync.WaitGroup{}

	//tups := pvcItemsConcurrent.IterBuffered()
	keys := pvcItemsConcurrent.Keys()
	klog.Infof("Will deleting pvc: %v", keys)
	for _, name := range keys {
		go func(pvcName string) {
			wg.Add(1)
			err := cs.CoreV1().PersistentVolumeClaims("test").Delete(pvcName, nil)
			if err != nil {
				klog.Errorf("GC: Failed to delete pvc: %v", err)
			}
			wg.Done()
		}(name)

		//err := cs.CoreV1().PersistentVolumeClaims("test").Delete(name, nil)
		//if err != nil {
		//	klog.Errorf("GC: Failed to delete pvc: %v", err)
		//}
	}

	wg.Wait()
	//pvc, err := cs.CoreV1().PersistentVolumeClaims("test").Get("pvc-test-0", metav1.GetOptions{})
	//if err != nil {
	//	klog.Fatalf("Failed to get pvc: %s", err)
	//}
	//klog.Infof("Pvc: %v", pvc)
	klog.Infoln("Delete pvc done")
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
				klog.Fatalf("Failed to create pvc: %s", err)
				return
			}
			klog.Infof("Created pvc: %v", resultPvc)
			//pvcItems[pvc.Name] = pvcItem{
			//	name:         pvc.Name,
			//	createTime:   pvc.CreationTimestamp.Time,
			//	//boundTime:    nil,
			//	//deleteTime:   nil,
			//	pvName:       "",
			//	//pvDeleteTime: nil,
			//}
			item := &pvcItem{
				name:         pvc.Name,
				//createTime:   pvc.CreationTimestamp.Time,
				//boundTime:    nil,
				//deleteTime:   nil,
				//pvName:       "",
				//pvDeleteTime: nil,
			}
			pvcItemsConcurrent.Set(pvc.Name, item)
		}(rawPvc, i)
	}
	wg.Wait()
	klog.Infof("Created pvc done: %v", pvcItemsConcurrent.Items())
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

	pvc, ok := newObj.(*v1.PersistentVolumeClaim)
	if !ok {
		klog.Errorf("Receive not pvc")
		return
	}

	//if pvc.Status.Phase == "Bound" && pvc.DeletionTimestamp.Time.IsZero() {
	if pvc.Status.Phase == "Bound" && pvc.DeletionTimestamp == nil {
		val, exist := pvcItemsConcurrent.Get(pvc.Name)

		item := val.(*pvcItem)
		if !exist {
			newItem := &pvcItem{
				name:       pvc.Name,
				createTime: pvc.CreationTimestamp.Time,
				boundTime:  start,

				pvName: pvc.Spec.VolumeName,
			}

			//pvcItems[pvc.Name] = newItem
			pvcItemsConcurrent.Set(pvc.Name, newItem)

			klog.Infof("Items add pvc: %+v", newItem)
		} else {
			item.boundTime = start
			item.pvName = pvc.Spec.VolumeName
			item.createTime = pvc.CreationTimestamp.Time

			klog.Infof("Items update pvc: %v", item)
		}
	} else {
		klog.Infof("Skip pvc update: %v, %v", pvc.Name, pvc.Status.Phase)
	}

	//klog.Infoln("Update pvc: ", pvc.Name, pvc.Status.Phase)
	data, _ := json.Marshal(pvc)
	klog.Infoln(string(data))
	return
}
func pvcDeleteFunc(obj interface{}) {
	start := time.Now()

	pvc, ok := obj.(*v1.PersistentVolumeClaim)
	if !ok {
		klog.Errorf("Receive not pvc")
		return
	}
	if pvc.DeletionTimestamp == nil {
		klog.Errorf("Pvc isn't deleting")
		return
	}

	//item, exist := pvcItems[pvc.Name]
	val, exist := pvcItemsConcurrent.Get(pvc.Name)
	if !exist {
		klog.Errorf("Pvc items have no %s", pvc.Name)
	} else {
		item := val.(*pvcItem)
		item.deleteTime = pvc.DeletionTimestamp.Time
		item.deletedTime = start
	}

	klog.Infoln("Del pvc: ", pvc.Name, pvc.Status.Phase)
	data, _ := json.Marshal(pvc)
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
	start := time.Now()

	pv, ok := obj.(*v1.PersistentVolume)
	if !ok {
		klog.Errorf("Receive not pv")
		return
	}

	pvcName := pv.Spec.ClaimRef.Name
	val, exist := pvcItemsConcurrent.Get(pvcName)
	item := val.(*pvcItem)
	if !exist {
		klog.Errorf("Pvc items have no pvc: %s", pvcName)
	} else {
		item.pvDeleteTime = pv.DeletionTimestamp.Time
		item.pvDeletedTime = start
	}

	klog.Infoln("Del pv: ", pv.Name, pv.Status.Phase)
	data, _ := json.Marshal(pv)
	klog.Infoln(string(data))
	return
}