// package mr

// import (
// 	"log"
// 	"os"
// 	"sync"
// )

// type Coordinator struct {
// 	mapWorkers, reduceWorkers int
// 	taskCh                    chan MTask
// 	mapCh                     chan []KVPair
// 	reduceCh                  chan RTask
// 	resultCh                  chan string
// 	mapfn                     Mapfn
// 	reducefn                  Reducefn
// 	workload                  []FileInfo
// 	wg                        sync.WaitGroup
// 	tmpStore                  map[string][]string
// 	// mu                        sync.Mutex
// 	m sync.Map
// }

// func NewCoordinator(numberOfMapWorker int, numberOfReduceWorker int) *Coordinator {
// 	return &Coordinator{
// 		mapWorkers:    numberOfMapWorker,
// 		reduceWorkers: numberOfReduceWorker,
// 		taskCh:        make(chan MTask),
// 		resultCh:      make(chan string, numberOfReduceWorker),
// 		mapCh:         make(chan []KVPair, numberOfMapWorker),
// 		reduceCh:      make(chan RTask, numberOfReduceWorker),
// 		workload:      make([]FileInfo, 0),
// 		tmpStore:      make(map[string][]string),
// 		wg:            sync.WaitGroup{},
// 	}
// }

// func (c *Coordinator) RegisterMapFn(fn Mapfn) *Coordinator { c.mapfn = fn; return c }

// func (c *Coordinator) RegisterReduceFn(fn Reducefn) *Coordinator { c.reducefn = fn; return c }

// func (c *Coordinator) LoadWorkloads(workloads []FileInfo) *Coordinator {
// 	c.workload = workloads
// 	return c
// }

// func (c *Coordinator) Run() error {

// 	var (
// 		mapWorkersWG, reduceWorkersWG sync.WaitGroup
// 		mapDoneCh                     = make(chan bool, 1)

// 		reduceDoneCh = make(chan bool, 1)
// 	)

// 	// assign map workers
// 	c.startMapWorkers(&mapWorkersWG, mapDoneCh)
// 	// assign reduce workers
// 	c.startReduceWorkers(&reduceWorkersWG, reduceDoneCh)

// 	c.sendOutTaskInformation()

// 	c.sendReduceOutputForProcessing(&mapWorkersWG)
// 	// assign  an intermediate  collaborator
// 	c.collateMapResultForReduction(mapDoneCh)

// 	c.monitorMapAndReduceWorkers(mapDoneCh, reduceDoneCh)

// 	oname := "./mr-out-0"

// 	ofile, err := os.Create(oname)

// 	if err != nil {
// 		return err
// 	}

// 	defer ofile.Close()

// 	for result := range c.resultCh {
// 		if _, err := ofile.WriteString(result + "\n"); err != nil {
// 			return err
// 		}
// 	}

// 	c.wg.Wait()

// 	return nil
// }

// func (c *Coordinator) monitorMapAndReduceWorkers(mapDoneCh chan bool, reduceDoneCh chan bool) {
// 	c.wg.Add(1)
// 	go func() {
// 		defer c.wg.Done()
// 		for i := c.mapWorkers; i > 0; i-- {
// 			<-mapDoneCh
// 		}
// 		log.Println("closing c.MapCh")
// 		close(c.mapCh)
// 	}()

// 	c.wg.Add(1)
// 	go func() {
// 		defer c.wg.Done()
// 		for i := c.reduceWorkers; i > 0; i-- {
// 			<-reduceDoneCh
// 		}
// 		log.Println("closing c.MapCh")
// 		close(c.resultCh)
// 	}()
// }

// func (c *Coordinator) sendOutTaskInformation() {
// 	c.wg.Add(1)
// 	go func() {
// 		defer c.wg.Done()
// 		for id, fileInfo := range c.workload {
// 			task := MTask{
// 				ID:      id,
// 				Payload: fileInfo,
// 			}
// 			c.taskCh <- task
// 		}
// 		close(c.taskCh)
// 	}()
// }

// func (c *Coordinator) collateMapResultForReduction(mapDoneCh chan bool) {
// 	c.wg.Add(1)
// 	go func() {
// 		defer c.wg.Done()
// 		for val := range c.mapCh {
// 			for _, v := range val {
// 				c.aggregate(v)
// 			}
// 		}
// 		mapDoneCh <- true
// 	}()
// }

// func (c *Coordinator) sendReduceOutputForProcessing(mapWorkersWG *sync.WaitGroup) {
// 	c.wg.Add(1)
// 	go func() {
// 		defer c.wg.Done()
// 		defer close(c.reduceCh)
// 		mapWorkersWG.Wait()
// 		log.Println("Waited for this")
// 		c.m.Range(func(k, v any) bool {
// 			ks := k.(string)
// 			vs := v.([]string)
// 			log.Println("HERe")
// 			c.reduceCh <- RTask{Key: ks, Value: vs}
// 			log.Printf("Send KV from store k: %v v:%v ", ks, vs)
// 			return true
// 		})
// 	}()
// }

// func (c *Coordinator) startMapWorkers(mapWorkersWG *sync.WaitGroup, mapDoneCh chan bool) {
// 	for i := 0; i < c.mapWorkers; i++ {
// 		id := i + 1
// 		c.wg.Add(1)
// 		mapWorkersWG.Add(1)
// 		go func(id int) {
// 			defer c.wg.Done()
// 			defer mapWorkersWG.Done()
// 			for task := range c.taskCh {
// 				mappedResult := c.mapfn(task.Payload.Filename, string(task.Payload.Contents))
// 				c.mapCh <- mappedResult
// 			}
// 			mapDoneCh <- true
// 			log.Println("Done with  c.taskCh")
// 		}(id)
// 	}
// }

// func (c *Coordinator) startReduceWorkers(reduceWorkersWG *sync.WaitGroup, reduceDoneCh chan bool) {
// 	for i := 0; i < c.reduceWorkers; i++ {
// 		id := i + 1
// 		c.wg.Add(1)
// 		reduceWorkersWG.Add(1)
// 		go func(id int) {
// 			defer c.wg.Done()
// 			defer reduceWorkersWG.Done()
// 			for val := range c.reduceCh {
// 				log.Printf("Reducer Worker[%d] -> Got %v ", id, val)
// 				c.resultCh <- c.reducefn(val.Key, val.Value)
// 			}
// 			reduceDoneCh <- true

// 		}(id)
// 	}
// }

// func (c *Coordinator) aggregate(v KVPair) {
// 	if existingVal, ok := c.m.Load(v.Key); ok {
// 		pa := existingVal.([]string)
// 		pa = append(pa, v.Value)
// 		c.m.Store(v.Key, pa)
// 	} else {
// 		c.m.Store(v.Key, []string{v.Value})
// 	}
// }

package mr

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	mapWorkers, reduceWorkers int
	taskCh                    chan MTask
	mapCh                     chan []KVPair
	reduceCh                  chan RTask
	resultCh                  chan string
	mapfn                     Mapfn
	reducefn                  Reducefn
	workload                  []FileInfo
	wg                        sync.WaitGroup
	m                         sync.Map
	mapResult                 []KVPair
}

func NewCoordinator(numberOfMapWorker int, numberOfReduceWorker int) *Coordinator {
	return &Coordinator{
		mapWorkers:    numberOfMapWorker,
		reduceWorkers: numberOfReduceWorker,
		taskCh:        make(chan MTask, 1),
		resultCh:      make(chan string),
		mapCh:         make(chan []KVPair, 1),
		reduceCh:      make(chan RTask, 1),
		workload:      make([]FileInfo, 0),
		wg:            sync.WaitGroup{},
		m:             sync.Map{},
		mapResult:     []KVPair{},
	}
}

func (c *Coordinator) RegisterMapFn(fn Mapfn) *Coordinator {
	c.mapfn = fn
	return c
}

func (c *Coordinator) RegisterReduceFn(fn Reducefn) *Coordinator {
	c.reducefn = fn
	return c
}

func (c *Coordinator) LoadWorkloads(workloads []FileInfo) *Coordinator {
	c.workload = workloads
	return c
}

func (c *Coordinator) Run() error {

	var (
		mapWorkersWG, reduceWorkersWG sync.WaitGroup
		mapDoneCh                     = make(chan bool, 1) // Signal map completion
		reduceDoneCh                  = make(chan bool, 1) // Signal reduce completion
	)

	// Assign map workers
	c.startMapWorkers(&mapWorkersWG, mapDoneCh)

	// Assign reduce workers
	c.startReduceWorkers(&reduceWorkersWG, reduceDoneCh)

	// Send out task information
	c.sendOutTaskInformation()

	// Send reduce tasks from temporary slice
	c.sendReduceOutputForProcessing(&mapWorkersWG)

	c.monitorMapAndReduceWorkers(mapDoneCh, reduceDoneCh)

	// Write output to file
	oname := "./mr-out-0"
	ofile, err := os.Create(oname)

	if err != nil {
		return err
	}

	defer ofile.Close()

	for result := range c.resultCh {
		if _, err := ofile.WriteString(result + "\n"); err != nil {
			return err
		}
	}

	log.Println("About to close all jobs")

	c.wg.Wait()

	log.Println("Done with all jobs")

	return nil
}

func (c *Coordinator) sendOutTaskInformation() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer close(c.taskCh) // Close task channel after sending all tasks
		for id, fileInfo := range c.workload {
			task := MTask{
				ID:      id,
				Payload: fileInfo,
			}
			log.Printf("Sending task for file: %s\n", fileInfo.Filename)
			c.taskCh <- task
		}
	}()
}

func (c *Coordinator) sendReduceOutputForProcessing(mapWorkersWG *sync.WaitGroup) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		// Use temporary slice to iterate and send reduce tasks
		for kvPairs := range c.mapCh {
			for _, kvPair := range kvPairs {
				fmt.Printf("maps : %s %v\n", kvPair.Key, kvPair.Value)
				c.aggregate(kvPair)
			}
		}

		mapWorkersWG.Wait() // Wait for all map workers to complete

		c.m.Range(func(k, v any) bool {
			ks := k.(string)
			vs := v.([]string)
			log.Printf("Aggregated result: Key: %s, Values: %v\n", ks, len(vs))
			c.reduceCh <- RTask{Key: ks, Value: vs}
			return true
		})

		log.Println("closing c.reduceCh")
		close(c.reduceCh)
	}()
}

func (c *Coordinator) startMapWorkers(mapWorkersWG *sync.WaitGroup, mapDoneCh chan bool) {
	for i := 0; i < c.mapWorkers; i++ {
		id := i + 1
		c.wg.Add(1)
		mapWorkersWG.Add(1)
		go func(id int) {
			defer c.wg.Done()
			defer mapWorkersWG.Done()
			for task := range c.taskCh {
				log.Printf("Map Worker[%d] -> Received task for file: %s\n", id, task.Payload.Filename)
				log.Printf("Map Worker[%d] -> Received task for content: %s\n", id, string(task.Payload.Contents))
				mappedResult := c.mapfn(task.Payload.Filename, string(task.Payload.Contents))
				if mappedResult != nil {
					c.mapCh <- mappedResult
					log.Printf("Map Worker[%d] -> Mapped result for file: %s\n", id, task.Payload.Filename)
				} else {
					log.Printf("Map Worker[%d] -> Error mapping file: %s\n", id, task.Payload.Filename)
				}
			}
			mapDoneCh <- true
			log.Println("Done with map worker", id)
		}(id)
	}
}

func (c *Coordinator) startReduceWorkers(reduceWorkersWG *sync.WaitGroup, reduceDoneCh chan bool) {
	for i := 0; i < c.reduceWorkers; i++ {
		id := i + 1
		c.wg.Add(1)
		reduceWorkersWG.Add(1)
		go func(id int) {
			defer c.wg.Done()
			defer reduceWorkersWG.Done()
			for val := range c.reduceCh {
				log.Printf("Reducer Worker[%d] -> Got %v ", id, val)
				c.resultCh <- c.reducefn(val.Key, val.Value)
			}
			reduceDoneCh <- true
			log.Println("Done with reduce worker", id)
		}(id)
	}
}

func (c *Coordinator) aggregate(v KVPair) {
	existingVal, ok := c.m.Load(v.Key)
	if ok {
		pa := existingVal.([]string)
		pa = append(pa, v.Value)
		c.m.Store(v.Key, pa)
	} else {
		c.m.Store(v.Key, []string{v.Value})
	}
}

func (c *Coordinator) monitorMapAndReduceWorkers(mapDoneCh, reduceDoneCh <-chan bool) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for i := c.mapWorkers; i > 0; i-- {
			<-mapDoneCh
		}
		time.Sleep(1000 * time.Millisecond)
		log.Println("closing c.mapCh")
		close(c.mapCh)
	}()
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for i := c.reduceWorkers; i > 0; i-- {
			<-reduceDoneCh
		}
		time.Sleep(10 * time.Millisecond)
		log.Println("closing c.result")
		close(c.resultCh)
	}()
}
