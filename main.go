package main

import (
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"plugin"
	"time"

	"github.com/caleberi/map_reduce/mr"
)

func main() {
	pluginFile := flag.String("plugin_file", "./build/fc.so", "name of plugin to run on the application")
	filePath := flag.String("file_path", "./pg-dorian_gray.txt", "file path / glob pattern for files")
	flag.Parse()
	mapfn, reducefn, err := loadPlugin(*pluginFile)

	if err != nil {
		log.Fatalf("error loading plugin : %v", err)
	}

	//  clean file path
	*filePath = filepath.Clean(*filePath)
	//  extract the directory path
	dirName := filepath.Dir(*filePath)
	// base filename
	baseFp := filepath.Base(*filePath)

	workloads := []mr.FileInfo{}

	if err := filepath.WalkDir(dirName, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return fs.SkipDir
		}

		matches, err := filepath.Match(baseFp, filepath.Base(path))

		if err != nil {
			return err
		}

		log.Printf("Comparing baseFp : %s to path :  %s (matches:%v)(regular_file:%v)", baseFp, filepath.Base(path), matches, d.Type().IsRegular())
		if d.Type().IsRegular() && matches {
			fs, err := os.Open(path)
			defer func() {
				fs.Close()
			}()
			if err != nil {
				log.Printf("error occured when opening file path : %v", err)
				return err
			}
			contents, err := io.ReadAll(fs)
			if err != nil {
				log.Printf("error occured when opening file path : %v", err)
				return err
			}
			workloads = append(workloads, mr.FileInfo{
				Filename: filepath.Base(path),
				Contents: contents,
			})
		}
		return nil
	}); err != nil {
		log.Fatalf("error retrieve files : %v", err)
	}

	mw, rw := 2, 2
	coordinator := mr.NewCoordinator(mw, rw).
		RegisterMapFn(mapfn).
		RegisterReduceFn(reducefn).
		LoadWorkloads(workloads)

	startTime := time.Now()
	if err := coordinator.Run(); err != nil {
		log.Fatalf("error occurred while running coordinator : %s", err)
	}
	timeTaken := time.Since(startTime).Milliseconds()
	totalByteProcessed := 0
	for _, fileinfo := range workloads {
		totalByteProcessed += len([]byte(fileinfo.Contents))
	}

	fmt.Printf("Coordinator Runtime [%dms] {mapWorker:%d,reduceWorker:%d} {TotalFileProcessed:%.1fmb} \n", timeTaken, mw, rw, float64(totalByteProcessed/(1024*1024)))
}

func loadPlugin(filename string) (mr.Mapfn, mr.Reducefn, error) {
	p, err := plugin.Open(filename)

	if err != nil {
		return nil, nil, err
	}

	xmapfn, err := p.Lookup("Map")
	if err != nil {
		return nil, nil, fmt.Errorf("cannot look up map function in %s", filename)
	}
	xreducefn, err := p.Lookup("Reduce")
	if err != nil {
		return nil, nil, fmt.Errorf("cannot look up reduce function in %s", filename)
	}
	// assert if the type signature is correct
	mapfn, reducefn := xmapfn.(mr.Mapfn), xreducefn.(mr.Reducefn)

	return mapfn, reducefn, nil
}
