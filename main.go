package main

import (
	"errors"
	properties "github.com/dmotylev/goproperties"
	"github.com/ogier/pflag"
	r "github.com/robxu9/reservoir"
	"log"
	"os"
	"path/filepath"
)

const (
	MAJOR_VERSION = 1
	MINOR_VERSION = 1
	PATCH_VERSION = 0

	CONFIG_WORKER_DIR = "workers"
)

// Flags
var helpFlag bool
var versionFlag bool
var verboseFlag bool
var workerDirFlag string

func init() {
	pflag.BoolVar(&helpFlag, "help", false, "Show help.")
	pflag.BoolVar(&versionFlag, "version", false, "Shows the current version of Reservoir.")
	pflag.BoolVar(&verboseFlag, "verbose", false, "Be very verbose.")
	pflag.StringVar(&workerDirFlag, "workers", CONFIG_WORKER_DIR, "Use directory for worker configuration files.")
}

// STOP SWITCH
var shutdown chan bool

func main() {
	pflag.Parse()

	if helpFlag {
		pflag.PrintDefaults()
		return
	}

	if versionFlag {
		log.Printf("This is version %d.%d-%d.", MAJOR_VERSION, MINOR_VERSION, PATCH_VERSION)
		return
	}

	log.Printf("Starting up version %d.%d-%d...\n", MAJOR_VERSION, MINOR_VERSION, PATCH_VERSION)

	// Read configuration
	_, err := os.Open(workerDirFlag)
	if err != nil {
		if err == os.ErrNotExist || err == os.ErrInvalid {
			err = os.Mkdir(workerDirFlag, os.ModeDir)
			if err != nil {
				log.Panicf("Could not create worker dir: %s", err)
			}
		} else {
			log.Panicf("Error accessing worker directory: %s", err)
		}
	}

	log.Printf("Reading worker configuration:\n")
	fileWalkErr := filepath.Walk(workerDirFlag, visit)

	if fileWalkErr != nil {
		log.Panicf("There seems to have been a problem reading configuration: %s\n", fileWalkErr)
	}

	log.Printf("Starting up scheduler...\n")
	r.Scheduler_Run()
	if r.SchedulerStatus == 0 {
		log.Panicf("Scheduler failed to start!\n")
	}

	log.Printf("Ping and Queue up Workers...\n")
	// Establish Ping GoRoutine and add them to Scheduler

	log.Printf("Now active.\n")

	<-shutdown

	log.Printf("Shutdown process starting... Running Exit Tasks.")
}

func visit(path string, f os.FileInfo, err error) error {
	if err != nil {
		return err
	}

	if f.IsDir() { // Ignore directories
		return nil
	}

	log.Printf("\tLoading configuration %s...", path)
	props, err := properties.Load(path)

	if err != nil {
		return err
	}

	workername := props.GetString("name", "")
	workerhost := props.GetString("host", "")
	workerprocesses := props.GetUint("subworkers", 2)

	if workername == "" || workerhost == "" || workerprocesses <= 0 {
		return errors.New("Cannot initialise worker: \"name\" and/or \"host\" is empty, or \"subworkers\" <= 0!")
	}

	var counter uint64

	for counter = 0; counter < workerprocesses; counter++ {
		r.AddWorker(&r.Worker{
			workername,
			counter,
			workerhost,
			nil,
			"",
			true,
		})
	}

	return nil
}
