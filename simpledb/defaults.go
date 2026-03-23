package simpledb

import "time"

const (
	defaultLogfileName  = "logfile"
	defaultFilename     = "datafile"
	defaultNumbuffs     = 100
	defaultNumwaits     = 10
	defaultCkpInterval  = time.Second * 10
	defaultPgclInterval = time.Second * 10
	defaultPageCleanerBatchSize = int32(100)
)
