package mr

type KVPair struct {
	Key   string
	Value string
}

type FileInfo struct {
	Filename string
	Contents []byte
}

type Mapfn = func(string, string) []KVPair
type Reducefn = func(string, []string) string

type MTask struct {
	ID      int
	Payload FileInfo
}

type RTask struct {
	Key   string
	Value []string
}
