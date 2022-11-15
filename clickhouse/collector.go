package clickhouse

import "sync"

func NewCollector(maxSize int) *Collector {
	collector := Collector{
		maxSize:  maxSize,
		corteges: make([]Cortege, maxSize),
		mutex:    sync.Mutex{},
	}

	cort := Cortege{
		Url:  "",
		Rate: -2147483648,
	}
	for i := 0; i < maxSize; i++ {
		collector.corteges[i] = cort
	}

	return &collector
}

type Collector struct {
	maxSize  int
	corteges []Cortege
	mutex    sync.Mutex
}

func (c *Collector) process(cortege *Cortege) {
	minIndex := 0
	min := c.corteges[0].Rate
	//might be avoided
	for i := 1; i < c.maxSize; i++ {
		current := c.corteges[i].Rate
		if current < min {
			minIndex = i
			min = current
		}
	}

	if cortege.Rate > min {
		c.corteges[minIndex] = *cortege
	}
}

func (c *Collector) GetResult() []Cortege {
	return c.corteges
}
