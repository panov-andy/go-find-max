package clickhouse

func NewCollector(maxSize int) Collector {
	return Collector{
		maxSize:  maxSize,
		corteges: []Cortege{},
	}
}

type Collector struct {
	maxSize  int
	corteges []Cortege
}

func (c *Collector) process(cortege Cortege) {
	arrSize := len(c.corteges)

	if arrSize < c.maxSize {
		c.corteges = append(c.corteges, cortege)
	} else {
		if c.corteges[arrSize-1].Rate < cortege.Rate {
			c.corteges[arrSize-1] = cortege
		}
	}

	sortByRate(c.corteges)
}

func (c *Collector) GetResult() []Cortege {
	return c.corteges
}
