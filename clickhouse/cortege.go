package clickhouse

import "sort"

type Cortege struct {
	Url  string
	Rate int
}

func sortByRate(corteges []Cortege) {
	sort.SliceStable(corteges, func(i, j int) bool {
		return corteges[i].Rate > corteges[j].Rate
	})
}
