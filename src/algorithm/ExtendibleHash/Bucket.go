package ExtendibleHash

import "GoSQL/src/utils"

type bucket struct {
	size  uint8
	depth uint32
	dir   []utils.Pair
}
