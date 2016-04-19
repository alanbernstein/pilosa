package pilosa

import (
	"fmt"
	"log"
	"strings"
)

type Biclique struct {
	Tiles []uint64
	Count uint64 // number of profiles
	Score uint64 // num tiles * Count
}

type BCList []Biclique

func (bcl BCList) Len() int {
	return len(bcl)
}
func (bcl BCList) Less(i, j int) bool {
	return bcl[i].Score > bcl[j].Score
}

func (bcl BCList) Swap(i, j int) {
	bcl[i], bcl[j] = bcl[j], bcl[i]
}

func (f *Fragment) MaxBiclique(n int) chan Biclique {
	f.mu.Lock()
	f.cache.Invalidate()
	pairs := f.cache.Top() // slice of bitmapPairs
	f.mu.Unlock()

	topPairs := pairs
	if n < len(pairs) {
		topPairs = pairs[:n]
	}

	results := make(chan []BitmapPair, 100)
	go func() {
		bicliqueFind(topPairs, nil, []BitmapPair{}, topPairs, []BitmapPair{}, results, 0)
		close(results)
	}()

	bicliques := make(chan Biclique, 100)
	// read results and convert each []BitmapPair to Biclique
	go func() {
		for bmPairs := range results {
			tiles := getTileIDs(bmPairs)
			bicliqueBitmap := intersectPairs(bmPairs)
			bicliques <- Biclique{
				Tiles: tiles,
				Count: bicliqueBitmap.Count(),
				Score: uint64(len(tiles)) * bicliqueBitmap.Count(),
			}
		}
		close(bicliques)
	}()
	return bicliques
}

func maxBiclique(topPairs []BitmapPair) []Biclique {
	// generate every permutation of topPairs
	pairChan := make(chan []BitmapPair, 10)
	ps := []BitmapPair(topPairs)
	go generateCombinations(ps, pairChan)
	var minCount uint64 = 1

	results := make([]Biclique, 100)
	i := 0

	for comb := range pairChan {
		fmt.Println("Got a combination! ", comb)
		// feed each to intersectPairs
		ret := intersectPairs(comb)
		if ret.Count() > minCount {
			tiles := getTileIDs(comb)
			results[i] = Biclique{
				Tiles: tiles,
				Count: ret.Count(),
				Score: uint64(len(tiles)) * ret.Count(),
			}
			i++
			if i > 99 {
				break
			}
		}
	}
	return results
}

var DEPTH_IND = "_ "

func bicliqueFind(G []BitmapPair, L *Bitmap, R []BitmapPair, P []BitmapPair, Q []BitmapPair, results chan []BitmapPair, depth int) {
	// G is topPairs
	// L should start with all bits set (L == U) (it will actually start nil, and we'll special case it below)
	// R starts empty
	// P starts as topPairs (all tiles are candidates)
	// Q starts empty
	if L == nil {
		log.Printf("%vCall bicliqueFind L=Profiles[1 2 3], R=%v, P=%v, Q=%v", strings.Repeat(DEPTH_IND, depth), R, P, Q)
	} else {
		log.Printf("%vCall bicliqueFind L=%v, R=%v, P=%v, Q=%v", strings.Repeat(DEPTH_IND, depth), L, R, P, Q)
	}
	for len(P) > 0 {
		// P ← P\{x};
		x := P[0]
		P = P[1:]
		log.Printf("%vSelect x = Tile:%v from P", strings.Repeat(DEPTH_IND, depth), x.ID)
		log.Printf("%vP ← P\\{x}; P = %v", strings.Repeat(DEPTH_IND, depth), P)

		// R' ← R ∪ {x};
		newR := append(R, x)
		log.Printf("%vR' ← R ∪ {x}; R' = %v", strings.Repeat(DEPTH_IND, depth), newR)

		//  L' ← {u ∈ L | (u, x) ∈ E(G)};
		var newL *Bitmap
		if L == nil {
			newL = x.Bitmap.Clone()
		} else {
			newL = L.Clone()
		}
		newL = newL.Intersect(x.Bitmap)
		log.Printf("%vL' ← {u ∈ L | (u, x) ∈ E(G)}; L' = %v", strings.Repeat(DEPTH_IND, depth), newL)
		newLcnt := newL.BitCount()

		// P' ← ∅; Q' ← ∅;
		newP := []BitmapPair{}
		newQ := []BitmapPair{}

		// Check maximality.
		isMaximal := true
		log.Printf("%vLooping over Q=%v to see if we can use Observation 4", strings.Repeat(DEPTH_IND, depth), Q)
		for _, v := range Q {
			// get the neighbors of v in L'
			neighbors := v.Bitmap.Intersect(newL)
			ncnt := neighbors.BitCount()
			log.Printf("%v  profiles shared by L' and %v are %v", strings.Repeat(DEPTH_IND, depth), v, neighbors)
			// Observation 4: end of branch
			if ncnt == newLcnt {
				isMaximal = false
				log.Printf("%v    Observation 4: a Tile in Q has all profiles in L', not maximal", strings.Repeat(DEPTH_IND, depth))
				break
			} else if ncnt > 0 {
				newQ = append(newQ, v)
				log.Printf("%v    Q' ← Q' ∪ {v}; Q' = %v", strings.Repeat(DEPTH_IND, depth), newQ)
			}
		}

		if isMaximal {
			log.Printf("%v%v, %v is a maximal candidate - check rest of P to expand to maximal", strings.Repeat(DEPTH_IND, depth), newL, newR)
			for _, v := range P {
				// get the neighbors of v in L'
				neighbors := v.Bitmap.Intersect(newL)
				ncnt := neighbors.BitCount()
				log.Printf("%v  Checking %v - neighbors = %v", strings.Repeat(DEPTH_IND, depth), v, neighbors)
				// Observation 3: expand to maximal
				if ncnt == newLcnt {
					log.Printf("%v    R' ← R' ∪ {v}; %v shares all profiles, appending to R'", strings.Repeat(DEPTH_IND, depth), v)
					newR = append(newR, v)
				} else if ncnt > 0 {
					log.Printf("%v    P' ← P' ∪ {v}; %v shares some profiles - appending to P' for use in recursive call", strings.Repeat(DEPTH_IND, depth), v)
					// keep vertice adjacent to some vertex in newL
					newP = append(newP, v)
				}
			}
			// report newR as maximal biclique
			results <- newR
			bicliqueBitmap := intersectPairs(newR)
			log.Printf("%vReporting %v, %v, as maximal", strings.Repeat(DEPTH_IND, depth), newR, bicliqueBitmap)
			if len(newP) > 0 {
				bicliqueFind(G, newL, newR, newP, newQ, results, depth+1)
				log.Printf("%vReturn from recursive call.", strings.Repeat(DEPTH_IND, depth))
			}
		}
		Q = append(Q, x)
		log.Printf("%vQ ← Q ∪ {x}; Q = %v", strings.Repeat(DEPTH_IND, depth), Q)
	}
}

func getTileIDs(pairs []BitmapPair) []uint64 {
	tileIDs := make([]uint64, len(pairs))
	for i := 0; i < len(pairs); i++ {
		tileIDs[i] = pairs[i].ID
	}
	return tileIDs
}

func generateCombinations(pairs []BitmapPair, pairChan chan<- []BitmapPair) {
	gcombs(pairs, pairChan)
	close(pairChan)
}

func gcombs(pairs []BitmapPair, pairChan chan<- []BitmapPair) {
	fmt.Println("gcombs, send to pairChan ", pairs)

	pairChan <- pairs

	if len(pairs) == 1 {
		return
	}
	for i := 0; i < len(pairs); i++ {
		pairscopy := make([]BitmapPair, len(pairs))
		copy(pairscopy, pairs)
		ps := append(pairscopy[:i], pairscopy[i+1:]...)

		gcombs(ps, pairChan)
	}
}

// intersectPairs generates a bitmap which represents all profiles which have all of the tiles in pairs
func intersectPairs(pairs []BitmapPair) *Bitmap {
	result := pairs[0].Bitmap.Clone()
	for i := 1; i < len(pairs); i++ {
		result = result.Intersect(pairs[i].Bitmap)
	}
	result.SetCount(result.BitCount())
	return result
}
