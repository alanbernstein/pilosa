package roaring

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

// randomBitmap generates a random Bitmap with Ncontainers.
// The type and cardinality of each container is random; keys are sequential.
func randomBitmap(Ncontainers int) *Bitmap {
	b := &Bitmap{
		keys:       make([]uint64, Ncontainers),
		containers: make([]*container, Ncontainers),
	}

	for n := 0; n < Ncontainers; n++ {
		b.keys[n] = uint64(n)
		// The intent here is to generate containers with uniformly distributed container
		// type, but after b.Optimize(), that won't be the case. As long as we get some
		// amount of each type, this still serves its purpose.
		// The Right Way would be to generate bitsets that get optimized into the correct
		// type, i.e. calling a function randomBitset(N, Nruns) with appropriate arguments.
		switch rand.Intn(3) + 1 {
		case 1:
			// Could be array or RLE.
			b.containers[n] = randomArray(rand.Intn(ArrayMaxSize-1) + 1)
			//Nbits := rand.Intn(ArrayMaxSize-1) + 1
			//Nruns := 10 // needs to be
			//b.containers[n] = randomRunBitset(Nbits, Nruns)
		case 2:
			// Guaranteed bitmap.
			b.containers[n] = randomBitset(rand.Intn(65536-ArrayMaxSize) + ArrayMaxSize)
			//Nbits := rand.Intn(65536-ArrayMaxSize) + ArrayMaxSize
			//Nruns := rand.Intn()
			//b.containers[n] = randomRunBitset(Nbits, Nruns)
		case 3:
			// Probably RLE.
			b.containers[n] = randomRunset(rand.Intn(RunMaxSize-1) + 1)
		}
	}
	b.Optimize()
	return b
}

// randomPositiveIntsFixedSum generates a sorted slice of positive
// integers of length `num`, such that their sum is `sum`.
func randomPositiveIntsFixedSum(num, sum int) []int {
	// Better performance may be possible for small values of num,
	// by using something faster than rand.Perm.

	// Generate num-1 ints in [0, sum-2].
	vals := rand.Perm(sum - 1)[0 : num-1]

	// Shift to [1, sum-1] (prevents any duplicate of 0 and sum).
	for i := 0; i < num-1; i++ {
		vals[i]++
	}

	// Append 0 and sum to the list.
	vals = append(vals, 0)
	vals = append(vals, sum)
	sort.Ints(vals)

	// Now we have an increasing list of ints like [0 a1 a2 ... sum],
	// return the diff.
	deltas := make([]int, num)
	for n := 0; n < num; n++ {
		deltas[n] = vals[n+1] - vals[n]
	}
	return deltas
}

// randomContainerType generates one random container, with equal probability of
// each of the three types. The container will be of RLE type until
// Optimize() is called, after which it will be the expected type.
func randomContainerByType(ctype byte) *container {
	// Refer to docs/roaring-container-decision-diagram.png.
	// Each container type corresponds to a simple polygonal region of
	// (cardinality-runCount) space.
	// Divide each region into combinations of triangle and rectangles,
	// then choose one of those subregions, and pick a point in it.
	// (TODO: weight subregion choice by its relative area)
	// Then use randomContainer(Nbits, Nruns) to produce a bitset at that
	// point in the space.
	var Nbits, Nruns int
	switch ctype {
	case ContainerArray: // 1
		// triangle: (0, 0), (aMax, 0), (aMax, aMax)
		Nbits, Nruns = randomPointInTriangle(ArrayMaxSize, 0, 0, ArrayMaxSize)
	case ContainerBitmap: // 2
		// rectangle: (aMax, rMax), (65536, rMax), (65546, aMax), (aMax, aMax)
		//            area = (65536 - aMax) * (aMax - rMax)
		// rectangle: (32768, aMax), (65536, aMax), (65536, 32768), (32768, 32768)
		//            area = 32768 * (32768-aMax)
		// triangle: (aMax, aMax), (32768, aMax), (32768, 32768)
		//           area = (32768-aMax) * (32768-aMax)/2
		switch rand.Intn(2) {
		case 0:
			Nbits, Nruns = randomPointInTriangle(ArrayMaxSize, 0, ArrayMaxSize, RunMaxSize)
		case 1:
			Nbits, Nruns = randomPointInRectangle(ArrayMaxSize, 0, 65535, RunMaxSize)
		}
	case ContainerRun: // 3
		// triangle: (0, 0), (aMax, 0), (aMax, rMax)
		//           area = aMax*rMax/2
		// rectangle: (aMax, 0), (65536, 0), (65536, rMax), (aMax, rMax)
		//             area = (65536-aMax -? 1) * rMax
		switch rand.Intn(2) {
		case 0:
			Nbits, Nruns = randomPointInTriangle(ArrayMaxSize, 0, ArrayMaxSize, RunMaxSize)
		case 1:
			Nbits, Nruns = randomPointInRectangle(ArrayMaxSize, 0, 65535, RunMaxSize)
		}
	}
	return randomContainer(Nbits, Nruns)
}

// randomPointInRectangle generates one random 2D point in the
// interior of the rectangle defined by (x1, y1), (x2, y2).
func randomPointInRectangle(x1, y1, x2, y2 int) (int, int) {
	// The parallelogram is ABDC where
	// A = (x1, y1)
	// B = (x1, y2)
	// D = (x2, y2)
	// C = (x2, y1)
	x := int(float64(x2-x1)*rand.Float64()) + x1
	y := int(float64(y2-y1)*rand.Float64()) + y1
	return x, y
}

// randomPointInTriangle generates one random 2D point in the
// interior of the triangle defined by (x1, y1), (x2, y2).
func randomPointInTriangle(x1, y1, x2, y2 int) (int, int) {
	// The triangle is ABC where
	// A = (0, 0)
	// B = (x1, y1)
	// C = (x2, y2)
	// Points inside the corresponding parallelogram but outside
	// the triangle can simply be "folded back" into the triangle.
	a1, a2 := rand.Float64(), rand.Float64()
	if a1+a2 > 1 {
		a1 = 1 - a1
		a2 = 1 - a2
	}
	x := int(a1*float64(x1) + a2*float64(x2))
	y := int(a1*float64(y1) + a2*float64(y2))
	return x, y
}

// randomRunBitset generates a container with N bits set randomly, such
// that it contains are Nruns runs of ones. This can be used to generate
// a random container of known type.
func randomContainer(N, Nruns int) *container {
	// 1. generate x = [Nruns numbers summing to N]
	// 2. generate y = [Nruns numbers summing to 65536-N]
	// 3. [x0, x0+y0], [x0+y0+x1], [x0+y0+x1+y1], ...
	// optionally: twiddle things so we get a mix of containers starting/ending with runs vs not

	// Algorithm requires generating a run container, but Optimize() will
	// convert it as necessary.
	c := &container{n: N}
	c.runs = make([]interval16, Nruns)

	x := randomPositiveIntsFixedSum(Nruns, N)
	y := randomPositiveIntsFixedSum(Nruns, 65536-N)
	var start, last uint16
	for i := 0; i < Nruns; i++ {
		start += uint16(x[i])
		last = start + uint16(y[i])
		c.runs[0] = interval16{start: start, last: last}
	}
	return c
}

func TestRandomContainer(t *testing.T) {
	for i := 0; i < 10; i++ {
		c := randomContainer(100, 5)
		fmt.Printf("%2d. %v %v %v\n", i, c, c.n, c.countRuns())
		c.Optimize()
		fmt.Printf("    %v %v %v\n", c, c.n, c.countRuns())
	}
}

func TestRandom(t *testing.T) {
	for i := 0; i < 10; i++ {
		vals := randomPositiveIntsFixedSum(10, 100)
		sum := 0
		for _, v := range vals {
			sum += v
		}
		fmt.Printf("sum(%v) = %v\n", vals, sum)
	}
}

// randomArray generates an array container with N elements.
func randomArray(N int) *container {
	c := &container{n: N}
	vals := rand.Perm(65536)[0:N]
	sort.Ints(vals)
	c.array = make([]uint16, N)
	for n := 0; n < N; n++ {
		c.array[n] = uint16(vals[n])
	}
	return c
}

// randomBitset generates a bitmap container with N elements.
func randomBitset(N int) *container {
	c := &container{n: N}
	vals := rand.Perm(65536)
	c.bitmap = make([]uint64, bitmapN)
	for n := 0; n < N; n++ {
		c.bitmap[vals[n]/64] |= (1 << uint64(vals[n]%64))
	}
	return c
}

// randomRunset generates an RLE container with N runs.
func randomRunset(N int) *container {
	c := &container{}
	vals := rand.Perm(65536)[0 : 2*N]
	sort.Ints(vals)
	c.runs = make([]interval16, N)
	c.n = 0
	for n := 0; n < N; n++ {
		c.runs[n] = interval16{
			start: uint16(vals[2*n]),
			last:  uint16(vals[2*n+1]),
		}
		c.n += vals[2*n+1] - vals[2*n] + 1
	}
	return c
}

func (b *Bitmap) DebugInfo() {
	info := b.Info()
	for n, c := range b.containers {
		fmt.Printf("%3v %5v %6v\n", info.Containers[n].Key, c.n, info.Containers[n].Type)
	}
}

func TestWriteRead(t *testing.T) {
	rand.Seed(5)
	Ncontainers := 10
	iterations := 1
	for i := 0; i < iterations; i++ {
		fmt.Printf("----------\niteration %d\n", i)
		b := randomBitmap(Ncontainers)
		b.DebugInfo()
		b2 := &Bitmap{}

		var buf bytes.Buffer
		_, err := b.WriteTo(&buf)
		if err != nil {
			t.Fatalf("error writing: %v", err)
		}

		err = b2.UnmarshalBinary(buf.Bytes())
		if err != nil {
			t.Fatalf("error unmarshaling: %v", err)
		}

		if !b.Equal(*b2) {
			if !reflect.DeepEqual(b.keys, b2.keys) {
				t.Fatalf("iteration %d key mismatch, exp \n%+v, got \n%+v", i, b.keys, b2.keys)
			}
			if len(b.containers) != len(b2.containers) {
				t.Fatalf("iteration %d container count mismatch, exp \n%+v, got \n%+v", i, len(b.containers), len(b2.containers))
			}

			for n, c := range b.containers {
				if !c.Equal(*b2.containers[n]) {
					t.Fatalf("iteration %d container mismatch, exp \n%+v, got \n%+v", i, b.containers[n], b2.containers[n])
				}
			}

			t.Fatalf("iteration %d unknown bitmap mismatch, exp \n%+v, got \n%+v", i, b, b2)

		}
	}

	// TODO check Count and TopN counts
}
