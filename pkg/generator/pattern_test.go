package generator

import (
	"bytes"
	"compress/zlib"
	"math/rand"
	"testing"
)

// Unit test which generates a buffer of pattern data and verifies that its actual compressibility
// is within 10% of the target compressibility.
func TestPatternSource_PatternCompressibility(t *testing.T) {
	rndSrc := rand.NewSource(0)
	rng := rand.New(rndSrc)
	buf := make([]byte, 100*1024)

	for comp := float32(0.0); comp <= 1.0; comp += 0.1 {
		patternFill(buf, rng, comp)

		// compress the pattern-filled output buffer
		var destbuf bytes.Buffer
		w := zlib.NewWriter(&destbuf)
		w.Write(buf)
		w.Close()

		// see if its compressed length is close to the target compressibility
		target := float32(len(buf)) * (1.0 - comp)
		actual := float32(len(destbuf.Bytes()))

		// t.Logf("compressibility %0.1f: target %0.0f, actual %0.0f", comp, target, actual)

		if actual < target*0.9 || actual > target*1.1 {
			t.Fatalf("compressed len %0.0f is not within 10%% of target %0.0f", actual, target)
		}
	}
}
