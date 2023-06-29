package generator

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"math/rand"
	"os"
	"testing"
)

func DISABLED_TestPatternSource_WritePatternedFiles(t *testing.T) {
	rndSrc := rand.NewSource(0)
	rng := rand.New(rndSrc)
	buf := make([]byte, 10*1024*1024)

	for comp := float32(0.0); comp <= 1.0; comp += 0.1 {
		patternFill(buf, rng, comp)
		f, err := os.Create(fmt.Sprintf("/tmp/patternfill-%d.bin", int(comp*100)))
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.Write(buf)
		if err != nil {
			t.Fatal(err)
		}
		err = f.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPatternSource_PatternCompressibility(t *testing.T) {
	rndSrc := rand.NewSource(0)
	rng := rand.New(rndSrc)
	buf := make([]byte, 1024*1024)

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
