package generator

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
)

func WithPattern() PatternOpts {
	return patternOptsDefaults()
}

// Apply pattern data options.
func (o PatternOpts) Apply() Option {
	return func(opts *Options) error {
		if err := o.validate(); err != nil {
			return err
		}
		opts.pattern = o
		opts.src = newPattern
		return nil
	}
}

func (o PatternOpts) validate() error {
	if o.size <= 4096 {
		// insist on 4K or larger size; hard to ensure any given compression ratio with tiny sizes
		return errors.New("pattern: size <= 4096")
	}
	if o.compressibility < 0 || o.compressibility > 1.0 {
		return fmt.Errorf("pattern: compressibility must be in range [0, 1.0], got %f", o.compressibility)
	}
	return nil
}

// RngSeed will which to a fixed RNG seed to make usage predictable.
func (o PatternOpts) RngSeed(s int64) PatternOpts {
	o.seed = &s
	return o
}

// Size will set a block size.
// Data of this size will be repeated until output size has been reached.
func (o PatternOpts) Size(s int) PatternOpts {
	o.size = s
	return o
}

// Compressibility will set the compressibility of the data, ranging from 0 (fully random) to 1.0 (fully pattern).
func (o PatternOpts) Compressibility(c float32) PatternOpts {
	o.compressibility = c
	return o
}

// PatternOpts are the options for the pattern data source.
type PatternOpts struct {
	seed            *int64
	size            int
	compressibility float32
}

func patternOptsDefaults() PatternOpts {
	return PatternOpts{
		seed:            nil,
		size:            16 * 1024 * 1024,
		compressibility: 0.5,
	}
}

type patternSource struct {
	o   Options
	buf *circularBuffer
	obj Object
	rng *rand.Rand
}

func newPattern(o Options) (Source, error) {
	rndSrc := rand.NewSource(int64(rand.Uint64()))
	if o.pattern.seed != nil {
		rndSrc = rand.NewSource(*o.pattern.seed)
	}
	rng := rand.New(rndSrc)

	size := o.pattern.size
	if int64(size) > o.totalSize {
		size = int(o.totalSize)
	}
	if size <= 0 {
		return nil, fmt.Errorf("size must be >= 0, got %d", size)
	}

	buf := make([]byte, size)
	patternFill(buf, rng, o.pattern.compressibility)

	src := &patternSource{
		o:   o,
		buf: newCircularBuffer(buf, o.totalSize),
		obj: Object{
			Reader:      nil,
			Name:        "",
			ContentType: "application/octet-stream",
			Size:        0,
		},
		rng: rng,
	}
	src.obj.setPrefix(o)
	return src, nil
}

var patternBlock []byte

func patternFill(buf []byte, rng *rand.Rand, compressibility float32) {
	// choose size of individual runs of random vs. pattern data
	blockLen := 1024
	if len(buf) > 1024*1024 {
		blockLen = 32 * 1024
	}

	// initialize pattern block if needed
	if patternBlock == nil || len(patternBlock) != blockLen {
		patternBlock = make([]byte, blockLen)
		for i := range patternBlock {
			patternBlock[i] = byte('A')
		}
	}

	blocks := len(buf) / blockLen   // number of full blocks
	leftover := len(buf) % blockLen // any partial block at the end
	patternBlocks := int(float32(blocks) * compressibility)
	randomBlocks := blocks - patternBlocks
	bias := int(float64(compressibility) * float64(math.MaxInt))

	// Do all the full blocks we've got
	for i := 0; i < blocks; i++ {
		tmp := rng.Int()
		if (randomBlocks == 0 || tmp < bias) && patternBlocks > 0 {
			if patternBlocks == 0 {
				panic("pattern: internal error: no pattern blocks left")
			}
			// fill with pattern block
			// fmt.Printf("block %d of %d: PATTERN (%d pattern, %d random remain)\n", i+1, blocks, patternBlocks, randomBlocks)
			copy(buf[i*blockLen:], patternBlock)
			patternBlocks--
		} else {
			if randomBlocks == 0 {
				panic("pattern: internal error: no random blocks left")
			}
			// fill with random block
			// fmt.Printf("block %d of %d: RANDOM (%d pattern, %d random remain)\n", i+1, blocks, patternBlocks, randomBlocks)
			rng.Read(buf[i*blockLen : (i+1)*blockLen])
			randomBlocks--
		}
	}

	// Fill in leftover
	if leftover == 0 {
		// Done
	} else if rng.Int() < bias {
		copy(buf[blocks*blockLen:], patternBlock[:leftover])
	} else {
		rng.Read(buf[blocks*blockLen:])
	}
}

func (p *patternSource) Object() *Object {
	// reinitialize and pattern-fill buffer
	dst := p.buf.data[:cap(p.buf.data)]
	patternFill(dst, p.rng, p.o.pattern.compressibility)
	p.buf.data = dst
	p.obj.Reader = p.buf.Reset(0)
	p.obj.Size = p.o.getSize(p.rng)

	// set random name
	var nBuf [16]byte
	randASCIIBytes(nBuf[:], p.rng)
	p.obj.setName(string(nBuf[:]) + ".bin")
	return &p.obj
}

func (p *patternSource) String() string {
	if p.o.randSize {
		return fmt.Sprintf("Pattern data; random size up to %d bytes, compressibility %0.2f",
			p.o.totalSize, p.o.pattern.compressibility)
	}
	return fmt.Sprintf("Pattern data; %d bytes total, compressibility %0.2f",
		p.buf.want, p.o.pattern.compressibility)
}

func (p *patternSource) Prefix() string {
	return p.obj.Prefix
}
