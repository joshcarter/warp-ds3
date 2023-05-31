package cli

import (
	"github.com/joshcarter/warp-ds3/pkg/bench"
	"github.com/minio/cli"
	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/console"
)

var bulkPutFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "obj.size",
		Value: "1MiB",
		Usage: "Size of each generated object. Can be a number or 10KiB/MiB/GiB. All sizes are base 2 binary.",
	},
	cli.IntFlag{
		Name:  "bulk.num",
		Value: 100,
		Usage: "Number of objects per bulk put operation.",
	},
}

// Put command.
var bulkPutCmd = cli.Command{
	Name:   "bulkput",
	Usage:  "benchmark bulk put objects",
	Action: mainBulkPut,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, ioFlags, bulkPutFlags, genFlags, benchFlags, analyzeFlags),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainPut is the entry point for cp command.
func mainBulkPut(ctx *cli.Context) error {
	checkBulkPutSyntax(ctx)
	src := newGenSource(ctx, "obj.size")
	b := bench.BulkPut{
		Common: bench.Common{
			Client:      newClient(ctx),
			Concurrency: ctx.Int("concurrent"),
			Source:      src,
			Bucket:      ctx.String("bucket"),
			Endpoint:    parseHosts(ctx.String("host"), ctx.Bool("resolve-host"))[0], // FIXME: ugly
			PutOpts:     bulkPutOpts(ctx),
		},
		BulkNum: ctx.Int("bulk.qty"),
	}
	return runBench(ctx, &b)
}

// putOpts retrieves put options from the context.
func bulkPutOpts(ctx *cli.Context) minio.PutObjectOptions {
	pSize, _ := toSize(ctx.String("part.size"))
	return minio.PutObjectOptions{
		ServerSideEncryption: newSSE(ctx),
		DisableMultipart:     ctx.Bool("disable-multipart"),
		SendContentMd5:       ctx.Bool("md5"),
		StorageClass:         ctx.String("storage-class"),
		PartSize:             pSize,
	}
}

func checkBulkPutSyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("Command takes no arguments")
	}
	if ctx.Int("bulk.num") <= 0 {
		console.Fatal("Bulk operation must have more than 0 objects.")
	}

	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
