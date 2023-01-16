package main

import (
	"os"

	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("booster")

var FlagRepo = &cli.StringFlag{
	Name:    "repo",
	Usage:   "repo directory for stargate",
	Value:   "~/.stargate",
	EnvVars: []string{"STARGATE_REPO"},
}

// IsVeryVerbose is a global var signalling if the CLI is running in very
// verbose mode or not (default: false).
var IsVeryVerbose bool

// FlagVeryVerbose enables very verbose mode, which is useful when debugging
// the CLI itself. It should be included as a flag on the top-level command
// (e.g. boost -vv).
var FlagVeryVerbose = &cli.BoolFlag{
	Name:        "vv",
	Usage:       "enables very verbose mode, useful for debugging the CLI",
	Destination: &IsVeryVerbose,
}

func main() {
	app := &cli.App{
		Name:                 "stargate",
		Usage:                "endpoint for retrieving with stargate protocol",
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			FlagVeryVerbose,
			FlagRepo,
		},
		Commands: []*cli.Command{
			initCmd,
			serverCmd,
			fetchCmd,
			importCmd,
		},
	}
	app.Setup()

	if err := app.Run(os.Args); err != nil {
		os.Stderr.WriteString("Error: " + err.Error() + "\n")
	}
}

func before(cctx *cli.Context) error {
	_ = logging.SetLogLevel("stargate", "INFO")

	if IsVeryVerbose {
		_ = logging.SetLogLevel("stargate", "DEBUG")
		_ = logging.SetLogLevel("unixfsstoresql", "DEBUG")
	}

	return nil
}
