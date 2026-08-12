package base

// RootCommand is the root command of all commands
var RootCommand *Command

func init() {
	RootCommand = &Command{
		UsageLine: CommandEnv.Exec,
		Long:      "The root command",
	}
}
