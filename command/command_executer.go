package command

import (
	"log"
	"os/exec"
	"syscall"
)

type CommandExecuter struct {
	errLogger *log.Logger
	infLogger *log.Logger
}

func New(errLogger, infLogger *log.Logger) *CommandExecuter {
	return &CommandExecuter{
		errLogger: errLogger,
		infLogger: infLogger,
	}
}

func (me CommandExecuter) Execute(cmd *exec.Cmd, body []byte) int {
	me.infLogger.Println("Processing message...")
	me.infLogger.Printf("Cmd: %s  params: %s \n", cmd.Path, cmd.Args)
	out, err := cmd.CombinedOutput()

	//log output php script to info
	me.infLogger.Printf("Output php: %s\n", string(out[:]))

	if err != nil {
		me.infLogger.Println("Failed. Check error log for details.")
		me.errLogger.Printf("Failed: %s\n", string(out[:]))
		me.errLogger.Printf("Error: %s\n", err)

		if exiterr, ok := err.(*exec.ExitError); ok {
			if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
				return status.ExitStatus()
			}
		}

		return 1
	}

	me.infLogger.Println("Processed!")

	return 0
}
