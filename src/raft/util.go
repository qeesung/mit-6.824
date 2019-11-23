package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const SDebug = 1

func SDPrintf(format string, a ...interface{}) {
	if SDebug > 0 {
		log.Printf("++++++++++++++"+format, a...)
	}
	return
}
