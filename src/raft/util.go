package raft

import "log"

// Debugging
const Debug = 1

func DPrintf(format string, rf *Raft, a ...interface{}) {
	if Debug > 0 {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		b := []interface{}{rf.me, Convert(rf.state)}
		log.Printf("| %d %8s: " + format, append(b, a...)...)
	}
}

func PlainDPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		log.Printf("| " + format, a...)
	}
}

func Convert(state int) string {
	if state == Follower {
		return "Follower"
	} else if state == Candidate {
		return "Candidate"
	} else if state == Leader {
		return "Leader"
	}
	return "State out of bound!!"
}

func Min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func Max(a int, b int) int {
	if a >= b {
		return a
	} else {
		return b
	}
}
