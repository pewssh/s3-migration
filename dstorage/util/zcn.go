package util

import "sync"

const (
	ZCNStatusSuccess int = 0
	ZCNStatusError   int = 1
)

type ZCNStatus struct {
	WalletString string
	Wg           *sync.WaitGroup
	Success      bool
	ErrMsg       string
}

func (zcn *ZCNStatus) OnWalletCreateComplete(status int, wallet string, err string) {
	defer zcn.Wg.Done()
	if status == ZCNStatusError {
		zcn.Success = false
		zcn.ErrMsg = err
		zcn.WalletString = ""
		return
	}
	zcn.Success = true
	zcn.ErrMsg = ""
	zcn.WalletString = wallet
	return
}
