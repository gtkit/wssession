package wssession

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain 在全部测试结束后断言无 goroutine 泄漏:
// readLoop / processLoop / writeLoop / ctx watcher / duplex turn 必须全部收敛。
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
