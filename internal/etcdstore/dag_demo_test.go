package etcdstore

import "testing"

func TestEtcd(t *testing.T) {
	cli, err := GetLocalClient()
	if err != nil {
		t.Errorf("cli build in wrong way")
	}
	NormalSase(cli)
}
