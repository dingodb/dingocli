package cli

import (
	"encoding/json"
	"github.com/dingodb/dingocli/internal/utils"
	"os"
	"path/filepath"
	"testing"
)

func TestWithHostsIsInvocationLocal(t *testing.T) {
	key := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(key, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(map[string]interface{}{"global": map[string]interface{}{"private_key_file": key}, "hosts": []interface{}{map[string]interface{}{"host": "new", "hostname": "127.0.0.1"}}})
	original := &DingoCli{hosts: "stored database snapshot", memStorage: utils.NewSafeMap()}
	scoped, err := original.WithHosts(string(data))
	if err != nil {
		t.Fatal(err)
	}
	if original.Hosts() != "stored database snapshot" || scoped.memStorage == original.memStorage {
		t.Fatal("mutated original context")
	}
	if _, err := scoped.GetHost("new"); err != nil {
		t.Fatal(err)
	}
	if _, err := scoped.GetHost("stored"); err == nil {
		t.Fatal("fell back to stored hosts")
	}
	if _, err := original.WithHosts(`{"hosts":[]}`); err == nil {
		t.Fatal("accepted empty hosts")
	}
	if original.Hosts() != "stored database snapshot" {
		t.Fatal("mutation after error")
	}
}
