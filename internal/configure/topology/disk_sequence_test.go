package topology

import (
	"fmt"
	"testing"
)

func TestDiskSequenceVariable(t *testing.T) {
	data := `
kind: dingofs
global:
  container_image: dingodatabase/dingofs:latest
  log_dir: /mnt/disk${service_replica_disk_sequence}/logs/${service_role}-${service_replica_sequence_with_offset}
  sequence_offset: 1
  disk_count: 3
mds_services:
  config:
    server.port: 690${service_replica_sequence}
    mds_storage_engine: tikv
    storage_url: list://127.0.0.1:2379
  deploy:
    - host: 10.0.0.1
      replicas: 6
`
	ctx := NewContext()
	ctx.Add("10.0.0.1", "10.0.0.1")
	dcs, err := ParseTopology(data, ctx)
	if err != nil {
		t.Fatalf("parse topology: %v", err)
	}
	if len(dcs) != 6 {
		t.Fatalf("expect 6 deploy configs, got %d", len(dcs))
	}
	for i, dc := range dcs {
		disk := i%3 + 1
		want := fmt.Sprintf("/mnt/disk%d/logs/mds-%d", disk, i+1)
		if got := dc.GetLogDir(); got != want {
			t.Errorf("replica %d: log dir %q, want %q", i, got, want)
		}
	}
}
