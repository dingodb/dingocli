package topology

import (
	"testing"
)

func TestParseTopologyTikvRoles(t *testing.T) {
	data := `
kind: dingofs
global:
  container_image: dingodatabase/dingofs:latest
mds_services:
  config:
    mds_storage_engine: tikv
    storage_url: list://127.0.0.1:2379
  deploy:
    - host: 10.0.0.1
    - host: 10.0.0.2
    - host: 10.0.0.3
`
	ctx := NewContext()
	ctx.Add("10.0.0.1", "10.0.0.1")
	ctx.Add("10.0.0.2", "10.0.0.2")
	ctx.Add("10.0.0.3", "10.0.0.3")
	dcs, err := ParseTopology(data, ctx)
	if err != nil {
		t.Fatalf("parse tikv topology: %v", err)
	}
	for _, dc := range dcs {
		if dc.GetRole() != ROLE_FS_MDS {
			t.Errorf("tikv topology should only contain %s, got %s", ROLE_FS_MDS, dc.GetRole())
		}
	}
	if len(dcs) != 3 {
		t.Errorf("expect 3 mds deploy configs, got %d", len(dcs))
	}
	if v := ctx.Lookup(CTX_KEY_MDS_VERSION); v != CTX_VAL_MDS_V2 {
		t.Errorf("expect mds version %s, got %v", CTX_VAL_MDS_V2, v)
	}
}

func TestParseTopologyMdsv2OnlyRoles(t *testing.T) {
	data := `
kind: dingofs
global:
  container_image: dingodatabase/dingofs:latest
mds_services:
  config:
    mds_storage_engine: rocksdb
  deploy:
    - host: 10.0.0.1
    - host: 10.0.0.2
    - host: 10.0.0.3
`
	ctx := NewContext()
	ctx.Add("10.0.0.1", "10.0.0.1")
	ctx.Add("10.0.0.2", "10.0.0.2")
	ctx.Add("10.0.0.3", "10.0.0.3")
	dcs, err := ParseTopology(data, ctx)
	if err != nil {
		t.Fatalf("parse mdsv2-only topology: %v", err)
	}
	roles := map[string]bool{}
	for _, dc := range dcs {
		roles[dc.GetRole()] = true
	}
	if !roles[ROLE_FS_MDS] || !roles[ROLE_FS_MDS_CLI] {
		t.Errorf("non-tikv mdsv2-only topology should contain %s and %s, got %v",
			ROLE_FS_MDS, ROLE_FS_MDS_CLI, roles)
	}
}

func TestGetMdsStorageEngineGlobalFallback(t *testing.T) {
	tp := &Topology{
		Global: map[string]interface{}{"mds_storage_engine": "tikv"},
	}
	if got := getMdsStorageEngine(tp); got != "tikv" {
		t.Errorf("global fallback: expect tikv, got %q", got)
	}
	tp.MdsServices = Service{Config: map[string]interface{}{"mds_storage_engine": "rocksdb"}}
	if got := getMdsStorageEngine(tp); got != "rocksdb" {
		t.Errorf("service config should override global: expect rocksdb, got %q", got)
	}
}
