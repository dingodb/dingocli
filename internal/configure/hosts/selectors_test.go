package hosts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/spf13/pflag"
)

func selectorFixture(t *testing.T) string {
	t.Helper()
	key := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(key, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	rows := []map[string]interface{}{
		{"host": "gpu-1", "hostname": "127.0.0.1", "labels": []string{"cache_servers", "production"}},
		{"host": "gpu-22", "hostname": "127.0.0.2", "labels": []string{"cache_servers", "test"}},
		{"host": "cpu-1", "hostname": "127.0.0.3", "labels": []string{"client_servers"}},
		{"host": "gpuX1", "hostname": "127.0.0.4", "labels": []string{"odd"}},
		{"host": "special", "hostname": "127.0.0.5", "labels": []string{"tag.*", "re:^gpu", "owner:region"}},
	}
	data, err := json.Marshal(map[string]interface{}{"global": map[string]interface{}{"private_key_file": key}, "hosts": rows})
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestExactRegexAndBooleanSelectors(t *testing.T) {
	data := selectorFixture(t)
	for _, test := range []struct{ selectors, want []string }{
		{[]string{"gpu-1"}, []string{"gpu-1"}},
		{[]string{"cache_servers"}, []string{"gpu-1", "gpu-22"}},
		{[]string{"gpu.1"}, []string{}},
		{[]string{"tag.*"}, []string{"special"}},
		{[]string{"re:^gpu-[0-9]{1,3}$"}, []string{"gpu-1", "gpu-22"}},
		{[]string{"cache_servers", "!re:-22$"}, []string{"gpu-1"}},
		{[]string{"re:^gpu", "&cache_servers", "&production"}, []string{"gpu-1"}},
		{[]string{"&cache_servers", "&re:-22$"}, []string{"gpu-22"}},
		{[]string{"re:^gpu-", "cpu-1"}, []string{"gpu-1", "gpu-22", "cpu-1"}},
		{[]string{"literal:re:^gpu"}, []string{"special"}},
		{[]string{"literal:owner:region"}, []string{"special"}},
	} {
		t.Run(stringsForTest(test.selectors), func(t *testing.T) {
			result, err := Filter(data, test.selectors)
			if err != nil {
				t.Fatal(err)
			}
			names := []string{}
			for _, host := range result {
				names = append(names, host.GetHost())
			}
			if !reflect.DeepEqual(names, test.want) {
				t.Fatalf("got %v want %v", names, test.want)
			}
		})
	}
	for _, selector := range []string{"re:[", "!re:(", "&re:", "re:"} {
		if _, err := Filter(data, []string{selector}); err == nil {
			t.Fatalf("accepted %q", selector)
		}
	}
}

func stringsForTest(values []string) string {
	data, _ := json.Marshal(values)
	return string(data)
}

func TestHostNameLabelIsAlwaysPresentOnce(t *testing.T) {
	host := NewHostConfig(0, map[string]interface{}{"host": "node"})
	host.labels = []string{"group"}
	if got := host.GetLabels(); !reflect.DeepEqual(got, []string{"group", "node"}) {
		t.Fatal(got)
	}
	if !reflect.DeepEqual(host.labels, []string{"group"}) {
		t.Fatal("mutated configured labels")
	}
	host.labels = []string{"node", "group"}
	if got := host.GetLabels(); !reflect.DeepEqual(got, []string{"node", "group"}) {
		t.Fatal(got)
	}
}

func TestLabelFlagsPreserveRegexCommasAndLegacySeparators(t *testing.T) {
	for _, separator := range []string{",", ":"} {
		t.Run(separator, func(t *testing.T) {
			flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
			var selectors []string
			AddLabelFlag(flags, &selectors, separator)
			if _, ok := flags.Lookup("labels").Value.(pflag.SliceValue); !ok {
				t.Fatal("lost string-slice interface")
			}
			args := []string{"-l", "cache_servers" + separator + "!test", "-l", "&re:^gpu-[0-9]{1,3}$", "-l", "literal:owner:region"}
			if err := flags.Parse(args); err != nil {
				t.Fatal(err)
			}
			want := []string{"cache_servers", "!test", "&re:^gpu-[0-9]{1,3}$", "literal:owner:region"}
			if !reflect.DeepEqual(selectors, want) {
				t.Fatal(selectors)
			}
			parsed, err := flags.GetStringSlice("labels")
			if err != nil || !reflect.DeepEqual(parsed, want) {
				t.Fatalf("%v %v", parsed, err)
			}
		})
	}
	for _, args := range [][]string{{"-l", "re:["}, {"-l", "cache_servers,!re:^gpu-[0-9]{1,3}$"}, {"-l", "cache_servers,\"re:^gpu\""}} {
		flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
		var selectors []string
		AddLabelFlag(flags, &selectors, ",")
		if err := flags.Parse(args); err == nil {
			t.Fatalf("accepted ambiguous/invalid selector %v", args)
		}
	}
}
