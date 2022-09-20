package exceltesting

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func Test_table_buildInsertSQL(t1 *testing.T) {
	type fields struct {
		name        string
		columnsType []string
		columns     []string
		data        [][]string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "build INSERT statement",
			fields: fields{
				name:    "company",
				columns: []string{"company_cd", "company_name", "founded_year", "created_at"},
				data:    [][]string{{"0001", "Future", "1989", "current_timestamp"}, {"0002", "YDC", "1972", "current_timestamp"}},
			},
			want: "INSERT INTO company (company_cd,company_name,founded_year,created_at) VALUES('0001', 'Future', '1989', current_timestamp),('0002', 'YDC', '1972', current_timestamp);\n",
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &table{
				name:    tt.fields.name,
				columns: tt.fields.columns,
				data:    tt.fields.data,
			}
			if got := t.buildInsertSQL(); got != tt.want {
				t1.Errorf("buildInsertSQL() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_table_merge(t *testing.T) {
	src := &table{
		name:    "src",
		columns: []string{"a", "b", "c"},
		data: [][]string{
			{"s1a", "s1b", "s1c"},
			{"s2a", "s2b", "s2c"},
		},
	}

	defaultValues := []dbColumn{
		{name: "c", dataType: "character varying", data: "y1c"},
		{name: "d", dataType: "integer", data: "y1d"},
	}

	src.merge(defaultValues)
	got := src

	want := &table{
		name:    "src",
		columns: []string{"a", "b", "c", "d"},
		data: [][]string{
			{"s1a", "s1b", "s1c", "y1d"},
			{"s2a", "s2b", "s2c", "y1d"},
		},
	}

	opts := []cmp.Option{cmp.AllowUnexported(table{})}
	if diff := cmp.Diff(want, got, opts...); diff != "" {
		t.Errorf("merge() mismatch (-want +got):\n%s", diff)
	}
}
