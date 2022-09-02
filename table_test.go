package exceltesting

import "testing"

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
