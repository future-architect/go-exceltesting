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
				name:        "company",
				columnsType: []string{"C", "C", "N"},
				columns:     []string{"company_cd", "company_name", "founded_year"},
				data:        [][]string{{"0001", "Future", "1989"}, {"0002", "YDC", "1972"}},
			},
			want: "INSERT INTO company (company_cd,company_name,founded_year) VALUES('0001', 'Future', 1989),('0002', 'YDC', 1972);\n",
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &table{
				name:        tt.fields.name,
				columnsType: tt.fields.columnsType,
				columns:     tt.fields.columns,
				data:        tt.fields.data,
			}
			if got := t.buildInsertSQL(); got != tt.want {
				t1.Errorf("buildInsertSQL() = %v, want %v", got, tt.want)
			}
		})
	}
}
