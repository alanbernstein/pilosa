// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pql_test

import (
	"reflect"
	"testing"

	"github.com/pilosa/pilosa/pql"
	_ "github.com/pilosa/pilosa/test"
)

// Ensure the parser can parse specific PQL combinations.
func TestParser_Pql(t *testing.T) {

	tests := []struct {
		pql    string
		pqlOld string
		call   pql.Call
	}{
		{
			pql:    "Row(aaa=10)",
			pqlOld: "Bitmap(frame=aaa, row=10)",
			call: pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					"frame": "aaa",
					"row":   int64(10),
				},
			},
		},
		{
			pql:    "Range(bbb > 20)",
			pqlOld: "Range(frame=bbb, fld > 20)",
			call: pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					"frame": "bbb",
					"bbb":   &pql.Condition{Op: pql.GT, Value: 20},
				},
			},
		},
		{
			pql:    "Range(10 < bbb < 20)",
			pqlOld: "Range(frame=bbb, fld >< [10, 20])",
			call: pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					"frame": "bbb",
					"bbb":   &pql.Condition{Op: pql.GT, Value: []interface{}{10, 20}},
				},
			},
		},
		{
			pql:    "Set(aaa=10, col=100)",
			pqlOld: "SetBit(frame=aaa, row=10, col=100)",
			call: pql.Call{
				Name: "Set",
				Args: map[string]interface{}{
					"frame": "aaa",
					"row":   int64(10),
					"col":   int64(100),
				},
			},
		},
		{
			pql:    `Set(aaa=10, col=100, "2017-03-02T03:00")`,
			pqlOld: `SetBit(frame=aaa, row=10, col=100, timestamp="2017-03-02T03:00")`,
			call: pql.Call{
				Name: "Set",
				Args: map[string]interface{}{
					"frame":     "aaa",
					"row":       int64(10),
					"col":       int64(100),
					"timestamp": "2017-03-02T03:00",
				},
			},
		},
		{
			pql:    "Set(bbb=8, col=200)",
			pqlOld: "SetFieldValue(frame=bbb, col=200, fld=8)",
			call: pql.Call{
				Name: "Set",
				Args: map[string]interface{}{
					"frame": "bbb",
					"col":   int64(200),
					"fld":   8,
				},
			},
		},
		{
			pql:    "Count(Row(aaa=10))",
			pqlOld: "Count(Bitmap(frame=aaa, row=10))",
			call: pql.Call{
				Name: "Count",
				Children: []*pql.Call{{
					Name: "Bitmap",
					Args: map[string]interface{}{"frame": "aaa", "row": int64(10)},
				}},
			},
		},
		{
			pql:    "Union(Row(aaa=10), Row(aaa=11))",
			pqlOld: "Union(Bitmap(frame=aaa, row=10), Bitmap(frame=aaa, row=11))",
			call: pql.Call{
				Name: "Union",
				Children: []*pql.Call{
					{
						Name: "Bitmap",
						Args: map[string]interface{}{"frame": "aaa", "row": int64(10)},
					},
					{
						Name: "Bitmap",
						Args: map[string]interface{}{"frame": "aaa", "row": int64(11)},
					},
				},
			},
		},
		{
			pql:    "Union(Row(aaa=10), Row(bbb<30))",
			pqlOld: "Union(Bitmap(frame=aaa, row=10), Range(frame=bbb, bsi < 30))",
			call: pql.Call{
				Name: "Union",
				Children: []*pql.Call{
					{
						Name: "Row",
						Args: map[string]interface{}{"frame": "aaa", "row": int64(10)},
					},
					{
						Name: "Range",
						Args: map[string]interface{}{"frame": "bbb", "bbb": &pql.Condition{Op: pql.LT, Value: 30}},
					},
				},
			},
		},
		{
			pql:    "TopN(field=aaa, n=25)",
			pqlOld: "TopN(frame=aaa, n=25)",
			call: pql.Call{
				Name: "TopN",
				Args: map[string]interface{}{
					"frame": "aaa",
					"n":     int64(25),
				},
			},
		},
		{
			pql:    "TopN(field=aaa, ids=[10, 20, 30])",
			pqlOld: "TopN(frame=aaa, ids=[10, 20, 30])",
			call: pql.Call{
				Name: "TopN",
				Args: map[string]interface{}{
					"frame": "aaa",
					"ids":   []int64{10, 20, 30},
				},
			},
		},
		{
			pql:    "TopN(field=aaa, attrName=foo, attrVals=[10, 20, 30])",
			pqlOld: "TopN(frame=aaa, field=foo, filters=[10, 20, 30])",
			call: pql.Call{
				Name: "TopN",
				Args: map[string]interface{}{
					"frame":    "aaa",
					"attrName": "foo",
					"attrVals": []int64{10, 20, 30},
				},
			},
		},
		{
			pql:    "TopN(field=aaa, threshold=5)",
			pqlOld: "TopN(frame=aaa, threshold=5)",
			call: pql.Call{
				Name: "TopN",
				Args: map[string]interface{}{
					"frame":     "aaa",
					"n":         int64(25),
					"threshold": 5,
				},
			},
		},
		{
			pql:    "TopN(field=aaa, n=25, threshold=5)",
			pqlOld: "TopN(frame=aaa, n=25, threshold=5)",
			call: pql.Call{
				Name: "TopN",
				Args: map[string]interface{}{
					"frame":     "aaa",
					"n":         int64(25),
					"threshold": 5,
				},
			},
		},
		{
			pql:    "TopN(Row(aaa=10), field=aaa, n=25)",
			pqlOld: "TopN(Bitmap(frame=aaa, row=10), frame=aaa, n=25)",
			call: pql.Call{
				Name: "TopN",
				Children: []*pql.Call{
					{
						Name: "Row",
						Args: map[string]interface{}{"frame": "aaa", "row": int64(10)},
					},
				},
				Args: map[string]interface{}{"frame": "aaa", "n": int64(25)},
			},
		},
		{
			pql:    "TopN(Row(aaa=10), field=aaa, n=25, threshold=5)",
			pqlOld: "TopN(Bitmap(frame=aaa, row=10), frame=aaa, n=25, threshold=5)",
			call: pql.Call{
				Name: "TopN",
				Children: []*pql.Call{
					{
						Name: "Row",
						Args: map[string]interface{}{
							"frame":     "aaa",
							"row":       int64(10),
							"threshold": 5,
						},
					},
				},
				Args: map[string]interface{}{"frame": "aaa", "n": int64(25)},
			},
		},
		{
			pql:    `Row(ts=1, "2010-01-01T00:00", "2017-03-02T03:00")`,
			pqlOld: `Range(frame=ts, row=1, start="2010-01-01T00:00", end="2017-03-02T03:00")`,
			call: pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					"frame": "ts",
					"row":   int64(1),
					"start": "2010-01-01T00:00",
					"end":   "2017-03-02T03:00",
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.pql, func(t *testing.T) {
			q, err := pql.ParseString(test.pql)
			if err != nil {
				t.Error(err)
			} else if !reflect.DeepEqual(q.Calls[0], &test.call) {
				t.Errorf("unexpected call: %s, exp: %s", q.Calls[0], &test.call)
			}
		})
	}
}

// Ensure the parser can parse generally.
func TestParser_Parse(t *testing.T) {
	// Parse with no children or arguments.
	t.Run("Empty", func(t *testing.T) {
		q, err := pql.ParseString(`Bitmap()`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "Bitmap",
			},
		) {
			t.Fatalf("unexpected call: %s", q.Calls[0])
		}
	})

	// Parse with only children.
	t.Run("ChildrenOnly", func(t *testing.T) {
		q, err := pql.ParseString(`Union(  Bitmap()  , Count()  )`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "Union",
				Children: []*pql.Call{
					&pql.Call{Name: "Bitmap"},
					&pql.Call{Name: "Count"},
				},
			},
		) {
			t.Fatalf("unexpected call: %s", q.Calls[0])
		}
	})

	// Parse a single child with a single argument.
	t.Run("ChildWithArgument", func(t *testing.T) {
		q, err := pql.ParseString(`Count( Bitmap( id=100))`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "Count",
				Children: []*pql.Call{
					{Name: "Bitmap", Args: map[string]interface{}{"id": int64(100)}},
				},
			},
		) {
			t.Fatalf("unexpected call: %s", q.Calls[0])
		}
	})

	// Parse with only arguments.
	t.Run("ArgumentsOnly", func(t *testing.T) {
		q, err := pql.ParseString(`MyCall( key= value, foo="bar", age = 12 , bool0=true, bool1=false, x=null  )`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "MyCall",
				Args: map[string]interface{}{
					"key":   "value",
					"foo":   "bar",
					"age":   int64(12),
					"bool0": true,
					"bool1": false,
					"x":     nil,
				},
			},
		) {
			t.Fatalf("unexpected call: %#v", q.Calls[0])
		}
	})

	// Parse with float arguments.
	t.Run("WithFloatArgs", func(t *testing.T) {
		q, err := pql.ParseString(`MyCall( key=12.25, foo= 13.167, bar=2., baz=0.9)`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "MyCall",
				Args: map[string]interface{}{
					"key": 12.25,
					"foo": 13.167,
					"bar": 2.,
					"baz": 0.9,
				},
			},
		) {
			t.Fatalf("unexpected call: %#v", q.Calls[0])
		}
	})

	// Parse with float arguments.
	t.Run("WithNegativeArgs", func(t *testing.T) {
		q, err := pql.ParseString(`MyCall( key=-12.25, foo= -13)`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "MyCall",
				Args: map[string]interface{}{
					"key": -12.25,
					"foo": int64(-13),
				},
			},
		) {
			t.Fatalf("unexpected call: %#v", q.Calls[0])
		}
	})

	// Parse with both child calls and arguments.
	t.Run("ChildrenAndArguments", func(t *testing.T) {
		q, err := pql.ParseString(`TopN(Bitmap(id=100, frame=other), frame=f, n=3)`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "TopN",
				Children: []*pql.Call{{
					Name: "Bitmap",
					Args: map[string]interface{}{"id": int64(100), "frame": "other"},
				}},
				Args: map[string]interface{}{"n": int64(3), "frame": "f"},
			},
		) {
			t.Fatalf("unexpected call: %#v", q.Calls[0])
		}
	})

	// Parse a list argument.
	t.Run("ListArgument", func(t *testing.T) {
		q, err := pql.ParseString(`TopN(frame="f", ids=[0,10,30])`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "TopN",
				Args: map[string]interface{}{
					"frame": "f",
					"ids":   []interface{}{int64(0), int64(10), int64(30)},
				},
			},
		) {
			t.Fatalf("unexpected call: %#v", q.Calls[0])
		}
	})

	// Parse with condition arguments.
	t.Run("WithCondition", func(t *testing.T) {
		q, err := pql.ParseString(`MyCall(key=foo, x == 12.25, y >= 100, 4 < z < 8, m != null)`)

		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "MyCall",
				Args: map[string]interface{}{
					"key": "foo",
					"x":   &pql.Condition{Op: pql.EQ, Value: 12.25},
					"y":   &pql.Condition{Op: pql.GTE, Value: int64(100)},
					"z":   &pql.Condition{Op: pql.BETWEEN, Value: []interface{}{int64(4), int64(8)}},
					"m":   &pql.Condition{Op: pql.NEQ, Value: nil},
				},
			},
		) {
			t.Fatalf("unexpected call: %#v", q.Calls[0])
		}
	})

}
