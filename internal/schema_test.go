package internal

import (
	_ "embed"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/tidwall/gjson"
	"testing"
)

//go:embed testdata/data.json
var payload string

type SchemaTestSuit struct {
	suite.Suite
}

func (ss *SchemaTestSuit) SetupSuite() {
}

func TestSchemaSuite(t *testing.T) {
	suite.Run(t, new(SchemaTestSuit))
}

func (ss *SchemaTestSuit) TestShorten() {
	tests := []struct {
		schema       *Schema
		numOfMapping int
	}{
		{
			schema:       &Schema{Name: "sonarcube1", Namespace: "a", Mapping: make(map[string]string)},
			numOfMapping: 30,
		},
		{
			schema: &Schema{Name: "sonarcube2", Namespace: "b", Mapping: map[string]string{
				"abc": "a",
			}},
			numOfMapping: 31,
		},
	}
	exists := map[string]string{
		"c": "simpleTest-c",
		"d": "simpleTest-d",
		"p": "1234",
	}
	for _, test := range tests {
		rs, evolved := test.schema.shorten(payload)
		shorted := rs.MustGet()
		assert.True(ss.T(), len(shorted) < len(payload))
		for k, v := range exists {
			trs := gjson.Get(shorted, k)
			assert.True(ss.T(), trs.Exists())
			assert.Equal(ss.T(), v, trs.Str, "should equal for key %s", k)
		}
		assert.Equal(ss.T(), test.numOfMapping, len(evolved.Mapping))
	}
}
